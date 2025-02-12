use crate::chord::actor::{ChordActor, ChordHandle, ChordMessage};
use crate::chord::{
    types::{ChordNode, Key, NodeId, Value, KEY_SIZE},
    workers::{
        run_finger_maintainer, run_predecessor_checker, run_stabilize_worker,
        run_successor_maintainer,
    },
};
use crate::error::*;
use crate::network::grpc::PeerConfig;
use crate::network::grpc::{client::ChordGrpcClient, server::ChordGrpcServer, thread::GrpcThread};
use crate::network::messages::chord::{GetRequest, NodeInfo, PutRequest};
use futures::{StreamExt, FutureExt};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};
use tokio::select;
use tokio::time::sleep;
use std::cmp::min;
use std::pin::Pin;
use futures::Future;
use std::net::SocketAddr;

const SERVER_STARTUP_WAIT: Duration = Duration::from_secs(2);
const CONNECTION_RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_CONNECTION_RETRIES: u32 = 5;

pub struct ChordPeer {
    chord_handle: ChordHandle,
    chord_node: ChordNode,
    port: u16,
    _actor_handle: tokio::task::JoinHandle<()>, // Store actor handle to maintain lifetime
    grpc_handle: Option<tokio::task::JoinHandle<Result<(), NetworkError>>>, // Store gRPC server handle
}

impl ChordPeer {
    pub async fn new(config: PeerConfig) -> Result<Self, NetworkError> {
        // Create a random NodeId in the 256-bit hash space
        let node_id = NodeId::random();

        // Get port for gRPC server
        let port = config.grpc_port.unwrap_or_else(|| get_random_port());

        // Create the local address string
        let addr = format!("127.0.0.1:{}", port);

        // Create the chord node
        let chord_node = ChordNode::new(node_id, addr.clone()).await;

        // Create the actor system
        let (chord_handle, mut actor) = ChordHandle::new(node_id, port, addr.clone()).await;

        // Spawn the actor and store its handle
        let actor_handle = tokio::spawn(async move {
            actor.run().await;
        });

        Ok(Self {
            chord_handle,
            chord_node,
            port,
            _actor_handle: actor_handle,
            grpc_handle: None,
        })
    }

    async fn start_grpc_server(&mut self) -> Result<(), NetworkError> {
        if self.grpc_handle.is_some() {
            return Ok(());
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let node = Arc::new(self.chord_node.clone());
        let thread_config = self.chord_node.get_shared_state();
        let local_addr = format!("127.0.0.1:{}", self.port);

        // Try to bind to the address first to ensure it's available
        let addr = local_addr.parse::<std::net::SocketAddr>().map_err(|e| {
            error!("Failed to parse address {}: {}", local_addr, e);
            NetworkError::Grpc(format!("Invalid address: {}", e))
        })?;

        // Create and start gRPC thread
        let mut grpc_thread = GrpcThread::new(node.clone(), thread_config.clone(), shutdown_rx, ready_tx);
        
        info!("Starting gRPC server on {}", local_addr);
        debug!("Server configuration: local_addr={}, node_id={}", local_addr, self.chord_node.node_id);
        
        let handle = tokio::spawn(async move {
            grpc_thread.run().await
        });

        // Store the handle immediately
        self.grpc_handle = Some(handle);
        
        // Wait for server to be ready with a timeout
        match tokio::time::timeout(Duration::from_secs(5), ready_rx).await {
            Ok(Ok(_)) => {
                info!("gRPC server is ready and listening on {}", local_addr);
                Ok(())
            }
            Ok(Err(_)) => {
                error!("gRPC server failed to initialize properly");
                Err(NetworkError::Grpc("Server failed to initialize".into()))
            }
            Err(_) => {
                error!("gRPC server startup timed out");
                Err(NetworkError::Grpc("Server startup timed out".into()))
            }
        }
    }

    pub async fn create_network(&mut self) -> Result<(), NetworkError> {
        info!("Creating new Chord network...");
        
        // Start gRPC server first and wait for it to be ready
        match self.start_grpc_server().await {
            Ok(_) => info!("gRPC server started successfully"),
            Err(e) => {
                error!("Failed to start gRPC server: {}", e);
                return Err(e);
            }
        }

        // Initialize as first node in the network
        match self.chord_node.join_network(None).await {
            Ok(_) => info!("Node initialized as bootstrap node"),
            Err(e) => {
                error!("Failed to initialize as bootstrap node: {}", e);
                return Err(NetworkError::Chord(e));
            }
        }

        // Add our own address to the node addresses map
        {
            let mut addresses = self.chord_node.node_addresses.lock().await;
            addresses.insert(self.chord_node.node_id, self.chord_node.local_addr.clone());
        }

        info!("Successfully initialized bootstrap node");
        info!("Bootstrap node is listening on port: {}", self.port);
        info!("Waiting for other nodes to join...");

        // For bootstrap node, we don't start maintenance workers immediately
        // They will be started when other nodes join the network
        // This is handled in the run() method

        Ok(())
    }

    pub async fn join(&mut self, bootstrap_addr: String) -> Result<(), NetworkError> {
        info!("Attempting to join network through bootstrap node: {}", bootstrap_addr);
        
        // Try to connect to bootstrap node once
        match ChordGrpcClient::new(bootstrap_addr.clone()).await {
            Ok(_) => info!("Successfully connected to bootstrap node"),
            Err(e) => {
                error!("Failed to connect to bootstrap node: {}", e);
                return Err(NetworkError::Chord(ChordError::JoinFailed(format!(
                    "Bootstrap node not available: {}", e
                ))));
            }
        }

        // Start our gRPC server
        self.start_grpc_server().await?;

        // Now try to join the Chord network
        self.chord_node
            .join_network(Some(bootstrap_addr))
            .await
            .map_err(NetworkError::Chord)?;

        info!("Successfully joined Chord network");

        // Share state with worker threads
        let thread_config = self.chord_node.get_shared_state();

        // Spawn worker threads after successful join
        tokio::spawn(run_stabilize_worker(thread_config.clone()));
        tokio::spawn(run_predecessor_checker(thread_config.clone()));
        tokio::spawn(run_finger_maintainer(thread_config.clone()));
        tokio::spawn(run_successor_maintainer(thread_config.clone()));

        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), NetworkError> {
        info!("Starting main event loop...");
        
        // Create a channel for shutdown signal
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        
        // Handle Ctrl+C
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(_) = tokio::signal::ctrl_c().await {
                info!("Received shutdown signal");
                let _ = shutdown_tx_clone.send(()).await;
            }
        });

        // For bootstrap node, we need to monitor when other nodes join
        let mut maintenance_workers_started = false;
        let mut check_interval = tokio::time::interval(Duration::from_secs(5));

        // Main event loop
        loop {
            tokio::select! {
                _ = check_interval.tick() => {
                    // Check if we need to start maintenance workers
                    if !maintenance_workers_started {
                        let has_other_nodes = {
                            let addresses = self.chord_node.node_addresses.lock().await;
                            addresses.len() > 1
                        };

                        if has_other_nodes {
                            info!("Other nodes have joined the network, starting maintenance workers...");
                            
                            // Share state with worker threads
                            let thread_config = self.chord_node.get_shared_state();
                            
                            // Start all maintenance workers
                            tokio::spawn(run_stabilize_worker(thread_config.clone()));
                            tokio::spawn(run_predecessor_checker(thread_config.clone()));
                            tokio::spawn(run_finger_maintainer(thread_config.clone()));
                            tokio::spawn(run_successor_maintainer(thread_config.clone()));
                            
                            maintenance_workers_started = true;
                            info!("Maintenance workers started successfully");
                        }
                    }

                    // Only run stabilization if maintenance workers are active
                    if maintenance_workers_started {
                        if let Err(e) = self.stabilize().await {
                            error!("Stabilization error: {}", e);
                        }
                    }
                }
                Some(_) = shutdown_rx.recv() => {
                    info!("Shutting down node...");
                    break;
                }
            }
        }

        info!("Node shutdown complete");
        Ok(())
    }

    async fn stabilize(&mut self) -> Result<(), NetworkError> {
        // For bootstrap node, if we don't have any successors yet, skip stabilization
        let has_successors = {
            let finger_table = self.chord_node.finger_table.lock().await;
            finger_table.has_successors()
        };

        if !has_successors {
            debug!("No successors yet, skipping stabilization (normal for bootstrap node)");
            return Ok(());
        }

        // Update finger table
        for i in 0..KEY_SIZE {
            let target = self.calculate_finger_id(i);
            // Find successor for this finger
            match self.chord_node.closest_preceding_node(&target.to_bytes()).await {
                Some(successor) => {
                    // Update finger table
                    let mut finger_table = self.chord_node.finger_table.lock().await;
                    finger_table.update_finger(i, successor);
                }
                None => {
                    debug!("No successor found for finger {}, skipping update", i);
                    continue;
                }
            }
        }

        // Run Chord stabilization only if we have successors
        self.chord_handle
            .stabilize()
            .await
            .map_err(|e| NetworkError::Chord(e))?;
        
        Ok(())
    }

    fn calculate_finger_id(&self, index: usize) -> NodeId {
        self.chord_node.node_id.get_finger_id(index)
    }

    pub async fn store_value(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), NetworkError> {
        let key = Key(key);
        let value = Value(value);
        let key_id = NodeId::from_key(&key.0);

        // Find the node responsible for this key
        if let Some(target_node) = self.chord_node.closest_preceding_node(&key.0).await {
            // Forward the store request to the responsible node
            let target_addr = self
                .chord_node
                .get_node_address(&target_node)
                .await
                .ok_or_else(|| {
                    NetworkError::Chord(ChordError::NodeNotFound("No address found".into()))
                })?;

            let mut client = ChordGrpcClient::new(target_addr)
                .await
                .map_err(|e| NetworkError::Grpc(format!("Failed to connect: {}", e)))?;

            let request = PutRequest {
                key: key.0,
                value: value.0,
                requesting_node: Some(NodeInfo {
                    node_id: self.chord_node.node_id.to_bytes().to_vec(),
                    address: self.chord_node.local_addr.clone(),
                }),
            };

            client
                .put(request)
                .await
                .map_err(|e| NetworkError::Grpc(format!("Failed to store value: {}", e)))?;

            Ok(())
        } else {
            Err(NetworkError::Chord(ChordError::NodeNotFound(
                "No responsible node found".into(),
            )))
        }
    }

    pub async fn get_value(&mut self, key: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        let key = Key(key);
        let key_id = NodeId::from_key(&key.0);

        // Find the node responsible for this key
        if let Some(target_node) = self.chord_node.closest_preceding_node(&key.0).await {
            // Forward the get request to the responsible node
            let target_addr = self
                .chord_node
                .get_node_address(&target_node)
                .await
                .ok_or_else(|| {
                    NetworkError::Chord(ChordError::NodeNotFound("No address found".into()))
                })?;

            let mut client = ChordGrpcClient::new(target_addr)
                .await
                .map_err(|e| NetworkError::Grpc(format!("Failed to connect: {}", e)))?;

            let request = GetRequest {
                key: key.0,
                requesting_node: Some(NodeInfo {
                    node_id: self.chord_node.node_id.to_bytes().to_vec(),
                    address: self.chord_node.local_addr.clone(),
                }),
            };

            let response = client
                .get(request)
                .await
                .map_err(|e| NetworkError::Grpc(format!("Failed to get value: {}", e)))?;

            if let Some(value) = response {
                Ok(value.0)
            } else {
                Err(NetworkError::Chord(ChordError::OperationFailed(
                    "Key not found".into(),
                )))
            }
        } else {
            Err(NetworkError::Chord(ChordError::NodeNotFound(
                "No responsible node found".into(),
            )))
        }
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }
}

fn get_random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}
