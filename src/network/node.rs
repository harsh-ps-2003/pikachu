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
use crate::network::grpc::{client::ChordGrpcClient, server::ChordGrpcServer};
use crate::network::messages::chord::{GetRequest, NodeInfo, PutRequest};
use futures::StreamExt;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;

pub struct ChordPeer {
    chord_handle: ChordHandle,
    chord_node: ChordNode,
    port: u16,
}

impl ChordPeer {
    pub async fn new(config: PeerConfig) -> Result<Self, NetworkError> {
        // Get network bits configuration
        let network_bits = config.network_bits.unwrap_or(160);

        // Create a random NodeId in the configured hash space
        let node_id = NodeId::random_with_bits(network_bits);

        // Get port for gRPC server
        let port = config.grpc_port.unwrap_or_else(|| get_random_port());

        // Create the local address
        let local_addr = format!("127.0.0.1:{}", port);

        // Create the chord node
        let chord_node = ChordNode::new(node_id, local_addr.clone()).await;

        // Create the actor and get its handle
        let (chord_handle, mut chord_actor) =
            ChordHandle::new(node_id, port, local_addr.clone()).await;

        // Spawn the actor task
        tokio::spawn(async move {
            chord_actor.run().await;
        });

        Ok(Self {
            chord_handle,
            chord_node,
            port,
        })
    }

    pub async fn join(&mut self, bootstrap_addr: String) -> Result<(), NetworkError> {
        // Join the Chord network
        self.chord_node
            .join_network(Some(bootstrap_addr))
            .await
            .map_err(NetworkError::Chord)?;

        // Share state with worker threads
        let thread_config = self.chord_node.get_shared_state();

        // Spawn worker threads
        tokio::spawn(run_stabilize_worker(thread_config.clone()));
        tokio::spawn(run_predecessor_checker(thread_config.clone()));
        tokio::spawn(run_finger_maintainer(thread_config.clone()));
        tokio::spawn(run_successor_maintainer(thread_config.clone()));

        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), NetworkError> {
        let mut stabilize_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            select! {
                _ = stabilize_interval.tick() => {
                    self.stabilize().await?;
                }
            }
        }
    }

    async fn stabilize(&mut self) -> Result<(), NetworkError> {
        // Update finger table
        for i in 0..KEY_SIZE {
            let target = self.calculate_finger_id(i);
            // Find successor for this finger
            let successor = self
                .chord_node
                .closest_preceding_node(&target.to_bytes())
                .await
                .ok_or_else(|| {
                    NetworkError::Chord(ChordError::NodeNotFound("No successor found".into()))
                })?;

            // Update finger table
            let mut finger_table = self.chord_node.finger_table.lock().await;
            finger_table.update_finger(i, successor);
        }

        // Run Chord stabilization
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
                .map_err(|e| NetworkError::Grpc(format!("Failed to create gRPC client: {}", e)))?;

            client
                .put(PutRequest {
                    key: key.0,
                    value: value.0,
                    requesting_node: Some(NodeInfo {
                        node_id: self.chord_node.node_id.to_bytes().to_vec(),
                        address: self.chord_node.local_addr.clone(),
                    }),
                })
                .await
                .map_err(|e| NetworkError::Grpc(format!("Failed to store value: {}", e)))?;

            Ok(())
        } else {
            Err(NetworkError::Chord(ChordError::NodeNotFound(
                "No responsible node found".into(),
            )))
        }
    }

    pub async fn retrieve_value(&mut self, key: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        let key = Key(key);
        let key_id = NodeId::from_key(&key.0);

        // Find the node responsible for this key
        if let Some(target_node) = self.chord_node.closest_preceding_node(&key.0).await {
            // Forward the retrieve request to the responsible node
            let target_addr = self
                .chord_node
                .get_node_address(&target_node)
                .await
                .ok_or_else(|| {
                    NetworkError::Chord(ChordError::NodeNotFound("No address found".into()))
                })?;

            let mut client = ChordGrpcClient::new(target_addr)
                .await
                .map_err(|e| NetworkError::Grpc(format!("Failed to create gRPC client: {}", e)))?;

            let response = client
                .get(GetRequest {
                    key: key.0,
                    requesting_node: Some(NodeInfo {
                        node_id: self.chord_node.node_id.to_bytes().to_vec(),
                        address: self.chord_node.local_addr.clone(),
                    }),
                })
                .await
                .map_err(|e| NetworkError::Grpc(format!("Failed to retrieve value: {}", e)))?;

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

    pub async fn create_network(&mut self) -> Result<(), NetworkError> {
        // Initialize as first node in the network
        self.chord_node
            .join_network(None)
            .await
            .map_err(NetworkError::Chord)?;

        // Share state with worker threads
        let thread_config = self.chord_node.get_shared_state();

        // Spawn worker threads
        tokio::spawn(run_stabilize_worker(thread_config.clone()));
        tokio::spawn(run_predecessor_checker(thread_config.clone()));
        tokio::spawn(run_finger_maintainer(thread_config.clone()));
        tokio::spawn(run_successor_maintainer(thread_config.clone()));

        info!(
            "Created new Chord network with node ID: {}",
            self.chord_node.node_id
        );
        Ok(())
    }
}

fn get_random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}
