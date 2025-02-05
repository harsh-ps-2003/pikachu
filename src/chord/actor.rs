use tokio::sync::{mpsc, oneshot};
use crate::{error::ChordError, chord::types::{NodeId, Key, Value, ChordState}, chord::{FINGER_TABLE_SIZE, SUCCESSOR_LIST_SIZE}};
use crate::network::messages::chord::{GetRequest, PutRequest, NodeInfo, KeyValue};
use crate::network::messages::message::ReplicateRequest;
use crate::network::grpc::client::ChordGrpcClient;
use std::collections::HashMap;
use log::{debug, info, warn};
use libp2p::multiaddr::{Protocol, Multiaddr};

// Actor Messages
#[derive(Debug)]
pub enum ChordMessage {
    // Node operations
    Join {
        bootstrap_node: NodeId,
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
    Leave {
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
    
    // DHT operations
    Put {
        key: Key,
        value: Value,
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
    Get {
        key: Key,
        respond_to: oneshot::Sender<Result<Option<Value>, ChordError>>,
    },
    
    // Chord protocol messages
    FindSuccessor {
        id: NodeId,
        respond_to: oneshot::Sender<Result<NodeId, ChordError>>,
    },
    GetPredecessor {
        respond_to: oneshot::Sender<Result<Option<NodeId>, ChordError>>,
    },
    
    // Internal maintenance
    Stabilize {
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
    FixFingers,
    CheckPredecessor,
    UpdateSuccessor {
        node: NodeId,
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
    GetNodeAddress {
        node: NodeId,
        respond_to: oneshot::Sender<Result<String, ChordError>>,
    },
    NodeLeft {
        node: NodeId,
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
    DiscoveredPeer {
        node_id: NodeId,
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
    PeerExpired {
        node_id: NodeId,
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
}

/// Responsible for handling messages related to the Chord protocol
pub struct ChordActor {
    state: ChordState,
    receiver: mpsc::Receiver<ChordMessage>,
}

impl ChordActor {
    pub fn new(node_id: NodeId, addr: Multiaddr, receiver: mpsc::Receiver<ChordMessage>) -> Self {
        Self {
            state: ChordState::new(node_id, addr),
            receiver,
        }
    }

    pub async fn run(&mut self) {
        let mut stabilize_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        let mut fix_fingers_interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        let mut check_predecessor_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    self.handle_message(msg).await;
                }
                _ = stabilize_interval.tick() => {
                    self.stabilize().await;
                }
                _ = fix_fingers_interval.tick() => {
                    self.fix_fingers().await;
                }
                _ = check_predecessor_interval.tick() => {
                    self.check_predecessor().await;
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: ChordMessage) {
        match msg {
            ChordMessage::Join { bootstrap_node, respond_to } => {
                if let Some(bootstrap_addr) = self.state.get_node_address(&bootstrap_node) {
                    let result = self.join(bootstrap_node, bootstrap_addr).await;
                    let _ = respond_to.send(result);
                } else {
                    let _ = respond_to.send(Err(ChordError::NodeNotFound(
                        format!("Bootstrap node {} not found", bootstrap_node)
                    )));
                }
            }
            ChordMessage::FindSuccessor { id, respond_to } => {
                let result = self.find_successor(id).await
                    .map_err(|_| ChordError::NodeNotFound(
                        format!("Could not find successor for id {}", id)
                    ));
                let _ = respond_to.send(result);
            }
            ChordMessage::GetPredecessor { respond_to } => {
                let _ = respond_to.send(Ok(self.state.predecessor));
            }
            ChordMessage::Put { key, value, respond_to } => {
                let result = self.put(key, value).await;
                let _ = respond_to.send(result);
            }
            ChordMessage::Get { key, respond_to } => {
                let result = self.get(&key).await;
                let _ = respond_to.send(result);
            }
            ChordMessage::Stabilize { respond_to } => {
                let result = self.stabilize().await;
                let _ = respond_to.send(result);
            }
            ChordMessage::FixFingers => {
                self.fix_fingers().await;
            }
            ChordMessage::CheckPredecessor => {
                self.check_predecessor().await;
            }
            ChordMessage::UpdateSuccessor { node, respond_to } => {
                let result = self.update_successor(node).await;
                let _ = respond_to.send(result);
            }
            ChordMessage::GetNodeAddress { node, respond_to } => {
                let result = self.state.get_node_address(&node)
                    .map(|addr| self.state.multiaddr_to_string(addr))
                    .ok_or_else(|| ChordError::NodeNotFound(
                        format!("No address found for node {}", node)
                    ));
                let _ = respond_to.send(result);
            }
            ChordMessage::NodeLeft { node, respond_to } => {
                debug!("Node {} left the network", node);
                self.handle_node_departure(node).await;
                let _ = respond_to.send(Ok(()));
            }
            ChordMessage::DiscoveredPeer { node_id, respond_to } => {
                if self.should_track_node(&node_id) {
                    self.state.add_known_node(node_id);
                }
                let _ = respond_to.send(Ok(()));
            }
            ChordMessage::PeerExpired { node_id, respond_to } => {
                self.handle_node_departure(node_id).await;
                let _ = respond_to.send(Ok(()));
            }
            _ => {}
        }
    }

    // Core Chord Protocol Implementation

    async fn join(&mut self, bootstrap_node: NodeId, bootstrap_addr: String) -> Result<(), ChordError> {
        if self.state.successor_list[0] {
            return Err(ChordError::NodeExists(
                format!("Node {} is already part of the ring", self.state.node_id)
            ));
        }
        
        info!("Node {} joining through {}", self.state.node_id, bootstrap_node);
        self.state.predecessor = None;
        // Find successor through bootstrap node
        // Update finger table
        Ok(())
    }

    /// Finding the immediate responsible node for a key
    async fn find_successor(&self, id: NodeId) -> Result<NodeId, ChordError> {
        if let Some(successor) = self.state.successor {
            if self.is_between(&id, &self.state.node_id, &successor) {
                return Ok(successor);
            }
        }
        
        // // Otherwise, forward to closest preceding node
        let n0 = self.closest_preceding_node(id);
        if n0 == self.state.node_id {
            return Err(ChordError::NodeNotFound(
                format!("No suitable successor found for id {}", id)
            ));
        }
        Ok(n0)
    }

    fn closest_preceding_node(&self, id: NodeId) -> NodeId {
        for i in (0..FINGER_TABLE_SIZE).rev() {
            if let Some(finger) = self.state.finger_table[i] {
                if self.is_between(&finger, &self.state.node_id, &id) {
                    return finger;
                }
            }
        }
        self.state.node_id
    }

    async fn notify(&mut self, node: NodeId) -> Result<(), ChordError> {
        if self.state.predecessor.is_none() || 
           self.is_between(&node, &self.state.predecessor.unwrap(), &self.state.node_id) {
            self.state.predecessor = Some(node);
        }
        Ok(())
    }

    async fn stabilize(&mut self) -> Result<(), ChordError> {
        if let Some(successor) = self.state.successor() {
            if let Some(successor_addr) = self.state.get_node_address(&successor) {
                let grpc_addr = self.state.multiaddr_to_string(successor_addr);
                match ChordGrpcClient::new(&grpc_addr).await {
                    Ok(_) => {
                        // Successor is alive, proceed with stabilization
                        // ... existing stabilization logic ...
                    }
                    Err(_) => {
                        // Successor is dead, remove it and use next successor
                        self.handle_node_departure(successor).await;
                    }
                }
            }
        }
        Ok(())
    }

    /// Calculate the start of the kth finger interval
    /// k is 0-based in our implementation, but 1-based in the paper
    fn calculate_finger_id(&self, k: u8) -> NodeId {
        self.state.node_id.add_power_of_two(k)
    }

    async fn fix_fingers(&mut self) -> Result<(), ChordError> {
        for i in 0..FINGER_TABLE_SIZE {
            let finger_id = self.calculate_finger_id(i as u8);
            if let Ok(successor) = self.find_successor(finger_id).await {
                self.state.finger_table[i] = Some(successor);
            }
        }
        Ok(())
    }

    async fn check_predecessor(&mut self) {
        debug!("Checking predecessor for node {}", self.state.node_id);
        // Check if predecessor is still alive
    }

    fn is_between(&self, id: &NodeId, start: &NodeId, end: &NodeId) -> bool {
        if start <= end {
            id > start && id <= end
        } else {
            id > start || id <= end
        }
    }

    async fn put(&mut self, key: Key, value: Value) -> Result<(), ChordError> {
        // Get the position in the ring corresponding to the key 
        let key_node_id = NodeId::from_key(&key.0);
        
        // Find the successor responsible for this key
        let responsible_node = self.find_successor(key_node_id).await?;
        
        if responsible_node == self.state.node_id {
            // We are responsible for this key
            self.state.storage.insert(key, value);
            Ok(())
        } else {
            // Forward the request to the responsible node using gRPC
            let mut client = self.create_grpc_client(responsible_node)?;
            
            client.put(PutRequest {
                key: key.0,
                value: value.0,
                requesting_node: Some(NodeInfo {
                    node_id: self.state.node_id.to_bytes().to_vec(),
                    address: self.state.address.clone(),
                }),
            }).await?;
            
            Ok(())
        }
    }

    async fn get(&self, key: &Key) -> Result<Option<Value>, ChordError> {
        // Get the position in the ring corresponding to the key
        let key_node_id = NodeId::from_key(&key.0);
        
        // Find the successor responsible for this key
        let responsible_node = self.find_successor(key_node_id).await?;
        
        if responsible_node == self.state.node_id {
            // We are responsible for this key
            Ok(self.state.storage.get(key).cloned())
        } else {
            // Forward the request to the responsible node using gRPC
            let mut client = self.create_grpc_client(responsible_node)?;
            
            let response = client.get(GetRequest {
                key: key.0.clone(),
                requesting_node: Some(NodeInfo {
                    node_id: self.state.node_id.to_bytes().to_vec(),
                    address: self.state.address.clone(),
                }),
            }).await?;
            
            if response.success {
                Ok(Some(Value(response.value)))
            } else {
                Ok(None)
            }
        }
    }

    // Helper method to get gRPC address for a node
    fn get_node_address(&self, node_id: NodeId) -> Result<String, ChordError> {
        // This would typically look up the address in a mapping maintained by the node
        // For now, we'll assume we have this information in state
        self.state.node_addresses.get(&node_id)
            .cloned()
            .ok_or_else(|| ChordError::NodeNotFound(format!("No address found for node {}", node_id)))
    }

    // When a node joins or leaves, transfer keys
    async fn transfer_keys(&mut self, from: NodeId, to: NodeId) -> Result<(), ChordError> {
        let mut keys_to_transfer = Vec::new();
        
        // Identify keys that should be transferred
        for (key, value) in self.state.storage.iter() {
            let key_node_id = NodeId::from_key(&key.0);
            if self.is_between(&key_node_id, &from, &to) {
                keys_to_transfer.push((key.clone(), value.clone()));
            }
        }
        
        // Remove transferred keys from our storage
        for (key, _) in &keys_to_transfer {
            self.state.storage.remove(key);
        }
        
        // Send keys to the new responsible node 
        if !keys_to_transfer.is_empty() {
            let mut client = self.create_grpc_client(to)?;
            
            // Convert keys to KeyValue proto messages
            let kv_data: Vec<KeyValue> = keys_to_transfer.into_iter()
                .map(|(k, v)| KeyValue {
                    key: k.0,
                    value: v.0,
                })
                .collect();

            // Send ReplicateRequest via gRPC
            client.replicate(ReplicateRequest {
                data: kv_data,
                requesting_node: Some(NodeInfo {
                    node_id: self.state.node_id.to_bytes(),
                    address: self.state.address.clone(),
                }),
            }).await?;
        }
        
        Ok(())
    }

    async fn update_successor(&self, node: NodeId) -> Result<(), ChordError> {
        // if the immediate successor leaves, after stabalization of network, the successor should be automatically updated
        // Implementation needed
        Ok(())
    }

    async fn handle_node_departure(&mut self, node: NodeId) {
        // Complete cleanup - this node is gone forever
        self.state.remove_node_address(&node);

        if self.state.successor_list[0] == Some(node) {
            // Need to find new successor
            if let Some(next_successor) = self.state.successor_list.first().cloned() {
                self.state.successor[0] = Some(next_successor);
            } else {
                self.state.successor = None;
            }
        }

        if self.state.predecessor == Some(node) {
            // Will be fixed by next stabilization
            self.state.predecessor = None;
        }

        // Clean finger table
        for finger in self.state.finger_table.iter_mut() {
            if *finger == Some(node) {
                *finger = None;
            }
        }

        // Remove from successor list
        self.state.successor_list.retain(|&x| x != node);
    }

    // When creating gRPC client, extract host and port from multiaddr
    async fn create_grpc_client(&self, node: NodeId) -> Result<ChordGrpcClient, ChordError> {
        let addr = self.state.get_multiaddr(&node)
            .ok_or_else(|| ChordError::NodeNotFound(format!("No address for node {}", node)))?;
            
        let grpc_addr = ChordState::to_grpc_addr(addr)?;
        
        ChordGrpcClient::new(&grpc_addr).await
            .map_err(|e| ChordError::InvalidRequest(format!("Failed to create gRPC client: {}", e)))
    }

    fn should_track_node(&self, node_id: &NodeId) -> bool {
        // Implement Chord-specific logic for determining if we should
        // track this node (e.g., based on ID distance, ring position, etc.)
        true
    }

    /// Updates the successor list during stabilization
    async fn update_successor_list(&mut self) -> Result<(), ChordError> {
        let mut new_list = Vec::with_capacity(SUCCESSOR_LIST_SIZE);
        
        // Start with our immediate successor
        if let Some(mut current) = self.state.successor() {
            new_list.push(current);
            
            // Get successors of our successor until list is full
            while new_list.len() < SUCCESSOR_LIST_SIZE {
                match self.get_successor_list(current).await {
                    Ok(next_successors) => {
                        for succ in next_successors {
                            if !new_list.contains(&succ) {
                                new_list.push(succ);
                                if new_list.len() >= SUCCESSOR_LIST_SIZE {
                                    break;
                                }
                            }
                        }
                        if let Some(next) = next_successors.first() {
                            current = *next;
                        } else {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
        
        self.state.successor_list = new_list;
        Ok(())
    }
}

/// Actor handle for interacting with the ChordActor
#[derive(Clone)]
pub struct ChordHandle {
    sender: mpsc::Sender<ChordMessage>,
}

impl ChordHandle {
    pub fn new(node_id: NodeId, grpc_port: u16, grpc_addr: String) -> (Self, ChordActor) {
        let (sender, receiver) = mpsc::channel(32);
        let actor = ChordActor::new(node_id, grpc_addr.parse().unwrap(), receiver);
        (Self { sender }, actor)
    }

    pub async fn join(&self, bootstrap_node: NodeId) -> Result<(), ChordError> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(ChordMessage::Join {
                bootstrap_node,
                respond_to: send,
            })
            .await
            .map_err(|_| ChordError::InvalidRequest("Actor is dead".into()))?;
        recv.await.map_err(|_| ChordError::InvalidRequest("Actor died during request".into()))?
    }

    pub async fn find_successor(&self, id: NodeId) -> Result<NodeId, ChordError> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(ChordMessage::FindSuccessor {
                id,
                respond_to: send,
            })
            .await
            .map_err(|_| ChordError::InvalidRequest("Actor is dead".into()))?;
        recv.await.map_err(|_| ChordError::InvalidRequest("Actor died during request".into()))?
    }

    pub async fn stabilize(&self) -> Result<(), ChordError> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(ChordMessage::Stabilize {
                respond_to: send,
            })
            .await
            .map_err(|_| ChordError::InvalidRequest("Actor is dead".into()))?;
        recv.await.map_err(|_| ChordError::InvalidRequest("Actor died during request".into()))?
    }

    pub async fn update_successor(&self, node: NodeId) -> Result<(), ChordError> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(ChordMessage::UpdateSuccessor {
                node,
                respond_to: send,
            })
            .await
            .map_err(|_| ChordError::InvalidRequest("Actor is dead".into()))?;
        recv.await.map_err(|_| ChordError::InvalidRequest("Actor died during request".into()))?
    }

    // Add other methods for Put, Get, etc.
} 