use tokio::sync::{mpsc, oneshot};
use crate::{
    error::ChordError, 
    chord::types::{NodeId, Key, Value, ChordNode}, 
    chord::{FINGER_TABLE_SIZE, SUCCESSOR_LIST_SIZE}
};
use crate::network::messages::chord::{GetRequest, PutRequest, NodeInfo, KeyValue, ReplicateRequest};
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
    NodeUnavailable {
        node: NodeId,
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
}

/// Responsible for handling messages related to the Chord protocol
pub struct ChordActor {
    node: ChordNode,
    receiver: mpsc::Receiver<ChordMessage>,
}

impl ChordActor {
    pub fn new(node_id: NodeId, addr: Multiaddr, receiver: mpsc::Receiver<ChordMessage>) -> Self {
        Self {
            node: ChordNode::new(node_id, format!("http://127.0.0.1:{}", get_port_from_multiaddr(&addr).unwrap())),
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
                if let Some(addr) = self.node.get_multiaddr(&bootstrap_node) {
                    // Convert Multiaddr to gRPC address string
                    let bootstrap_addr = self.node.to_grpc_addr(&addr)
                        .map_err(|e| ChordError::InvalidRequest(format!("Invalid address: {}", e)))
                        .unwrap_or_default();
                    
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
                let predecessor = self.node.predecessor.lock().unwrap().clone();
                let _ = respond_to.send(Ok(predecessor));
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
                let result = self.node.get_multiaddr(&node)
                    .map(|addr| self.node.to_grpc_addr(addr))
                    .transpose()
                    .map_err(|e| ChordError::InvalidRequest(format!("Invalid address: {}", e)))
                    .and_then(|maybe_addr| maybe_addr.ok_or_else(|| 
                        ChordError::NodeNotFound(format!("No address found for node {}", node))
                    ));
                let _ = respond_to.send(result);
            }
            ChordMessage::NodeUnavailable { node, respond_to } => {
                debug!("Node {} is no longer available", node);
                self.handle_node_departure(node).await;
                let _ = respond_to.send(Ok(()));
            }
            _ => {}
        }
    }

    // Core Chord Protocol Implementation

    async fn join(&mut self, bootstrap_node: NodeId, bootstrap_addr: String) -> Result<(), ChordError> {
        if self.node.successor_list.contains(&self.node.node_id) {
            return Err(ChordError::NodeExists(
                format!("Node {} is already part of the ring", self.node.node_id)
            ));
        }
        
        info!("Node {} joining through {}", self.node.node_id, bootstrap_node);
        self.node.predecessor = None;
        // Find successor through bootstrap node
        // Update finger table
        Ok(())
    }

    /// Finding the immediate responsible node for a key
    async fn find_successor(&self, id: NodeId) -> Result<NodeId, ChordError> {
        if let Some(successor) = self.node.successor_list.first() {
            if self.is_between(&id, &self.node.node_id, &successor) {
                return Ok(*successor);
            }
        }
        
        // // Otherwise, forward to closest preceding node
        let n0 = self.closest_preceding_node(id);
        if n0 == self.node.node_id {
            return Err(ChordError::NodeNotFound(
                format!("No suitable successor found for id {}", id)
            ));
        }
        Ok(n0)
    }

    fn closest_preceding_node(&self, id: NodeId) -> NodeId {
        for i in (0..FINGER_TABLE_SIZE).rev() {
            if let Some(finger) = self.node.finger_table[i] {
                if self.is_between(&finger, &self.node.node_id, &id) {
                    return finger;
                }
            }
        }
        self.node.node_id
    }

    async fn notify(&mut self, node: NodeId) -> Result<(), ChordError> {
        if self.node.predecessor.lock().unwrap().is_none() || 
            self.is_between(&node, &self.node.predecessor.lock().unwrap().unwrap(), &self.node.node_id) {
            self.node.predecessor = Some(node);
        }
        Ok(())
    }

    async fn stabilize(&mut self) -> Result<(), ChordError> {
        if let Some(successor) = self.node.successor() {
            if let Some(successor_addr) = self.node.get_multiaddr(&successor) {
                let grpc_addr = self.node.to_grpc_addr(&successor_addr)
                    .map_err(|e| ChordError::InvalidRequest(format!("Invalid address: {}", e)))
                    .unwrap_or_default();
                match ChordGrpcClient::new(grpc_addr.clone()).await {
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
        self.node.node_id.add_power_of_two(k)
    }

    async fn fix_fingers(&mut self) -> Result<(), ChordError> {
        for i in 0..FINGER_TABLE_SIZE {
            let finger_id = self.calculate_finger_id(i as u8);
            if let Ok(successor) = self.find_successor(finger_id).await {
                self.node.finger_table[i] = Some(successor);
            }
        }
        Ok(())
    }

    async fn check_predecessor(&mut self) {
        debug!("Checking predecessor for node {}", self.node.node_id);
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
        
        if responsible_node == self.node.node_id {
            // We are responsible for this key
            self.node.storage.insert(key, value);
            Ok(())
        } else {
            // Forward the request to the responsible node using gRPC
            let mut client = self.create_grpc_client(responsible_node).await?;
            
            client.put(PutRequest {
                key: key.0,
                value: value.0,
                requesting_node: Some(NodeInfo {
                    node_id: self.node.node_id.to_bytes().to_vec(),
                    address: self.node.node_addresses.get(&self.node.node_id).unwrap().clone(),
                }),
            }).await;
            
            Ok(())
        }
    }

    async fn get(&self, key: &Key) -> Result<Option<Value>, ChordError> {
        // Get the position in the ring corresponding to the key
        let key_node_id = NodeId::from_key(&key.0);
        
        // Find the successor responsible for this key
        let responsible_node = self.find_successor(key_node_id).await?;
        
        if responsible_node == self.node.node_id {
            // We are responsible for this key
            Ok(self.node.storage.get(key).cloned())
        } else {
            // Forward the request to the responsible node using gRPC
            let mut client = self.create_grpc_client(responsible_node)?;
            
            let response = client.get(GetRequest {
                key: key.0.clone(),
                requesting_node: Some(NodeInfo {
                    node_id: self.node.node_id.to_bytes().to_vec(),
                    address: self.node.node_addresses.get(&self.node.node_id).unwrap().clone(),
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
        self.node.node_addresses.get(&node_id)
            .cloned()
            .ok_or_else(|| ChordError::NodeNotFound(format!("No address found for node {}", node_id)))
    }

    // When a node joins or leaves, transfer keys
    async fn transfer_keys(&mut self, from: NodeId, to: NodeId) -> Result<(), ChordError> {
        let mut keys_to_transfer = Vec::new();
        
        // Identify keys that should be transferred
        for (key, value) in self.node.storage.iter() {
            let key_node_id = NodeId::from_key(&key.0);
            if self.is_between(&key_node_id, &from, &to) {
                keys_to_transfer.push((key.clone(), value.clone()));
            }
        }
        
        // Remove transferred keys from our storage
        for (key, _) in &keys_to_transfer {
            self.node.storage.remove(key);
        }
        
        // Send keys to the new responsible node 
        if !keys_to_transfer.is_empty() {
            let mut client = self.create_grpc_client(to).await?;
            
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
                    node_id: self.node.node_id.to_bytes(),
                    address: self.node.address.clone(),
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
        self.node.remove_node_address(&node);

        if self.node.successor_list[0] == node {
            // Need to find new successor
            if let Some(next_successor) = self.node.successor_list.first().cloned() {
                self.node.successor_list[0] = next_successor;
            } else {
                self.node.successor_list = Vec::new();
            }
        }

        if self.node.predecessor == Some(node) {
            // Will be fixed by next stabilization
            self.node.predecessor = None;
        }

        // Clean finger table
        for finger in self.node.finger_table.iter_mut() {
            if *finger == Some(node) {
                *finger = None;
            }
        }

        // Remove from successor list
        self.node.successor_list.retain(|&x| x != node);
    }

    // When creating gRPC client, extract host and port from multiaddr
    async fn create_grpc_client(&self, node: NodeId) -> Result<ChordGrpcClient, ChordError> {
        let addr = self.node.get_multiaddr(&node)
            .ok_or_else(|| ChordError::NodeNotFound(format!("No address for node {}", node)))?;
            
        let grpc_addr = self.node.to_grpc_addr(addr)?;
        
        ChordGrpcClient::new(grpc_addr.clone()).await
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
        if let Some(mut current) = self.node.successor() {
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
        
        self.node.successor_list = new_list;
        Ok(())
    }

    async fn replicate_data(&self, keys_to_transfer: Vec<(Key, Value)>, to_node: NodeId) -> Result<(), ChordError> {
        if let Some(node_addr) = self.node.get_multiaddr(&to_node) {
            let mut client = self.create_grpc_client(to_node).await?;
            
            let kv_data = keys_to_transfer.into_iter()
                .map(|(k, v)| KeyValue {
                    key: k.0,
                    value: v.0,
                })
                .collect::<Vec<_>>();
            
            // Send ReplicateRequest via gRPC
            client.replicate(ReplicateRequest {
                data: kv_data,
                source_node: Some(NodeInfo {
                    id: self.node.node_id.to_bytes().to_vec(),
                    addr: self.node.local_addr.clone(),
                }),
            }).await?;
        }
        
        Ok(())
    }
}

/// Actor handle for interacting with the ChordActor
/// The server handles gRPC requests and forwards them to the Chord actor via ChordHandle
#[derive(Clone, Debug)]
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