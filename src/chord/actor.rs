use tokio::sync::{mpsc, oneshot};
use crate::{error::ChordError, chord::types::{NodeId, Key, Value, ChordState}, chord::{FINGER_TABLE_SIZE, SUCCESSOR_LIST_SIZE}};
use crate::network::messages::chord::PutRequest;
use crate::network::grpc::client::ChordGrpcClient;
use std::collections::HashMap;
use log::{debug, info, warn};

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
    Notify {
        node: NodeId,
        respond_to: oneshot::Sender<Result<(), ChordError>>,
    },
    
    // Internal maintenance
    Stabilize,
    FixFingers,
    CheckPredecessor,
}

/// Responsible for handling messages related to the Chord protocol
pub struct ChordActor {
    state: ChordState,
    receiver: mpsc::Receiver<ChordMessage>,
}

impl ChordActor {
    pub fn new(node_id: NodeId, receiver: mpsc::Receiver<ChordMessage>) -> Self {
        Self {
            state: ChordState {
                node_id,
                predecessor: None,
                successor: None,
                successor_list: Vec::with_capacity(SUCCESSOR_LIST_SIZE),
                finger_table: vec![None; FINGER_TABLE_SIZE],
                storage: HashMap::new(),
            },
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
                let result = self.join(bootstrap_node).await;
                let _ = respond_to.send(result);
            }
            ChordMessage::FindSuccessor { id, respond_to } => {
                let result = self.find_successor(id).await;
                let _ = respond_to.send(result);
            }
            ChordMessage::GetPredecessor { respond_to } => {
                let _ = respond_to.send(Ok(self.state.predecessor));
            }
            ChordMessage::Notify { node, respond_to } => {
                let result = self.notify(node).await;
                let _ = respond_to.send(result);
            }
            ChordMessage::Put { key, value, respond_to } => {
                let result = self.put(key, value).await;
                let _ = respond_to.send(result);
            }
            ChordMessage::Get { key, respond_to } => {
                let result = self.get(&key).await;
                let _ = respond_to.send(result);
            }
            ChordMessage::Stabilize => {
                self.stabilize().await;
            }
            ChordMessage::FixFingers => {
                self.fix_fingers().await;
            }
            ChordMessage::CheckPredecessor => {
                self.check_predecessor().await;
            }
            _ => {}
        }
    }

    // Core Chord Protocol Implementation

    async fn join(&mut self, bootstrap_node: NodeId) -> Result<(), ChordError> {
        info!("Node {} joining through {}", self.state.node_id, bootstrap_node);
        self.state.predecessor = None;
        // Find successor through bootstrap node
        // Update finger table
        Ok(())
    }

    async fn find_successor(&self, id: NodeId) -> Result<NodeId, ChordError> {
        if let Some(successor) = self.state.successor {
            if self.is_between(&id, &self.state.node_id, &successor) {
                return Ok(successor);
            }
        }
        
        let n0 = self.closest_preceding_node(id);
        // Forward the request to n0
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

    async fn stabilize(&mut self) {
        debug!("Running stabilization for node {}", self.state.node_id);
        if let Some(successor) = self.state.successor {
            // Get successor's predecessor
            // Update successor if needed
            // Notify successor
        }
    }

    async fn fix_fingers(&mut self) {
        debug!("Fixing finger table for node {}", self.state.node_id);
        // Update finger table entries
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
            let mut client = ChordGrpcClient::new(self.get_node_address(responsible_node)?).await?;
            
            client.put(PutRequest {
                key: key.0,
                value: value.0,
                requesting_node: Some(NodeInfo {
                    node_id: self.state.node_id.to_bytes(),
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
            let mut client = ChordGrpcClient::new(self.get_node_address(responsible_node)?).await?;
            
            let response = client.get(GetRequest {
                key: key.0.clone(),
                requesting_node: Some(NodeInfo {
                    node_id: self.state.node_id.to_bytes(),
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
            let mut client = ChordGrpcClient::new(self.get_node_address(to)?).await?;
            
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
}

/// Actor handle for interacting with the ChordActor
#[derive(Clone)]
pub struct ChordHandle {
    sender: mpsc::Sender<ChordMessage>,
}

impl ChordHandle {
    pub fn new(node_id: NodeId) -> (Self, ChordActor) {
        let (sender, receiver) = mpsc::channel(32);
        let actor = ChordActor::new(node_id, receiver);
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

    // Add other methods for Put, Get, etc.
} 