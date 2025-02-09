use crate::chord::types::{NodeId, FingerTable, SharedFingerTable, KEY_SIZE, Key, Value};
use crate::chord::CHORD_PROTOCOL;
use crate::error::ConnectionDeniedError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::task::{Context, Poll};
use std::collections::VecDeque;

// Type alias for connection tracking
type ConnectionId = String;

#[derive(Debug)]
pub enum ChordRoutingEvent {
    // Routing table updates
    SuccessorUpdated(NodeId),
    PredecessorUpdated(NodeId),
    FingerUpdated(usize, NodeId),
    
    // Connection events
    ConnectionEstablished(NodeId),
    ConnectionClosed(NodeId),
    
    // Routing events
    RouteFound(NodeId, NodeId),
    RoutingError(NodeId, String),
    
    // Storage events
    KeyStored(Key),
    KeyRetrieved(Key, Value),
    KeyNotFound(Key),
}

#[derive(Debug)]
pub enum ChordRoutingAction {
    FindSuccessor(NodeId),
    UpdateFinger(usize, NodeId),
    Notify(NodeId),
    StoreKey(Key, Value),
    RetrieveKey(Key),
}

pub struct ChordRoutingBehaviour {
    // Node identity
    local_node_id: NodeId,
    
    // Routing table 
    finger_table: SharedFingerTable,
    predecessor: Option<NodeId>,
    
    // Storage
    storage: Arc<Mutex<HashMap<Key, Value>>>,
    
    // Connection tracking
    connected_peers: HashMap<NodeId, Vec<ConnectionId>>,
    
    // Event queue
    events: VecDeque<ChordRoutingEvent>,
    
    // Pending requests
    pending_requests: HashMap<NodeId, ChordRoutingAction>,
}

impl ChordRoutingBehaviour {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            finger_table: Arc::new(Mutex::new(FingerTable::new(local_node_id))),
            predecessor: None,
            storage: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: HashMap::new(),
            events: VecDeque::new(),
            pending_requests: HashMap::new(),
        }
    }

    pub async fn get_successor(&self) -> Option<NodeId> {
        self.finger_table.lock().await.get_successor()
    }

    pub async fn get_predecessor(&self) -> Option<NodeId> {
        self.predecessor
    }

    pub async fn set_successor(&mut self, peer: NodeId) {
        self.finger_table.lock().await.update_finger(0, peer);
        self.events.push_back(ChordRoutingEvent::SuccessorUpdated(peer));
    }

    pub fn set_predecessor(&mut self, peer: NodeId) {
        self.predecessor = Some(peer);
        self.events.push_back(ChordRoutingEvent::PredecessorUpdated(peer));
        
        // When predecessor changes, we need to check if any keys need to be transferred
        self.transfer_keys_if_needed();
    }

    async fn transfer_keys_if_needed(&mut self) {
        if let Some(pred) = self.predecessor {
            let pred_id = pred;  // NodeId is already the correct type
            let mut keys_to_transfer = Vec::new();
            
            // Check which keys belong to our predecessor
            let storage = self.storage.lock().await;
            for (key, value) in storage.iter() {
                let key_id = NodeId::from_key(&key.0);
                if key_id.is_between(&pred_id, &self.local_node_id) {
                    keys_to_transfer.push((key.clone(), value.clone()));
                }
            }
            drop(storage);
            
            // Transfer the keys
            for (key, value) in keys_to_transfer {
                self.store_key(key, value).await;
            }
        }
    }

    pub async fn store_key(&mut self, key: Key, value: Value) {
        let key_id = NodeId::from_key(&key.0);
        
        // Check if we're responsible for this key
        if let Some(successor) = self.get_successor().await {
            if key_id.is_between(&self.local_node_id, &successor) {
                // We're responsible for this key
                let mut storage = self.storage.lock().await;
                storage.insert(key.clone(), value);
                self.events.push_back(ChordRoutingEvent::KeyStored(key));
            } else {
                // Forward to the closest preceding node
                self.find_successor(key_id).await;
            }
        }
    }

    pub async fn retrieve_key(&mut self, key: Key) {
        let key_id = NodeId::from_key(&key.0);
        
        // Check if we have the key
        let value_opt = {
            let storage = self.storage.lock().await;
            storage.get(&key).cloned()
        };

        match value_opt {
            Some(value) => {
                self.events.push_back(ChordRoutingEvent::KeyRetrieved(key, value));
            }
            None => {
                // If we don't have the key, check if we should have it
                if let Some(successor) = self.get_successor().await {
                    if key_id.is_between(&self.local_node_id, &successor) {
                        // We should have had it but don't
                        self.events.push_back(ChordRoutingEvent::KeyNotFound(key));
                    } else {
                        // Forward to the closest preceding node
                        self.find_successor(key_id).await;
                    }
                }
            }
        }
    }

    pub async fn update_finger(&mut self, index: usize, peer: NodeId) {
        if index < KEY_SIZE {
            self.finger_table.lock().await.update_finger(index, peer);
            self.events.push_back(ChordRoutingEvent::FingerUpdated(index, peer));
        }
    }

    pub async fn find_successor(&mut self, id: NodeId) {
        let closest = {
            let table = self.finger_table.lock().await;
            table.find_closest_preceding_node(&id)
        };
        
        match closest {
            Some(next_hop) => {
                self.events.push_back(ChordRoutingEvent::RouteFound(id, next_hop));
            }
            None => {
                self.events.push_back(ChordRoutingEvent::RoutingError(
                    id,
                    "No route found".to_string(),
                ));
            }
        }
    }

    pub async fn handle_peer_discovered(&mut self, peer: NodeId) {
        // If we don't have a successor yet, or if this peer should be our successor
        if self.get_successor().await.is_none() || 
           peer.is_between(&self.local_node_id, &self.get_successor().await.unwrap()) {
            self.set_successor(peer).await;
        }
        
        // Update finger table if this peer fits in any interval
        let table = self.finger_table.lock().await;
        for (i, entry) in table.entries.iter().enumerate() {
            if peer.is_between(&entry.start, &entry.interval_end) {
                drop(table); // Release the lock before calling update_finger
                self.update_finger(i, peer).await;
                break;
            }
        }
    }

    pub async fn handle_peer_expired(&mut self, peer: &NodeId) {
        // Remove from connected peers
        self.connected_peers.remove(peer);
        
        // If this was our successor, we need to find a new one
        if let Some(successor) = self.get_successor().await {
            if &successor == peer {
                // Try to find the next best successor from our finger table
                let table = self.finger_table.lock().await;
                for entry in table.entries.iter() {
                    if let Some(node) = entry.node {
                        if node != successor {
                            drop(table);
                            self.set_successor(node).await;
                            break;
                        }
                    }
                }
            }
        }
        
        // If this was our predecessor, clear it and check keys
        if let Some(pred) = self.predecessor {
            if &pred == peer {
                self.predecessor = None;
                self.transfer_keys_if_needed().await;
            }
        }
    }
}
 