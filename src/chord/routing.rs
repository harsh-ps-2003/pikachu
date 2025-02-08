use crate::chord::types::{NodeId, FingerTable, SharedFingerTable, KEY_SIZE, Key, Value};
use crate::chord::CHORD_PROTOCOL;
use crate::error::ConnectionDeniedError;
use libp2p::{
    swarm::{
        NetworkBehaviour,
        ConnectionHandler,
        ConnectionId,
        FromSwarm,
        ToSwarm,
        THandler,
        THandlerInEvent,
        THandlerOutEvent,
        ConnectionDenied,
        PortUse,
    },
    PeerId,
    Multiaddr,
    core::Endpoint,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::collections::VecDeque;

#[derive(Debug)]
pub enum ChordRoutingEvent {
    // Routing table updates
    SuccessorUpdated(PeerId),
    PredecessorUpdated(PeerId),
    FingerUpdated(usize, PeerId),
    
    // Connection events
    ConnectionEstablished(PeerId),
    ConnectionClosed(PeerId),
    
    // Routing events
    RouteFound(NodeId, PeerId),
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
    Notify(PeerId),
    StoreKey(Key, Value),
    RetrieveKey(Key),
}

pub struct ChordRoutingBehaviour {
    // Node identity
    local_node_id: NodeId,
    
    // Routing table 
    finger_table: SharedFingerTable,
    predecessor: Option<PeerId>,
    
    // Storage
    storage: Arc<Mutex<HashMap<Key, Value>>>,
    
    // Connection tracking
    connected_peers: HashMap<PeerId, Vec<ConnectionId>>,
    
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

    pub fn get_successor(&self) -> Option<PeerId> {
        self.finger_table.lock().unwrap().get_successor()
    }

    pub fn get_predecessor(&self) -> Option<PeerId> {
        self.predecessor
    }

    pub fn set_successor(&mut self, peer: PeerId) {
        self.finger_table.lock().unwrap().update_finger(0, peer);
        self.events.push_back(ChordRoutingEvent::SuccessorUpdated(peer));
    }

    pub fn set_predecessor(&mut self, peer: PeerId) {
        self.predecessor = Some(peer);
        self.events.push_back(ChordRoutingEvent::PredecessorUpdated(peer));
        
        // When predecessor changes, we need to check if any keys need to be transferred
        self.transfer_keys_if_needed();
    }

    fn transfer_keys_if_needed(&mut self) {
        if let Some(pred) = self.predecessor {
            let pred_id = NodeId::from_peer_id(&pred);
            let mut keys_to_transfer = Vec::new();
            
            // Check which keys belong to our predecessor
            let storage = self.storage.lock().unwrap();
            for (key, value) in storage.iter() {
                let key_id = NodeId::from_bytes(&key.0);
                if key_id.is_between(&pred_id, &self.local_node_id) {
                    keys_to_transfer.push((key.clone(), value.clone()));
                }
            }
            drop(storage);
            
            // Transfer the keys
            for (key, value) in keys_to_transfer {
                self.store_key(key, value);
            }
        }
    }

    pub fn store_key(&mut self, key: Key, value: Value) {
        let key_id = NodeId::from_bytes(&key.0);
        
        // Check if we're responsible for this key
        if let Some(successor) = self.get_successor() {
            let successor_id = NodeId::from_peer_id(&successor);
            if key_id.is_between(&self.local_node_id, &successor_id) {
                // We're responsible for this key
                self.storage.lock().unwrap().insert(key.clone(), value);
                self.events.push_back(ChordRoutingEvent::KeyStored(key));
            } else {
                // Forward to the closest preceding node
                self.find_successor(key_id);
            }
        }
    }

    pub fn retrieve_key(&mut self, key: Key) {
        let key_id = NodeId::from_bytes(&key.0);
        
        // Check if we have the key
        let storage = self.storage.lock().unwrap();
        match storage.get(&key) {
            Some(value) => {
                self.events.push_back(ChordRoutingEvent::KeyRetrieved(key, value.clone()));
            }
            None => {
                // If we don't have the key, check if we should have it
                if let Some(successor) = self.get_successor() {
                    let successor_id = NodeId::from_peer_id(&successor);
                    if key_id.is_between(&self.local_node_id, &successor_id) {
                        // We should have had it but don't
                        self.events.push_back(ChordRoutingEvent::KeyNotFound(key));
                    } else {
                        // Forward to the closest preceding node
                        self.find_successor(key_id);
                    }
                }
            }
        }
    }

    pub fn update_finger(&mut self, index: usize, peer: PeerId) {
        if index < KEY_SIZE {
            self.finger_table.lock().unwrap().update_finger(index, peer);
            self.events.push_back(ChordRoutingEvent::FingerUpdated(index, peer));
        }
    }

    pub fn find_successor(&mut self, id: NodeId) {
        let closest = {
            let table = self.finger_table.lock().unwrap();
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

    pub fn handle_peer_discovered(&mut self, peer: PeerId) {
        let peer_id = NodeId::from_peer_id(&peer);
        
        // If we don't have a successor yet, or if this peer should be our successor
        if self.get_successor().is_none() || peer_id.is_between(&self.local_node_id, &NodeId::from_peer_id(&self.get_successor().unwrap())) {
            self.set_successor(peer);
        }
        
        // Update finger table if this peer fits in any interval
        let table = self.finger_table.lock().unwrap();
        for (i, entry) in table.entries.iter().enumerate() {
            if peer_id.is_between(&entry.start, &entry.interval_end) {
                drop(table); // Release the lock before calling update_finger
                self.update_finger(i, peer);
                break;
            }
        }
    }

    pub fn handle_peer_expired(&mut self, peer: &PeerId) {
        // Remove from connected peers
        self.connected_peers.remove(peer);
        
        // If this was our successor, we need to find a new one
        if let Some(successor) = self.get_successor() {
            if &successor == peer {
                // Try to find the next best successor from our finger table
                let table = self.finger_table.lock().unwrap();
                for entry in table.entries.iter() {
                    if let Some(node) = entry.node {
                        if node != successor {
                            drop(table);
                            self.set_successor(node);
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
                self.transfer_keys_if_needed();
            }
        }
    }
}

impl NetworkBehaviour for ChordRoutingBehaviour {
    type ConnectionHandler = libp2p::swarm::dummy::ConnectionHandler;
    type ToSwarm = ChordRoutingEvent;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.connected_peers.entry(peer)
            .or_default()
            .push(connection_id);
        self.events.push_back(ChordRoutingEvent::ConnectionEstablished(peer));
        Ok(libp2p::swarm::dummy::ConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.connected_peers.entry(peer)
            .or_default()
            .push(connection_id);
        self.events.push_back(ChordRoutingEvent::ConnectionEstablished(peer));
        Ok(libp2p::swarm::dummy::ConnectionHandler)
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        // Handle swarm events if needed
    }

    fn on_connection_handler_event(
        &mut self,
        _peer_id: PeerId,
        _connection_id: ConnectionId,
        _event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        // Handle connection events
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ToSwarm::GenerateEvent(event));
        }
        Poll::Pending
    }
} 