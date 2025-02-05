use crate::chord::types::NodeId;
use crate::chord::CHORD_PROTOCOL;
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
        NetworkBehaviourAction,
    },
    PeerId,
    Multiaddr,
};
use std::collections::HashMap;
use std::task::{Context, Poll};

#[derive(Debug)]
pub enum ChordRoutingEvent {
    // Routing table updates
    SuccessorUpdated(PeerId),
    PredecessorUpdated(PeerId),
    FingerUpdated(u8, PeerId),
    
    // Connection events
    ConnectionEstablished(PeerId),
    ConnectionClosed(PeerId),
    
    // Routing events
    RouteFound(NodeId, PeerId),
    RoutingError(NodeId, String),
}

#[derive(Debug)]
pub enum ChordRoutingAction {
    FindSuccessor(NodeId),
    UpdateFinger(u8, NodeId),
    Notify(PeerId),
}

pub struct ChordRoutingBehaviour {
    // Routing table 
    successor: Option<PeerId>,
    predecessor: Option<PeerId>,
    finger_table: HashMap<u8, PeerId>,
    
    // Connection tracking
    connected_peers: HashMap<PeerId, Vec<ConnectionId>>,
    
    // Pending requests
    pending_requests: HashMap<NodeId, ChordRoutingAction>,
}

impl ChordRoutingBehaviour {
    pub fn new() -> Self {
        Self {
            successor: None,
            predecessor: None,
            finger_table: HashMap::new(),
            connected_peers: HashMap::new(),
            pending_requests: HashMap::new(),
        }
    }

    pub fn set_successor(&mut self, peer: PeerId) {
        self.successor = Some(peer);
    }

    pub fn set_predecessor(&mut self, peer: PeerId) {
        self.predecessor = Some(peer);
    }

    pub fn update_finger(&mut self, index: u8, peer: PeerId) {
        self.finger_table.insert(index, peer);
    }

    pub fn find_closest_preceding_node(&self, id: &NodeId) -> Option<PeerId> {
        for i in (0..8).rev() {
            if let Some(peer) = self.finger_table.get(&i) {
                if self.is_between(peer, &self.local_node_id(), id) {
                    return Some(*peer);
                }
            }
        }
        self.successor
    }

    fn is_between(&self, peer: &PeerId, start: &NodeId, end: &NodeId) -> bool {
        let peer_id = NodeId::from(peer.to_bytes());
        peer_id > *start && peer_id <= *end
    }

    fn local_node_id(&self) -> NodeId {
        // Implementation needed
        unimplemented!()
    }

    pub fn handle_peer_expired(&mut self, peer_id: &PeerId) {
        // Implementation needed
        unimplemented!()
    }
}

impl NetworkBehaviour for ChordRoutingBehaviour {
    type ConnectionHandler = libp2p::swarm::dummy::ConnectionHandler;
    type ToSwarm = ChordRoutingEvent;

    fn handle_established_connection(
        &mut self,
        peer_id: PeerId,
        conn: ConnectionId,
        role_override: Option<libp2p::core::Endpoint>,
        _: Option<&Vec<u8>>,
    ) {
        // Add protocol identifier to connection metadata
        let protocol_version = CHORD_PROTOCOL.to_vec();
        self.connected_peers
            .entry(peer_id)
            .or_default()
            .push(conn);
    }

    fn handle_pending_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _maybe_peer: Option<PeerId>,
        _addresses: &[Multiaddr],
        _effective_role: libp2p::core::Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        Ok(Vec::new())
    }

    fn handle_pending_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        Ok(())
    }

    fn handle_connection_closed(
        &mut self,
        peer_id: PeerId,
        conn: ConnectionId,
        _: libp2p::core::Endpoint,
        _: Option<&Vec<u8>>,
    ) {
        if let Some(connections) = self.connected_peers.get_mut(&peer_id) {
            connections.retain(|c| c != &conn);
            if connections.is_empty() {
                self.connected_peers.remove(&peer_id);
            }
        }
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        Poll::Pending
    }

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        Ok(())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        peer: PeerId,
        _addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionDenied {
    #[error("Connection denied: {0}")]
    Custom(String),
} 