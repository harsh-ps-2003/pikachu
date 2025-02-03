use sha2::{Sha256, Digest};
use std::fmt;
use std::collections::HashMap;
use crate::error::ChordError;
use libp2p::PeerId;

/*
While the NodeId is used for routing and determining data responsibility in the ring, 
the PeerId is still essential for working with network layer.
*/

/// NodeId represents a unique position in the Chord ring for a particular Node
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId([u8; 32]);

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", hex::encode(&self.0[..8]))
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

impl NodeId {
    /// Creates NodeId from key bytes to determine their responsible node
    pub fn from_key(key: &[u8]) -> Self {
        // keys are hashed into the same numerical space as nodes
        let mut hasher = Sha256::new();
        hasher.update(key);
        let result = hasher.finalize();
        let mut id = [0u8; 32];
        id.copy_from_slice(&result);
        NodeId(id)
    }

    // Create NodeId from PeerId to get the Node's position in chord ring
    pub fn from_peer_id(peer_id: &PeerId) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(peer_id.to_bytes());
        let result = hasher.finalize();
        let mut id = [0u8; 32];
        id.copy_from_slice(&result);
        NodeId(id)
    }

    /// Returns the distance between the nodes
    /// Both PeerID and key will be in same identifier space (NodeID) and the NodeIDs are compared to find distance between them in the Chord ring, and find the correct positioning of keys in the ring
    pub fn distance(&self, other: &NodeId) -> NodeId {
        let mut result = [0u8; 32];
        for i in 0..32 {
            result[i] = self.0[i] ^ other.0[i];
        }
        NodeId(result)
    }

    /// Return the byte array corresponding to the NodeID
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Return NodeID corresponding to byte arrayß
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut id = [0u8; 32];
        id.copy_from_slice(bytes);
        NodeId(id)
    }

    // Helper to convert back to PeerId if needed
    pub fn to_peer_id(&self) -> Option<PeerId> {
        PeerId::from_bytes(&self.0).ok()
    }
}

impl From<PeerId> for NodeId {
    fn from(peer_id: PeerId) -> Self {
        Self::from_peer_id(&peer_id)
    }
}

pub struct ChordState {
    pub node_id: NodeId,
    pub grpc_port: u16,  // Each node's gRPC server port
    pub address: String, // Format: "http://127.0.0.1:{grpc_port}"
    pub predecessor: Option<NodeId>,
    pub successor: Option<NodeId>,
    pub successor_list: Vec<NodeId>,
    pub finger_table: Vec<Option<NodeId>>,
    pub storage: HashMap<Key, Value>,
    pub node_addresses: HashMap<NodeId, String>,  // Mapping of NodeId to gRPC addresses
}

/// Key type for storing data in the DHT
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key(pub Vec<u8>);

/// Value type for storing data in the DHT
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(pub Vec<u8>);