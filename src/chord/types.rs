use sha2::{Sha256, Digest};
use std::fmt;
use std::collections::HashMap;
use crate::error::ChordError;
use libp2p::{PeerId, Multiaddr, Protocol};

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

    // Add this method to handle modular arithmetic in the ID space
    pub fn add_power_of_two(&self, k: u8) -> Self {
        let mut result = [0u8; 32];
        let mut hasher = Sha256::new();
        
        // Convert node ID and 2^k to big-endian bytes
        let node_bytes = self.0;
        let power = 1u32 << k;
        let power_bytes = power.to_be_bytes();
        
        // Hash the concatenation to get (n + 2^k) mod 2^256
        hasher.update(&node_bytes);
        hasher.update(&power_bytes);
        result.copy_from_slice(&hasher.finalize());
        
        NodeId(result)
    }
}

impl From<PeerId> for NodeId {
    fn from(peer_id: PeerId) -> Self {
        Self::from_peer_id(&peer_id)
    }
}

/// ChordState represents the local state within our Chord node.
pub struct ChordState {
    pub node_id: NodeId,
    pub predecessor: Option<NodeId>,
    pub successor_list: Vec<NodeId>,
    pub finger_table: Vec<Option<NodeId>>,
    pub storage: HashMap<Key, Value>,
    pub node_addresses: HashMap<NodeId, Multiaddr>,  // Single source of truth for node addresses
}

impl ChordState {
    pub fn new(node_id: NodeId, addr: Multiaddr) -> Self {
        let mut state = Self {
            node_id,
            predecessor: None,
            successor_list: Vec::new(),
            finger_table: vec![None; 256],
            storage: HashMap::new(),
            node_addresses: HashMap::new(),
        };
        state.node_addresses.insert(node_id, addr);
        state
    }

    /// Get the multiaddr for a node if it exists
    pub fn get_multiaddr(&self, node_id: &NodeId) -> Option<&Multiaddr> {
        self.node_addresses.get(node_id)
    }

    /// Helper to get the immediate successor from the successor list
    pub fn successor(&self) -> Option<NodeId> {
        self.successor_list.first().copied()
    }

    /// Get gRPC address from multiaddr
    pub fn to_grpc_addr(addr: &Multiaddr) -> Result<String, ChordError> {
        let mut host = None;
        let mut port = None;
        
        for proto in addr.iter() {
            match proto {
                Protocol::Ip4(ip) => host = Some(ip.to_string()),
                Protocol::Tcp(p) => port = Some(p),
                _ => {}
            }
        }
        
        match (host, port) {
            (Some(h), Some(p)) => Ok(format!("http://{}:{}", h, p)),
            _ => Err(ChordError::InvalidRequest("Invalid multiaddr format".into()))
        }
    }

    pub fn add_known_node(&mut self, node_id: NodeId) {
        // Update finger tables, successor lists etc. as needed
        // This is purely Chord protocol state management
    }
}

/// Key type for storing data in the DHT
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key(pub Vec<u8>);

/// Value type for storing data in the DHT
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(pub Vec<u8>);