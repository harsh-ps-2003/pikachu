use crate::error::ChordError;
use crate::network::grpc::ChordGrpcClient;
use crate::network::messages::chord::{
    GetRequest, HandoffRequest, HandoffResponse, KeyValue, NodeInfo, TransferKeysRequest,
    TransferKeysResponse,
};
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

/*
While the NodeId is used for routing and determining data responsibility in the ring,
we use direct gRPC communication with address:port for network communication
*/

// Shared state types for thread-safe access
pub type SharedStorage = Arc<Mutex<HashMap<Key, Value>>>;
pub type SharedPredecessor = Arc<Mutex<Option<NodeId>>>;
pub type SharedSuccessorList = Arc<Mutex<Vec<NodeId>>>;
pub type SharedFingerTable = Arc<Mutex<FingerTable>>;

// Number of bits in the key space (using SHA-256)
pub const KEY_SIZE: usize = 256;
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

    /// Returns the distance between the nodes
    pub fn distance(&self, other: &NodeId) -> NodeId {
        let mut result = [0u8; 32];
        for i in 0..32 {
            result[i] = self.0[i] ^ other.0[i];
        }
        NodeId(result)
    }

    /// Return the byte array corresponding to the NodeId
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Return NodeId corresponding to byte array
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut id = [0u8; 32];
        id.copy_from_slice(bytes);
        NodeId(id)
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

    pub fn new(id: [u8; 32]) -> Self {
        NodeId(id)
    }

    // Check if this node is between start and end in the ring
    pub fn is_between(&self, start: &NodeId, end: &NodeId) -> bool {
        if start == end {
            return true;
        }
        if start < end {
            self > start && self <= end
        } else {
            self > start || self <= end
        }
    }

    // Get the n-th finger (n is 0-based)
    pub fn get_finger_id(&self, n: usize) -> NodeId {
        assert!(n < KEY_SIZE);

        // Create a new array for the result
        let mut result = [0u8; 32];

        // Copy the original ID
        result.copy_from_slice(&self.0);

        // Calculate the power of 2 for this finger
        let power = 2u128.pow(n as u32);

        // Convert to big-endian bytes
        let power_bytes = power.to_be_bytes();

        // Add the power to the current ID (with overflow handling)
        let mut carry = 0u8;
        for i in (0..16).rev() {
            let sum = result[16 + i] as u16 + power_bytes[i] as u16 + carry as u16;
            result[16 + i] = sum as u8;
            carry = (sum >> 8) as u8;
        }

        // Handle any remaining carry
        if carry > 0 {
            for i in (0..16).rev() {
                let sum = result[i] as u16 + carry as u16;
                result[i] = sum as u8;
                carry = (sum >> 8) as u8;
                if carry == 0 {
                    break;
                }
            }
        }

        NodeId(result)
    }

    /// Generate a random NodeId in the hash space
    pub fn random() -> Self {
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 32];
        rng.fill_bytes(&mut bytes);
        NodeId(bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FingerEntry {
    pub start: NodeId,        // Start of the finger interval
    pub interval_end: NodeId, // End of the finger interval
    pub node: Option<NodeId>, // Node that succeeds start
}

#[derive(Debug)]
pub struct FingerTable {
    pub entries: Vec<FingerEntry>,
    pub node_id: NodeId,
}

impl FingerTable {
    pub fn new(node_id: NodeId) -> Self {
        let mut entries = Vec::with_capacity(KEY_SIZE);

        for i in 0..KEY_SIZE {
            let start = node_id.get_finger_id(i);
            let interval_end = if i == KEY_SIZE - 1 {
                node_id
            } else {
                node_id.get_finger_id(i + 1)
            };

            entries.push(FingerEntry {
                start,
                interval_end,
                node: None,
            });
        }

        FingerTable { entries, node_id }
    }

    pub fn update_finger(&mut self, index: usize, node: NodeId) {
        if index < self.entries.len() {
            self.entries[index].node = Some(node);
        }
    }

    pub fn get_successor(&self) -> Option<NodeId> {
        self.entries.first().and_then(|entry| entry.node)
    }

    pub fn find_closest_preceding_node(&self, id: &NodeId) -> Option<NodeId> {
        for entry in self.entries.iter().rev() {
            if let Some(node) = entry.node {
                if node.is_between(&self.node_id, id) {
                    return Some(node);
                }
            }
        }
        None
    }
}

// Thread configuration
#[derive(Clone, Debug)]
pub struct ThreadConfig {
    pub local_addr: String,
    pub local_node_id: NodeId,
    pub finger_table: SharedFingerTable,
    pub storage: SharedStorage,
    pub predecessor: SharedPredecessor,
    pub successor_list: SharedSuccessorList,
    pub node_addresses: Arc<Mutex<HashMap<NodeId, String>>>,
}

impl ThreadConfig {
    pub fn new(
        local_addr: String,
        local_node_id: NodeId,
        finger_table: SharedFingerTable,
        storage: SharedStorage,
        predecessor: SharedPredecessor,
        successor_list: SharedSuccessorList,
        node_addresses: Arc<Mutex<HashMap<NodeId, String>>>,
    ) -> Self {
        Self {
            local_addr,
            local_node_id,
            finger_table,
            storage,
            predecessor,
            successor_list,
            node_addresses,
        }
    }

    pub async fn get_node_addr(&self, node_id: &NodeId) -> Option<String> {
        let addresses = self.node_addresses.lock().await;
        addresses.get(node_id).cloned()
    }

    pub async fn add_node_addr(&self, node_id: NodeId, addr: String) {
        let mut addresses = self.node_addresses.lock().await;
        addresses.insert(node_id, addr);
    }
}

/// ChordNode manages the initialization and state of a Chord node
#[derive(Debug, Clone)]
pub struct ChordNode {
    // Core node identity
    pub node_id: NodeId,
    pub local_addr: String,

    // Thread-safe shared state
    pub finger_table: SharedFingerTable,
    pub storage: SharedStorage,
    pub predecessor: SharedPredecessor,
    pub successor_list: SharedSuccessorList,

    // Address management - maps NodeId to gRPC address (host:port)
    pub node_addresses: Arc<Mutex<HashMap<NodeId, String>>>,
}

impl ChordNode {
    pub async fn new(node_id: NodeId, local_addr: String) -> Self {
        let finger_table = Arc::new(Mutex::new(FingerTable::new(node_id)));
        let storage = Arc::new(Mutex::new(HashMap::new()));
        let predecessor = Arc::new(Mutex::new(None));
        let successor_list = Arc::new(Mutex::new(Vec::new()));
        let node_addresses = Arc::new(Mutex::new(HashMap::new()));

        Self {
            node_id,
            local_addr,
            finger_table,
            storage,
            predecessor,
            successor_list,
            node_addresses,
        }
    }

    pub async fn join_network(&mut self, bootstrap_addr: Option<String>) -> Result<(), ChordError> {
        if let Some(addr) = bootstrap_addr {
            // Join existing network
            let mut client = ChordGrpcClient::new(addr)
                .await
                .map_err(|e| ChordError::JoinFailed(e.to_string()))?;

            // Find our successor
            let successor_info = client
                .find_successor(self.node_id.to_bytes().to_vec())
                .await
                .map_err(|e| ChordError::JoinFailed(e.to_string()))?;

            // Initialize finger table
            self.init_finger_table(successor_info)
                .await
                .map_err(|e| ChordError::JoinFailed(e.to_string()))?;

            // Transfer keys from successor
            self.transfer_keys_from_successor()
                .await
                .map_err(|e| ChordError::JoinFailed(e.to_string()))?;
        } else {
            // Create new network
            let mut finger_table = self.finger_table.lock().await;
            for i in 0..KEY_SIZE {
                finger_table.update_finger(i, self.node_id);
            }
            drop(finger_table);

            let mut successor_list = self.successor_list.lock().await;
            successor_list.push(self.node_id);
        }

        Ok(())
    }

    async fn init_finger_table(&mut self, successor_info: NodeInfo) -> Result<(), ChordError> {
        let successor_id = NodeId::from_bytes(&successor_info.node_id);
        let successor_addr = successor_info.address;

        // Set immediate successor
        {
            let mut finger_table = self.finger_table.lock().await;
            finger_table.update_finger(0, successor_id);
        }

        // Initialize successor list
        {
            let mut successor_list = self.successor_list.lock().await;
            successor_list.push(successor_id);
        }

        // Create client for successor
        let mut successor_client = ChordGrpcClient::new(successor_addr)
            .await
            .map_err(|e| ChordError::JoinFailed(e.to_string()))?;

        // Initialize remaining fingers
        for i in 1..KEY_SIZE {
            let finger_id = self.node_id.get_finger_id(i);

            // If finger_id is between us and our successor, use successor
            if finger_id.is_between(&self.node_id, &successor_id) {
                let mut finger_table = self.finger_table.lock().await;
                finger_table.update_finger(i, successor_id);
            } else {
                // Otherwise, find the appropriate node
                let finger_info = successor_client
                    .find_successor(finger_id.to_bytes().to_vec())
                    .await
                    .map_err(|e| ChordError::JoinFailed(e.to_string()))?;

                let mut finger_table = self.finger_table.lock().await;
                finger_table.update_finger(i, NodeId::from_bytes(&finger_info.node_id));
            }
        }

        Ok(())
    }

    /// Transfer keys from successor when joining the network
    async fn transfer_keys_from_successor(&mut self) -> Result<(), ChordError> {
        let successor_id = {
            let finger_table = self.finger_table.lock().await;
            finger_table
                .get_successor()
                .ok_or_else(|| ChordError::JoinFailed("No successor found".into()))?
        };

        let successor_addr = self.get_node_address(&successor_id).await.ok_or_else(|| {
            ChordError::NodeNotFound(format!("No address for successor {}", successor_id))
        })?;

        let mut client = ChordGrpcClient::new(successor_addr)
            .await
            .map_err(|e| ChordError::JoinFailed(e.to_string()))?;

        // Create a channel for receiving data from successor
        let (tx, mut rx) = tokio::sync::mpsc::channel::<KeyValue>(32);
        client.set_handoff_channel(tx);

        // Start the handoff request in a separate task
        let handoff_task = tokio::spawn(async move {
            client
                .request_handoff()
                .await
                .map_err(|e| ChordError::JoinFailed(format!("Handoff failed: {}", e)))
        });

        // Process received key-value pairs
        let storage = self.storage.clone();
        while let Some(kv) = rx.recv().await {
            let mut storage = storage.lock().await;
            storage.insert(Key(kv.key), Value(kv.value));
        }

        // Wait for handoff task to complete
        match handoff_task.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(ChordError::JoinFailed(format!(
                "Handoff task failed: {}",
                e
            ))),
        }
    }

    pub fn get_shared_state(&self) -> ThreadConfig {
        ThreadConfig::new(
            self.local_addr.clone(),
            self.node_id,
            self.finger_table.clone(),
            self.storage.clone(),
            self.predecessor.clone(),
            self.successor_list.clone(),
            self.node_addresses.clone(),
        )
    }

    pub async fn get_node_address(&self, node_id: &NodeId) -> Option<String> {
        let addresses = self.node_addresses.lock().await;
        addresses.get(node_id).cloned()
    }

    pub async fn add_known_node(&mut self, node_id: NodeId, addr: String) {
        {
            let mut addresses = self.node_addresses.lock().await;
            addresses.insert(node_id, addr);
        }

        // Update finger table if needed
        let mut finger_table = self.finger_table.lock().await;
        for i in 0..KEY_SIZE {
            let finger_id = self.node_id.get_finger_id(i);
            if finger_id.is_between(&self.node_id, &node_id) {
                finger_table.update_finger(i, node_id);
            }
        }
    }

    pub async fn get_successor(&self) -> Option<NodeId> {
        let successor_list = self.successor_list.lock().await;
        successor_list.first().cloned()
    }

    /// Check if this node owns the given key
    pub async fn owns_key(&self, key: &[u8]) -> bool {
        let key_id = NodeId::from_key(key);
        let predecessor = self.predecessor.lock().await;

        match *predecessor {
            Some(pred) => {
                // We own the key if it's in range (predecessor, our_id]
                key_id.is_between(&pred, &self.node_id)
            }
            None => true, // If we have no predecessor, we own everything
        }
    }

    /// Find the closest preceding node for a given key from our finger table
    pub async fn closest_preceding_node(&self, key: &[u8]) -> Option<NodeId> {
        let key_id = NodeId::from_key(key);
        let finger_table = self.finger_table.lock().await;

        // Check finger table entries from highest to lowest
        for i in (0..KEY_SIZE).rev() {
            if let Some(node) = finger_table.entries[i].node {
                // Check if finger is between us and the target key
                if node.is_between(&self.node_id, &key_id) {
                    return Some(node);
                }
            }
        }
        None
    }

    /// Lookup a key in the DHT
    pub async fn lookup_key(&self, key: &[u8]) -> Result<Option<Value>, ChordError> {
        // First check if we own the key
        if self.owns_key(key).await {
            let storage = self.storage.lock().await;
            return Ok(storage.get(&Key(key.to_vec())).cloned());
        }

        // If we don't own it, find the closest preceding node
        let next_hop = match self.closest_preceding_node(key).await {
            Some(node) => node,
            None => {
                // If no closer node found, try our immediate successor
                let successor_list = self.successor_list.lock().await;
                match successor_list.first() {
                    Some(succ) => *succ,
                    None => return Err(ChordError::NodeNotFound("No route to key".into())),
                }
            }
        };

        // Forward the lookup to the next hop
        let grpc_addr = self
            .get_node_address(&next_hop)
            .await
            .ok_or_else(|| ChordError::NodeNotFound(format!("No address for node {}", next_hop)))?;

        let mut client = ChordGrpcClient::new(grpc_addr)
            .await
            .map_err(|e| ChordError::OperationFailed(format!("Failed to connect: {}", e)))?;

        // Make the recursive lookup call
        let result = client
            .get(GetRequest {
                key: key.to_vec(),
                requesting_node: Some(NodeInfo {
                    node_id: self.node_id.to_bytes().to_vec(),
                    address: self.local_addr.clone(),
                }),
            })
            .await
            .map_err(|e| ChordError::OperationFailed(format!("Lookup failed: {}", e)))?;

        Ok(result)
    }

    /// Perform a streaming handoff of all data to the successor node during shutdown
    pub async fn handoff_data(&self) -> Result<(), ChordError> {
        let successor_id = {
            let successor_list = self.successor_list.lock().await;
            successor_list
                .first()
                .cloned()
                .ok_or_else(|| ChordError::OperationFailed("No successor found".into()))?
        };

        let successor_addr = self.get_node_address(&successor_id).await.ok_or_else(|| {
            ChordError::NodeNotFound(format!("No address for successor {}", successor_id))
        })?;

        // Create gRPC client for successor
        let mut client = ChordGrpcClient::new(successor_addr).await.map_err(|e| {
            ChordError::OperationFailed(format!("Failed to connect to successor: {}", e))
        })?;

        // Create a channel for streaming data
        let (tx, rx) = tokio::sync::mpsc::channel::<KeyValue>(32); // Buffer size of 32 for flow control

        // Spawn a task to stream data from storage
        let storage = self.storage.clone();
        tokio::spawn(async move {
            // Get all key-value pairs while holding the lock
            let key_value_pairs = {
                let storage = storage.lock().await;
                storage
                    .iter()
                    .map(|(key, value)| KeyValue {
                        key: key.0.clone(),
                        value: value.0.clone(),
                    })
                    .collect::<Vec<_>>()
            }; // MutexGuard is dropped here

            // Stream the collected pairs
            for kv in key_value_pairs {
                if tx.send(kv).await.is_err() {
                    break; // Receiver dropped, stop sending
                }
            }
        });

        // Stream the data to successor using client streaming
        let response = client
            .handoff(rx)
            .await
            .map_err(|e| ChordError::OperationFailed(format!("Handoff failed: {}", e)))?;

        if !response.success {
            return Err(ChordError::OperationFailed(format!(
                "Handoff failed: {}",
                response.error
            )));
        }

        // Clear our storage after successful handoff
        let mut storage = self.storage.lock().await;
        storage.clear();

        Ok(())
    }

    /// Transfer keys to target node (used for both join and leave scenarios)
    pub async fn transfer_keys(
        &self,
        target_id: NodeId,
        target_addr: String,
        is_leaving: bool,
    ) -> Result<(), ChordError> {
        let mut client = ChordGrpcClient::new(target_addr)
            .await
            .map_err(|e| ChordError::OperationFailed(format!("Failed to connect: {}", e)))?;

        // Create a channel for streaming data
        let (tx, rx) = tokio::sync::mpsc::channel::<KeyValue>(32);

        // Spawn a task to stream keys that should be transferred
        let storage = self.storage.clone();
        let node_id = self.node_id;
        tokio::spawn(async move {
            // Get all key-value pairs that need to be transferred while holding the lock
            let key_value_pairs = {
                let storage = storage.lock().await;
                storage
                    .iter()
                    .filter_map(|(key, value)| {
                        let key_id = NodeId::from_key(&key.0);
                        if key_id.is_between(&node_id, &target_id) || is_leaving {
                            Some(KeyValue {
                                key: key.0.clone(),
                                value: value.0.clone(),
                            })
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            }; // MutexGuard is dropped here

            // Stream the collected pairs
            for kv in key_value_pairs {
                if tx.send(kv).await.is_err() {
                    break; // Receiver dropped, stop sending
                }
            }
        });

        // Stream the data to target node
        let response = client
            .handoff(rx)
            .await
            .map_err(|e| ChordError::OperationFailed(format!("Handoff failed: {}", e)))?;

        if !response.success {
            return Err(ChordError::OperationFailed(response.error));
        }

        // If we're leaving or the transfer was successful, remove transferred keys
        if is_leaving {
            let mut storage = self.storage.lock().await;
            storage.clear();
        } else {
            let mut storage = self.storage.lock().await;
            storage.retain(|key, _| {
                let key_id = NodeId::from_key(&key.0);
                !key_id.is_between(&self.node_id, &target_id)
            });
        }

        Ok(())
    }

    pub async fn find_successor(&self, id: NodeId) -> Result<NodeId, ChordError> {
        let successor_list = self.successor_list.lock().await;
        if let Some(successor) = successor_list.first() {
            if id.is_between(&self.node_id, successor) {
                return Ok(*successor);
            }
        }
        drop(successor_list);

        // Otherwise, forward to closest preceding node
        if let Some(closest) = self.closest_preceding_node(&id.to_bytes()).await {
            if closest == self.node_id {
                // If we're the closest, return our successor
                let successor_list = self.successor_list.lock().await;
                return successor_list
                    .first()
                    .cloned()
                    .ok_or_else(|| ChordError::NodeNotFound("No successor found".into()));
            }

            // Forward the query to the closest preceding node
            if let Some(addr) = self.get_node_address(&closest).await {
                let mut client = ChordGrpcClient::new(addr).await.map_err(|e| {
                    ChordError::OperationFailed(format!("Failed to connect: {}", e))
                })?;

                // Get the node info from the response
                let node_info = client
                    .find_successor(id.to_bytes().to_vec())
                    .await
                    .map_err(|e| {
                        ChordError::OperationFailed(format!("Failed to find successor: {}", e))
                    })?;

                // Convert NodeInfo to NodeId
                return Ok(NodeId::from_bytes(&node_info.node_id));
            }
        }

        Err(ChordError::NodeNotFound(
            "No suitable successor found".into(),
        ))
    }
}

/// Key type for storing data in the DHT
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key(pub Vec<u8>);

/// Value type for storing data in the DHT
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(pub Vec<u8>);
