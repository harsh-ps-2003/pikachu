use tokio::sync::{mpsc, oneshot, Mutex};
use std::sync::Arc;
use crate::{
    error::ChordError, 
    chord::types::{NodeId, Key, Value, ChordNode}, 
    chord::{FINGER_TABLE_SIZE, SUCCESSOR_LIST_SIZE}
};
use crate::network::messages::chord::{GetRequest, PutRequest, NodeInfo, KeyValue, ReplicateRequest};
use crate::network::grpc::client::ChordGrpcClient;
use std::collections::HashMap;
use log::{debug, info, warn, error};
use std::time::Duration;

/// Actor Messages
/// Each message includes a oneshot sender, so the actor processes the message and sends the response back through the corresponding sender
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

/// The Actor responsible for handling chord messages
pub struct ChordActor {
    node: ChordNode,
    receiver: mpsc::Receiver<ChordMessage>,
}

impl ChordActor {
    pub async fn new(node_id: NodeId, addr: String, receiver: mpsc::Receiver<ChordMessage>) -> Self {
        Self {
            node: ChordNode::new(node_id, addr).await,
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
                if let Some(addr) = self.node.get_node_address(&bootstrap_node).await {
                    let result = self.join(bootstrap_node, addr).await;
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
                let predecessor = self.node.predecessor.lock().await.clone();
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
                let result = self.get_node_address(node).await;
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
        let successor_list = self.node.successor_list.lock().await;
        if successor_list.contains(&self.node.node_id) {
            return Err(ChordError::NodeExists(
                format!("Node {} is already part of the ring", self.node.node_id)
            ));
        }
        drop(successor_list);
        
        info!("Node {} joining through {}", self.node.node_id, bootstrap_node);
        *self.node.predecessor.lock().await = None;
        // Find successor through bootstrap node
        // Update finger table
        Ok(())
    }

    /// Finding the immediate responsible node for a key
    async fn find_successor(&self, id: NodeId) -> Result<NodeId, ChordError> {
        let successor_list = self.node.successor_list.lock().await;
        if let Some(successor) = successor_list.first() {
            if self.is_between(&id, &self.node.node_id, successor) {
                return Ok(*successor);
            }
        }
        drop(successor_list);
        
        // Otherwise, forward to closest preceding node
        let n0 = self.closest_preceding_node(id).await;
        if n0 == self.node.node_id {
            return Err(ChordError::NodeNotFound(
                format!("No suitable successor found for id {}", id)
            ));
        }
        Ok(n0)
    }

    async fn closest_preceding_node(&self, id: NodeId) -> NodeId {
        let finger_table = self.node.finger_table.lock().await;
        for i in (0..FINGER_TABLE_SIZE).rev() {
            if let Some(finger) = finger_table.entries[i].node {
                if self.is_between(&finger, &self.node.node_id, &id) {
                    return finger;
                }
            }
        }
        drop(finger_table);
        self.node.node_id
    }

    async fn notify(&mut self, node: NodeId) -> Result<(), ChordError> {
        let mut predecessor = self.node.predecessor.lock().await;
        if predecessor.is_none() || 
            self.is_between(&node, &predecessor.unwrap(), &self.node.node_id) {
            *predecessor = Some(node);
        }
        Ok(())
    }

    async fn stabilize(&mut self) -> Result<(), ChordError> {
        if let Some(successor) = self.node.get_successor().await {
            if let Some(successor_addr) = self.node.get_node_address(&successor).await {
                match ChordGrpcClient::new(successor_addr).await {
                    Ok(mut client) => {
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
                let mut finger_table = self.node.finger_table.lock().await;
                finger_table.entries[i].node = Some(successor);
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
        let key_node_id = NodeId::from_key(&key.0);
        let responsible_node = self.find_successor(key_node_id).await?;
        
        if responsible_node == self.node.node_id {
            // We are responsible for this key
            let mut storage = self.node.storage.lock().await;
            storage.insert(key, value);
            Ok(())
        } else {
            // Forward the request to the responsible node using gRPC
            let mut client = self.create_grpc_client(responsible_node).await?;
            
            let node_addresses = self.node.node_addresses.lock().await;
            let addr = node_addresses.get(&self.node.node_id)
                .ok_or_else(|| ChordError::NodeNotFound("No address found".into()))?
                .clone();
            
            client.put(PutRequest {
                key: key.0,
                value: value.0,
                requesting_node: Some(NodeInfo {
                    node_id: self.node.node_id.to_bytes().to_vec(),
                    address: addr,
                }),
            }).await;
            
            Ok(())
        }
    }

    async fn get(&self, key: &Key) -> Result<Option<Value>, ChordError> {
        let key_node_id = NodeId::from_key(&key.0);
        let responsible_node = self.find_successor(key_node_id).await?;
        
        if responsible_node == self.node.node_id {
            // We are responsible for this key
            let storage = self.node.storage.lock().await;
            Ok(storage.get(key).cloned())
        } else {
            // Forward the request to the responsible node using gRPC
            let mut client = self.create_grpc_client(responsible_node).await?;
            
            let node_addresses = self.node.node_addresses.lock().await;
            let addr = node_addresses.get(&self.node.node_id)
                .ok_or_else(|| ChordError::NodeNotFound("No address found".into()))?
                .clone();
            drop(node_addresses); // Release the lock early
            
            let response = client.get(GetRequest {
                key: key.0.clone(),
                requesting_node: Some(NodeInfo {
                    node_id: self.node.node_id.to_bytes().to_vec(),
                    address: addr,
                }),
            })
            .await
            .map_err(|e| ChordError::OperationFailed(format!("Get request failed: {}", e)))?;

            Ok(response.map(|value| Value(value.0)))
        }
    }

    // Helper method to get gRPC address for a node
    async fn get_node_address(&self, node_id: NodeId) -> Result<String, ChordError> {
        // Lock the mutex to access the HashMap
        let node_addresses = self.node.node_addresses.lock().await;
        
        // Get the address from the HashMap
        node_addresses.get(&node_id)
            .cloned()
            .ok_or_else(|| ChordError::NodeNotFound(format!("No address found for node {}", node_id)))
    }

    // When a node joins or leaves, transfer keys
    async fn transfer_keys(&mut self, from: NodeId, to: NodeId) -> Result<(), ChordError> {
        let mut keys_to_transfer = Vec::new();
        
        // Identify keys that should be transferred
        {
            let storage = self.node.storage.lock().await;
            for (key, value) in storage.iter() {
                let key_node_id = NodeId::from_key(&key.0);
                if self.is_between(&key_node_id, &from, &to) {
                    keys_to_transfer.push((key.clone(), value.clone()));
                }
            }
        }
        
        // Remove transferred keys from our storage
        {
            let mut storage = self.node.storage.lock().await;
            for (key, _) in &keys_to_transfer {
                storage.remove(key);
            }
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
                source_node: Some(NodeInfo {
                    node_id: self.node.node_id.to_bytes().to_vec(),
                    address: self.node.local_addr.clone(),
                }),
            })
            .await
            .map_err(|e| ChordError::OperationFailed(format!("Transfer failed: {}", e)))?;
        }
        
        Ok(())
    }

    async fn update_successor(&self, node: NodeId) -> Result<(), ChordError> {
        // if the immediate successor leaves, after stabalization of network, the successor should be automatically updated
        // Implementation needed
        Ok(())
    }

    async fn handle_node_departure(&mut self, node: NodeId) {
        debug!("Handling departure of node {}", node);
        
        // Remove from node addresses with proper cleanup
        {
            let mut addresses = self.node.node_addresses.lock().await;
            addresses.remove(&node);
        }

        // First check successor list state and collect necessary info
        let (is_empty, was_immediate_successor) = {
            let mut successor_list = self.node.successor_list.lock().await;
            let was_immediate = successor_list.first().map_or(false, |&s| s == node);
            
            // Remove failed node from successor list
            successor_list.retain(|&x| x != node);
            
            (successor_list.is_empty(), was_immediate)
        };

        // Handle empty successor list case
        if is_empty {
            error!("Successor list is empty after node departure");
            self.trigger_emergency_stabilization().await;
            return;
        }

        // If immediate successor failed, handle with robust retry mechanism
        if was_immediate_successor {
            self.handle_immediate_successor_failure(node).await;
        }

        // Update predecessor if needed with proper validation
        {
            let mut predecessor = self.node.predecessor.lock().await;
            if *predecessor == Some(node) {
                debug!("Clearing failed predecessor {}", node);
                *predecessor = None;
                drop(predecessor); // Explicitly drop the lock before calling find_new_predecessor
                self.find_new_predecessor().await;
            }
        }

        // Clean finger table with validation
        self.clean_finger_table(node).await;
        
        // Validate overall network state
        self.validate_network_state().await;
    }

    async fn handle_immediate_successor_failure(&mut self, failed_node: NodeId) {
        debug!("Handling immediate successor {} failure", failed_node);
        
        let mut retry_count = 0;
        let max_retries = 3;
        let mut backoff_duration = Duration::from_millis(100);
        
        while retry_count < max_retries {
            let stabilization_result = {
                let successor_list = self.node.successor_list.lock().await;
                if let Some(&next_successor) = successor_list.first() {
                    // Update finger table's immediate successor
                    let mut finger_table = self.node.finger_table.lock().await;
                    finger_table.update_finger(0, next_successor);
                    drop(finger_table);
                    
                    // Attempt stabilization with exponential backoff
                    if let Some(next_addr) = self.node.get_node_address(&next_successor).await {
                        debug!("Attempting stabilization with next successor {} (attempt {})", 
                               next_successor, retry_count + 1);
                        
                        match ChordGrpcClient::new(next_addr).await {
                            Ok(mut client) => {
                                match client.stabilize().await {
                                    Ok(_) => {
                                        debug!("Successfully stabilized with new successor {}", next_successor);
                                        true
                                    }
                                    Err(e) => {
                                        error!("Failed to stabilize with new successor (attempt {}): {}", 
                                               retry_count + 1, e);
                                        false
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to connect to new successor (attempt {}): {}", 
                                       retry_count + 1, e);
                                false
                            }
                        }
                    } else {
                        error!("No address found for next successor {}", next_successor);
                        false
                    }
                } else {
                    error!("No valid successor found in list");
                    false
                }
            };

            if stabilization_result {
                debug!("Successfully recovered from successor failure");
                return;
            }

            retry_count += 1;
            if retry_count < max_retries {
                tokio::time::sleep(backoff_duration).await;
                backoff_duration *= 2; // Exponential backoff
            }
        }

        error!("Failed to recover from successor failure after {} attempts", max_retries);
        self.trigger_emergency_stabilization().await;
    }

    async fn trigger_emergency_stabilization(&mut self) {
        debug!("Triggering emergency stabilization");
        
        // Attempt to rebuild successor list from finger table
        let mut new_successors = Vec::new();
        {
            let finger_table = self.node.finger_table.lock().await;
            for entry in &finger_table.entries {
                if let Some(node) = entry.node {
                    if !new_successors.contains(&node) {
                        new_successors.push(node);
                    }
                }
            }
        }

        if !new_successors.is_empty() {
            let mut successor_list = self.node.successor_list.lock().await;
            *successor_list = new_successors;
            successor_list.truncate(SUCCESSOR_LIST_SIZE);
            
            // Attempt to stabilize with each potential successor
            for &potential_successor in successor_list.iter() {
                if let Some(addr) = self.node.get_node_address(&potential_successor).await {
                    if let Ok(mut client) = ChordGrpcClient::new(addr).await {
                        if client.stabilize().await.is_ok() {
                            debug!("Emergency stabilization successful with node {}", potential_successor);
                            return;
                        }
                    }
                }
            }
        }

        error!("Emergency stabilization failed to find any valid successors");
    }

    async fn find_new_predecessor(&mut self) {
        debug!("Searching for new predecessor");
        
        // First collect potential nodes to check
        let potential_nodes = {
            let finger_table = self.node.finger_table.lock().await;
            let mut nodes = Vec::new();
            for entry in finger_table.entries.iter().rev() {
                if let Some(node) = entry.node {
                    nodes.push(node);
                }
            }
            nodes
        };

        // Now try each node without holding the finger table lock
        for node in potential_nodes {
            if let Some(addr) = self.node.get_node_address(&node).await {
                if let Ok(mut client) = ChordGrpcClient::new(addr).await {
                    if let Ok(pred_resp) = client.get_predecessor().await {
                        if let Some(pred_info) = pred_resp.predecessor {
                            let pred_id = NodeId::from_bytes(&pred_info.node_id);
                            let mut predecessor = self.node.predecessor.lock().await;
                            *predecessor = Some(pred_id);
                            debug!("Found new predecessor {}", pred_id);
                            return;
                        }
                    }
                }
            }
        }
        
        debug!("No suitable predecessor found");
    }

    async fn clean_finger_table(&mut self, failed_node: NodeId) {
        debug!("Cleaning finger table entries for failed node {}", failed_node);
        
        let mut finger_table = self.node.finger_table.lock().await;
        for i in 0..FINGER_TABLE_SIZE {
            if finger_table.entries[i].node == Some(failed_node) {
                // Try to find alternative node for this finger
                if let Ok(new_successor) = self.find_successor(self.calculate_finger_id(i as u8)).await {
                    finger_table.entries[i].node = Some(new_successor);
                    debug!("Updated finger {} to new node {}", i, new_successor);
                } else {
                    finger_table.entries[i].node = None;
                    debug!("Cleared finger {} as no alternative found", i);
                }
            }
        }
    }

    async fn validate_network_state(&mut self) {
        debug!("Validating network state consistency");
        
        // Validate successor list
        {
            let successor_list = self.node.successor_list.lock().await;
            if successor_list.is_empty() {
                error!("Invalid state: Empty successor list detected");
                drop(successor_list);
                self.trigger_emergency_stabilization().await;
                return;
            }
            
            // Verify immediate successor is reachable
            if let Some(&immediate_successor) = successor_list.first() {
                if let Some(addr) = self.node.get_node_address(&immediate_successor).await {
                    if let Err(e) = ChordGrpcClient::new(addr).await {
                        error!("Invalid state: Immediate successor unreachable: {}", e);
                        drop(successor_list);
                        self.handle_immediate_successor_failure(immediate_successor).await;
                        return;
                    }
                }
            }
        }

        // Validate finger table consistency
        {
            let finger_table = self.node.finger_table.lock().await;
            let mut needs_repair = false;
            
            for (i, entry) in finger_table.entries.iter().enumerate() {
                if let Some(node) = entry.node {
                    if let Some(addr) = self.node.get_node_address(&node).await {
                        if let Err(_) = ChordGrpcClient::new(addr).await {
                            debug!("Invalid finger table entry {} pointing to unreachable node {}", i, node);
                            needs_repair = true;
                        }
                    } else {
                        needs_repair = true;
                    }
                }
            }
            
            if needs_repair {
                drop(finger_table);
                self.fix_fingers().await.ok();
            }
        }
    }

    // When creating gRPC client, extract host and port from multiaddr
    async fn create_grpc_client(&self, node: NodeId) -> Result<ChordGrpcClient, ChordError> {
        let addr = self.get_node_address(node).await?;
            
        ChordGrpcClient::new(addr)
            .await
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
        
        // Get current successor first
        let current_successor = {
            let successor_list = self.node.successor_list.lock().await;
            successor_list.first().cloned()
        };
        
        // Build new successor list without holding any locks
        if let Some(mut current) = current_successor {
            new_list.push(current);
            
            // Get successors of our successor until list is full
            while new_list.len() < SUCCESSOR_LIST_SIZE {
                match self.create_grpc_client(current).await {
                    Ok(mut client) => {
                        match client.get_successor_list().await {
                            Ok(successors) => {
                                for succ_info in successors {
                                    let succ_id = NodeId::from_bytes(&succ_info.node_id);
                                    if !new_list.contains(&succ_id) && succ_id != self.node.node_id {
                                        new_list.push(succ_id);
                                        
                                        // Update node address
                                        let mut addresses = self.node.node_addresses.lock().await;
                                        addresses.insert(succ_id, succ_info.address);
                                    }
                                    if new_list.len() >= SUCCESSOR_LIST_SIZE {
                                        break;
                                    }
                                }
                                current = new_list.last().cloned().unwrap_or(current);
                            }
                            Err(e) => {
                                debug!("Failed to get successor list from {}: {}", current, e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Failed to connect to node {}: {}", current, e);
                        break;
                    }
                }
            }
        }
        
        // Only update the successor list if we found any valid successors
        if !new_list.is_empty() {
            let mut successor_list = self.node.successor_list.lock().await;
            *successor_list = new_list;
            debug!("Updated successor list with {} entries", successor_list.len());
        } else {
            debug!("No valid successors found to update the list");
        }
        
        Ok(())
    }

    async fn replicate_data(&self, keys_to_transfer: Vec<(Key, Value)>, to_node: NodeId) -> Result<(), ChordError> {
        if let Some(node_addr) = self.node.get_node_address(&to_node).await {
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
                    node_id: self.node.node_id.to_bytes().to_vec(),
                    address: self.node.local_addr.clone(),
                }),
            })
            .await
            .map_err(|e| ChordError::OperationFailed(format!("Replication failed: {}", e)))?;
        }
        
        Ok(())
    }

    async fn lookup_key(&self, key: &[u8]) -> Result<Option<Value>, ChordError> {
        // First check if we own the key
        if self.node.owns_key(key).await {
            let storage = self.node.storage.lock().await;
            return Ok(storage.get(&Key(key.to_vec())).cloned());
        }

        // Convert key to NodeId for routing
        let key_id = NodeId::from_key(key);

        // If we don't own it, find the closest preceding node or immediate successor
        let next_hop = {
            let closest = self.closest_preceding_node(key_id).await;
            if closest == self.node.node_id {
                // If closest is self, use immediate successor
                let successor_list = self.node.successor_list.lock().await;
                let next = successor_list.first()
                    .cloned()
                    .ok_or_else(|| ChordError::NodeNotFound("No route to key".into()))?;
                drop(successor_list);
                next
            } else {
                closest
            }
        };

        // Forward the lookup to the next hop
        let mut client = self.create_grpc_client(next_hop).await?;

        let response = client.get(GetRequest {
            key: key.to_vec(),
            requesting_node: Some(NodeInfo {
                node_id: self.node.node_id.to_bytes().to_vec(),
                address: self.node.local_addr.clone(),
            }),
        })
        .await
        .map_err(|e| ChordError::OperationFailed(format!("Lookup failed: {}", e)))?;

        Ok(response.map(|value| Value(value.0)))
    }
}

/// Actor handle for interacting with the ChordActor
/// The server handles gRPC requests and forwards them to the Chord actor via ChordHandle
#[derive(Clone, Debug)]
pub struct ChordHandle {
    sender: mpsc::Sender<ChordMessage>,
}

impl ChordHandle {
    pub async fn new(node_id: NodeId, grpc_port: u16, grpc_addr: String) -> (Self, ChordActor) {
        let (sender, receiver) = mpsc::channel(32);
        let actor = ChordActor::new(node_id, grpc_addr, receiver).await;
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