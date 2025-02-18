use crate::chord::types::{ChordNode, Key, NodeId, ThreadConfig, Value};
use crate::network::grpc::client::ChordGrpcClient;
use crate::network::messages::chord::{
    chord_node_server::{ChordNode as ChordNodeService, ChordNodeServer},
    FindSuccessorRequest, FindSuccessorResponse, FixFingerRequest, FixFingerResponse,
    GetPredecessorRequest, GetPredecessorResponse, GetRequest, GetResponse,
    GetSuccessorListRequest, GetSuccessorListResponse, HandoffRequest, HandoffResponse,
    HeartbeatRequest, HeartbeatResponse, JoinRequest, JoinResponse, KeyValue, LookupRequest,
    LookupResponse, NodeInfo, NotifyRequest, NotifyResponse, PutRequest, PutResponse,
    ReplicateRequest, ReplicateResponse, StabilizeRequest, StabilizeResponse, TransferKeysRequest,
    TransferKeysResponse,
};
use chrono::Utc;
use futures::Stream;
use futures::StreamExt;
use log::{debug, error, info};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};

#[derive(Debug, Clone)]
pub struct ChordGrpcServer {
    node: Arc<ChordNode>,
    config: ThreadConfig,
}

impl ChordGrpcServer {
    pub fn new(node: Arc<ChordNode>, config: ThreadConfig) -> Self {
        Self { node, config }
    }
}

#[tonic::async_trait]
impl ChordNodeService for ChordGrpcServer {
    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let req = request.into_inner();
        let key_id = NodeId::from_key(&req.key);

        match self.node.find_successor(key_id).await {
            Ok(node) => {
                let addr = self
                    .node
                    .get_node_address(&node)
                    .await
                    .ok_or_else(|| Status::internal("Node address not found"))?;

                Ok(Response::new(LookupResponse {
                    responsible_node: Some(NodeInfo {
                        node_id: node.to_bytes().to_vec(),
                        address: addr,
                    }),
                    success: true,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(LookupResponse {
                responsible_node: None,
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let key = Key(req.key);

        // First check if we own the key
        let key_id = NodeId::from_key(&key.0);
        match self.node.find_successor(key_id).await {
            Ok(responsible_node) => {
                if responsible_node == self.node.node_id {
                    // We are responsible for this key
                    let storage = self.node.storage.lock().await;
                    match storage.get(&key) {
                        Some(value) => Ok(Response::new(GetResponse {
                            value: value.0.clone(),
                            success: true,
                            error: String::new(),
                        })),
                        None => Ok(Response::new(GetResponse {
                            value: Vec::new(),
                            success: false,
                            error: "Key not found".to_string(),
                        })),
                    }
                } else {
                    // Forward to responsible node
                    let addr = self.node.get_node_address(&responsible_node).await
                        .ok_or_else(|| Status::internal("Responsible node address not found"))?;

                    let mut client = ChordGrpcClient::new(addr)
                        .await
                        .map_err(|e| Status::internal(format!("Failed to connect to responsible node: {}", e)))?;

                    match client.get(GetRequest {
                        key: key.0,
                        requesting_node: req.requesting_node,
                    }).await {
                        Ok(Some(value)) => Ok(Response::new(GetResponse {
                            value: value.0,
                            success: true,
                            error: String::new(),
                        })),
                        Ok(None) => Ok(Response::new(GetResponse {
                            value: Vec::new(),
                            success: false,
                            error: "Key not found".to_string(),
                        })),
                        Err(e) => Ok(Response::new(GetResponse {
                            value: Vec::new(),
                            success: false,
                            error: e.to_string(),
                        })),
                    }
                }
            }
            Err(e) => Ok(Response::new(GetResponse {
                value: Vec::new(),
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let key = Key(req.key);
        let value = Value(req.value);

        // First check if we own the key
        let key_id = NodeId::from_key(&key.0);
        match self.node.find_successor(key_id).await {
            Ok(responsible_node) => {
                if responsible_node == self.node.node_id {
                    // We are responsible for this key
                    let mut storage = self.node.storage.lock().await;
                    storage.insert(key, value);
                    Ok(Response::new(PutResponse {
                        success: true,
                        error: String::new(),
                    }))
                } else {
                    // Forward to responsible node
                    let addr = self.node.get_node_address(&responsible_node).await
                        .ok_or_else(|| Status::internal("Responsible node address not found"))?;

                    let mut client = ChordGrpcClient::new(addr)
                        .await
                        .map_err(|e| Status::internal(format!("Failed to connect to responsible node: {}", e)))?;

                    match client.put(PutRequest {
                        key: key.0,
                        value: value.0,
                        requesting_node: req.requesting_node,
                    }).await {
                        Ok(()) => Ok(Response::new(PutResponse {
                            success: true,
                            error: String::new(),
                        })),
                        Err(e) => Ok(Response::new(PutResponse {
                            success: false,
                            error: e.to_string(),
                        })),
                    }
                }
            }
            Err(e) => Ok(Response::new(PutResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let req = request.into_inner();
        let joining_node = req
            .joining_node
            .ok_or_else(|| Status::invalid_argument("Missing joining node info"))?;
        let node_id = NodeId::from_bytes(&joining_node.node_id);

        // If we're the bootstrap node and alone in the network
        let mut successor_list = self.config.successor_list.lock().await;
        let is_bootstrap_alone =
            successor_list.len() == 1 && successor_list[0] == self.node.node_id;

        if is_bootstrap_alone {
            // We become the joining node's successor
            // And the joining node becomes our predecessor
            successor_list.clear();
            successor_list.push(node_id);
            successor_list.push(self.node.node_id);

            let mut predecessor = self.config.predecessor.lock().await;
            *predecessor = Some(node_id);

            let mut addresses = self.config.node_addresses.lock().await;
            addresses.insert(node_id, joining_node.address.clone());

            debug!("Bootstrap node accepting join from {}", node_id);

            return Ok(Response::new(JoinResponse {
                success: true,
                successor: Some(NodeInfo {
                    node_id: self.node.node_id.to_bytes().to_vec(),
                    address: self.config.local_addr.clone(),
                }),
                predecessor: None, // New node starts with no predecessor
                transferred_data: Vec::new(),
                error: String::new(),
            }));
        }

        // Normal join case - find the appropriate position in the ring
        if successor_list.is_empty() || node_id.is_between(&self.node.node_id, &successor_list[0]) {
            // The joining node should be between us and our current successor
            let current_successor = successor_list[0];
            successor_list.insert(0, node_id);
            successor_list.truncate(3); // Keep max 3 successors

            let mut predecessor = self.config.predecessor.lock().await;
            if predecessor.is_none() {
                *predecessor = Some(node_id);
            }

            let mut addresses = self.config.node_addresses.lock().await;
            addresses.insert(node_id, joining_node.address.clone());

            let successor_addr = addresses
                .get(&current_successor)
                .cloned()
                .ok_or_else(|| Status::internal("Successor address not found"))?;

            debug!(
                "Node {} joined between {} and {}",
                node_id, self.node.node_id, current_successor
            );

            Ok(Response::new(JoinResponse {
                success: true,
                successor: Some(NodeInfo {
                    node_id: current_successor.to_bytes().to_vec(),
                    address: successor_addr,
                }),
                predecessor: Some(NodeInfo {
                    node_id: self.node.node_id.to_bytes().to_vec(),
                    address: self.config.local_addr.clone(),
                }),
                transferred_data: Vec::new(),
                error: String::new(),
            }))
        } else {
            // We're not the right node to handle this join
            Ok(Response::new(JoinResponse {
                success: false,
                successor: None,
                predecessor: None,
                transferred_data: Vec::new(),
                error: "Not the correct position in ring".to_string(),
            }))
        }
    }

    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> Result<Response<NotifyResponse>, Status> {
        let req = request.into_inner();
        let predecessor = req
            .predecessor
            .ok_or_else(|| Status::invalid_argument("Missing predecessor info"))?;
        let node_id = NodeId::from_bytes(&predecessor.node_id);

        debug!(
            "Received notify from node {} (local node: {})",
            node_id, self.node.node_id
        );

        let mut pred_lock = self.config.predecessor.lock().await;
        let current_pred = *pred_lock;

        debug!(
            "Current state - predecessor: {:?}, local node: {}",
            current_pred, self.node.node_id
        );

        // Update predecessor if:
        // 1. We have no predecessor, or
        // 2. The new node is between our current predecessor and us
        let should_update = if current_pred.is_none() {
            debug!("No current predecessor, will accept new predecessor");
            true
        } else if let Some(curr_pred) = current_pred {
            let is_between = node_id.is_between(&curr_pred, &self.node.node_id);
            debug!(
                "Checking if {} is between current predecessor {} and local node {}: {}",
                node_id, curr_pred, self.node.node_id, is_between
            );
            is_between
        } else {
            false
        };

        if should_update {
            debug!(
                "Updating predecessor: {:?} -> {} (node: {})",
                current_pred, node_id, self.node.node_id
            );

            *pred_lock = Some(node_id);

            // Store the node's address
            let mut addresses = self.config.node_addresses.lock().await;
            addresses.insert(node_id, predecessor.address.clone());

            debug!("Node {} state after predecessor update:", self.node.node_id);
            debug!("  - Predecessor: {}", node_id);
            debug!("  - Address: {}", predecessor.address);

            // Log successor information for context
            let successor_list = self.config.successor_list.lock().await;
            if let Some(succ) = successor_list.first() {
                debug!("  - Successor: {}", succ);
            }
        } else {
            debug!(
                "Rejected predecessor update: current={:?}, proposed={} (node: {})",
                current_pred, node_id, self.node.node_id
            );
        }

        Ok(Response::new(NotifyResponse {
            accepted: should_update,
            error: if !should_update {
                "Predecessor update rejected".to_string()
            } else {
                String::new()
            },
        }))
    }

    async fn stabilize(
        &self,
        _request: Request<StabilizeRequest>,
    ) -> Result<Response<StabilizeResponse>, Status> {
        let predecessor = self.config.predecessor.lock().await;

        // Get predecessor info if it exists
        let pred_info = if let Some(p) = predecessor.as_ref() {
            let addr = self.config.get_node_addr(p).await.unwrap_or_default();
            Some(NodeInfo {
                node_id: p.to_bytes().to_vec(),
                address: addr,
            })
        } else {
            None
        };

        Ok(Response::new(StabilizeResponse {
            predecessor: pred_info,
            success: true,
            error: String::new(),
        }))
    }

    async fn find_successor(
        &self,
        request: Request<FindSuccessorRequest>,
    ) -> Result<Response<FindSuccessorResponse>, Status> {
        let req = request.into_inner();
        let id = NodeId::from_bytes(&req.id);

        match self.node.find_successor(id).await {
            Ok(successor) => {
                let addr = self
                    .node
                    .get_node_address(&successor)
                    .await
                    .ok_or_else(|| Status::internal("Successor address not found"))?;

                Ok(Response::new(FindSuccessorResponse {
                    successor: Some(NodeInfo {
                        node_id: successor.to_bytes().to_vec(),
                        address: addr,
                    }),
                    success: true,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(FindSuccessorResponse {
                successor: None,
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn get_predecessor(
        &self,
        _request: Request<GetPredecessorRequest>,
    ) -> Result<Response<GetPredecessorResponse>, Status> {
        let predecessor = self.config.predecessor.lock().await;

        // Get predecessor info if it exists
        let pred_info = if let Some(p) = predecessor.as_ref() {
            let addr = self.config.get_node_addr(p).await.unwrap_or_default();
            Some(NodeInfo {
                node_id: p.to_bytes().to_vec(),
                address: addr,
            })
        } else {
            None
        };

        Ok(Response::new(GetPredecessorResponse {
            predecessor: pred_info,
            success: true,
            error: String::new(),
        }))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse {
            alive: true,
            timestamp: Utc::now().timestamp() as u64,
        }))
    }

    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let req = request.into_inner();
        let mut storage = self.config.storage.lock().await;

        // Store all received key-value pairs
        for kv in req.data {
            storage.insert(Key(kv.key), Value(kv.value));
        }

        Ok(Response::new(ReplicateResponse {
            success: true,
            error: String::new(),
        }))
    }

    async fn transfer_keys(
        &self,
        request: Request<TransferKeysRequest>,
    ) -> Result<Response<TransferKeysResponse>, Status> {
        let req = request.into_inner();
        let target_id = NodeId::from_bytes(&req.target_id);
        let incoming_keys_len = req.keys.len();

        // Lock storage to handle key transfers
        let mut storage = self.config.storage.lock().await;
        let mut transferred_data = Vec::new();

        // Handle incoming keys if any (for join/leave operations)
        for kv in req.keys {
            let key_value = KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            };
            storage.insert(Key(kv.key), Value(kv.value));
            transferred_data.push(key_value);
        }
        debug!("Stored {} transferred keys", transferred_data.len());

        // If this is a join operation, find keys that should be transferred to the new node
        if req.is_join {
            // First collect all keys
            let all_keys: Vec<_> = storage
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            // Then check each key
            for (key, value) in all_keys {
                let key_id = NodeId::from_key(&key.0);
                if self.node.owns_key(&key.0).await
                    && key_id.is_between(&self.node.node_id, &target_id)
                {
                    storage.remove(&key);
                    transferred_data.push(KeyValue {
                        key: key.0,
                        value: value.0,
                    });
                }
            }
            debug!(
                "Transferring {} keys to joining node",
                transferred_data.len()
            );
        }

        // If this is a leave operation, we've already handled the keys
        if req.is_leave {
            debug!("Accepted {} keys from leaving node", incoming_keys_len);
        }

        Ok(Response::new(TransferKeysResponse {
            success: true,
            transferred_data,
            error: String::new(),
        }))
    }

    async fn handoff(
        &self,
        request: Request<Streaming<KeyValue>>,
    ) -> Result<Response<HandoffResponse>, Status> {
        let mut stream = request.into_inner();
        let mut keys_transferred = 0;
        let storage = self.node.storage.clone();

        // Process incoming key-value pairs
        while let Some(kv) = stream.next().await {
            match kv {
                Ok(kv) => {
                    let mut storage = storage.lock().await;
                    storage.insert(Key(kv.key), Value(kv.value));
                    keys_transferred += 1;
                }
                Err(e) => {
                    return Ok(Response::new(HandoffResponse {
                        success: false,
                        keys_transferred: keys_transferred as u32,
                        error: format!("Stream error: {}", e),
                    }));
                }
            }
        }

        Ok(Response::new(HandoffResponse {
            success: true,
            keys_transferred: keys_transferred as u32,
            error: String::new(),
        }))
    }

    type RequestHandoffStream = Pin<Box<dyn Stream<Item = Result<KeyValue, Status>> + Send>>;

    async fn request_handoff(
        &self,
        request: Request<HandoffRequest>,
    ) -> Result<Response<Self::RequestHandoffStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(32);

        // Clone necessary data for the async task
        let storage = self.node.storage.clone();

        // Spawn a task to stream the data
        tokio::spawn(async move {
            let storage = storage.lock().await;

            // Stream all key-value pairs
            for (key, value) in storage.iter() {
                let kv = KeyValue {
                    key: key.0.clone(),
                    value: value.0.clone(),
                };

                if tx.send(Ok(kv)).await.is_err() {
                    break; // Client disconnected
                }
            }

            Ok::<_, Status>(())
        });

        // Convert the channel receiver into a stream
        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::RequestHandoffStream
        ))
    }

    async fn get_successor_list(
        &self,
        request: Request<GetSuccessorListRequest>,
    ) -> Result<Response<GetSuccessorListResponse>, Status> {
        let successor_list = self.config.successor_list.lock().await;
        let mut successors = Vec::new();

        // Convert NodeIds to NodeInfo
        for node_id in successor_list.iter() {
            if let Some(addr) = self.config.get_node_addr(node_id).await {
                successors.push(NodeInfo {
                    node_id: node_id.to_bytes().to_vec(),
                    address: addr,
                });
            }
        }

        Ok(Response::new(GetSuccessorListResponse {
            successors,
            success: true,
            error: String::new(),
        }))
    }

    async fn fix_finger(
        &self,
        request: Request<FixFingerRequest>,
    ) -> Result<Response<FixFingerResponse>, Status> {
        let req = request.into_inner();
        let index = req.index as usize;

        // Calculate the finger ID for this index
        let finger_id = self.config.local_node_id.get_finger_id(index);

        // Find the successor for this finger ID
        match self
            .node
            .closest_preceding_node(&finger_id.to_bytes())
            .await
        {
            Some(successor_id) => {
                // Update finger table
                let mut finger_table = self.config.finger_table.lock().await;
                finger_table.update_finger(index, successor_id);

                // Get the address for the response
                let addr = self
                    .config
                    .get_node_addr(&successor_id)
                    .await
                    .unwrap_or_default();

                Ok(Response::new(FixFingerResponse {
                    finger_node: Some(NodeInfo {
                        node_id: successor_id.to_bytes().to_vec(),
                        address: addr,
                    }),
                    success: true,
                    error: String::new(),
                }))
            }
            None => Ok(Response::new(FixFingerResponse {
                finger_node: None,
                success: false,
                error: "No suitable node found for finger".to_string(),
            })),
        }
    }
}
