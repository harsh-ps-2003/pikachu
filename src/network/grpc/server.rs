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
use log::{debug, error};
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

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let key = req.key;

        debug!("Received get request for key: {:?}", key);

        // Use the lookup_key method to find the value
        match self.node.lookup_key(&key).await {
            Ok(Some(value)) => {
                debug!("Found value for key");
                Ok(Response::new(GetResponse {
                    success: true,
                    value: value.0,
                    error: String::new(),
                }))
            }
            Ok(None) => {
                debug!("Key not found");
                Ok(Response::new(GetResponse {
                    success: false,
                    value: Vec::new(),
                    error: "Key not found".to_string(),
                }))
            }
            Err(e) => {
                error!("Lookup failed: {}", e);
                Err(Status::internal(format!("Lookup failed: {}", e)))
            }
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let key = req.key;
        let value = req.value;

        debug!("Received put request for key: {:?}", key);

        // Check if we own the key
        if !self.node.owns_key(&key).await {
            // Forward to the appropriate node
            match self.node.lookup_key(&key).await {
                Ok(_) => {
                    // The key exists somewhere, but we don't own it
                    return Err(Status::failed_precondition("Key belongs to another node"));
                }
                Err(e) => {
                    error!("Failed to check key ownership: {}", e);
                    return Err(Status::internal("Failed to check key ownership"));
                }
            }
        }

        // Store the key-value pair
        let mut storage = self.node.storage.lock().await;
        storage.insert(Key(key), Value(value));

        Ok(Response::new(PutResponse {
            success: true,
            error: String::new(),
        }))
    }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let req = request.into_inner();
        let joining_node = req
            .joining_node
            .ok_or_else(|| Status::invalid_argument("Missing joining node info"))?;
        let node_id = NodeId::from_bytes(&joining_node.node_id);

        // Acquire all locks in a consistent order to prevent deadlocks
        let mut successor_list = self.config.successor_list.lock().await;
        let mut predecessor = self.config.predecessor.lock().await;
        let mut addresses = self.config.node_addresses.lock().await;

        // Add the joining node to our successor list if it should be
        if successor_list.is_empty() || node_id.is_between(&self.node.node_id, &successor_list[0]) {
            // Update all state atomically
            successor_list.insert(0, node_id);
            *predecessor = Some(node_id);
            addresses.insert(node_id, joining_node.address.clone());

            debug!("Node {} joined the network", node_id);
        }

        let successor_addr = self
            .config
            .get_node_addr(&successor_list[0])
            .await
            .ok_or_else(|| Status::internal("Successor address not found"))?;

        // Get predecessor info if it exists
        let pred_info = if let Some(p) = predecessor.as_ref() {
            let addr = addresses.get(p).cloned().unwrap_or_default();
            Some(NodeInfo {
                node_id: p.to_bytes().to_vec(),
                address: addr,
            })
        } else {
            None
        };

        Ok(Response::new(JoinResponse {
            success: true,
            successor: Some(NodeInfo {
                node_id: successor_list[0].to_bytes().to_vec(),
                address: successor_addr,
            }),
            predecessor: pred_info,
            transferred_data: Vec::new(),
            error: String::new(),
        }))
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

        let mut pred_lock = self.config.predecessor.lock().await;

        // Update predecessor if appropriate
        if pred_lock.is_none() || node_id.is_between(&pred_lock.unwrap(), &self.node.node_id) {
            *pred_lock = Some(node_id);
        }

        Ok(Response::new(NotifyResponse {
            accepted: true,
            error: String::new(),
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
