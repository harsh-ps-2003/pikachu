use tonic::{Request, Response, Status, Streaming};
use std::sync::Arc;
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use crate::chord::types::{ChordNode, ThreadConfig, Key, Value, NodeId};
use crate::network::messages::chord::{
    chord_node_server::{ChordNode as ChordNodeService, ChordNodeServer},
    LookupRequest, LookupResponse,
    PutRequest, PutResponse,
    GetRequest, GetResponse,
    JoinRequest, JoinResponse,
    NotifyRequest, NotifyResponse,
    StabilizeRequest, StabilizeResponse,
    FindSuccessorRequest, FindSuccessorResponse,
    GetPredecessorRequest, GetPredecessorResponse,
    HeartbeatRequest, HeartbeatResponse,
    ReplicateRequest, ReplicateResponse,
    TransferKeysRequest, TransferKeysResponse,
    NodeInfo, KeyValue,
    HandoffRequest, HandoffResponse,
};
use chrono::Utc;
use tokio::sync::mpsc;
use log::{debug, error};

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
            Ok(node) => Ok(Response::new(LookupResponse {
                responsible_node: Some(NodeInfo {
                    node_id: node.to_bytes().to_vec(),
                    address: self.node.get_multiaddr(&node)
                        .ok_or_else(|| Status::internal("Node address not found"))?
                        .to_string(),
                }),
                success: true,
                error: String::new(),
            })),
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
                    error: None,
                }))
            }
            Ok(None) => {
                debug!("Key not found");
                Ok(Response::new(GetResponse {
                    success: false,
                    value: Vec::new(),
                    error: Some("Key not found".into()),
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
        if !self.node.owns_key(&key) {
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
        let mut storage = self.node.storage.lock()
            .map_err(|_| Status::internal("Failed to acquire storage lock"))?;
        
        storage.insert(Key(key), Value(value));
        
        Ok(Response::new(PutResponse {
            success: true,
            error: None,
        }))
    }

    async fn join(
        &self,
        request: Request<JoinRequest>,
    ) -> Result<Response<JoinResponse>, Status> {
        let req = request.into_inner();
        let node_id = NodeId::from_bytes(&req.node_id)
            .map_err(|_| Status::invalid_argument("Invalid node ID"))?;
        
        let mut successor_list = self.config.successor_list.lock()
            .map_err(|_| Status::internal("Failed to acquire successor list lock"))?;
        let mut predecessor = self.config.predecessor.lock()
            .map_err(|_| Status::internal("Failed to acquire predecessor lock"))?;
        
        // Add the joining node to our successor list if it should be
        if successor_list.is_empty() || self.node.is_between(&node_id, &self.node.node_id, &successor_list[0]) {
            successor_list.insert(0, node_id);
            *predecessor = Some(node_id);
        }
        
        Ok(Response::new(JoinResponse {
            success: true,
            successor: Some(NodeInfo {
                node_id: successor_list[0].to_bytes().to_vec(),
                address: self.node.get_multiaddr(&successor_list[0])
                    .ok_or_else(|| Status::internal("Successor address not found"))?
                    .to_string(),
            }),
            predecessor: predecessor.as_ref().map(|p| NodeInfo {
                node_id: p.to_bytes().to_vec(),
                address: self.node.get_multiaddr(p)
                    .ok_or_else(|| Status::internal("Predecessor address not found"))?
                    .to_string(),
            }),
            transferred_data: Vec::new(),
            error: String::new(),
        }))
    }

    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> Result<Response<NotifyResponse>, Status> {
        let req = request.into_inner();
        let node_id = NodeId::from_bytes(&req.node_id)
            .map_err(|_| Status::invalid_argument("Invalid node ID"))?;
        
        let mut predecessor = self.config.predecessor.lock()
            .map_err(|_| Status::internal("Failed to acquire predecessor lock"))?;
        
        // Update predecessor if appropriate
        if predecessor.is_none() || 
           self.node.is_between(&node_id, &predecessor.unwrap(), &self.node.node_id) {
            *predecessor = Some(node_id);
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
        let predecessor = self.config.predecessor.lock()
            .map_err(|_| Status::internal("Failed to acquire predecessor lock"))?;
        
        Ok(Response::new(StabilizeResponse {
            predecessor: predecessor.as_ref().map(|p| NodeInfo {
                node_id: p.to_bytes().to_vec(),
                address: self.node.get_multiaddr(p)
                    .ok_or_else(|| Status::internal("Predecessor address not found"))?
                    .to_string(),
            }),
            success: true,
            error: String::new(),
        }))
    }

    async fn find_successor(
        &self,
        request: Request<FindSuccessorRequest>,
    ) -> Result<Response<FindSuccessorResponse>, Status> {
        let req = request.into_inner();
        let id = NodeId::from_bytes(&req.id)
            .map_err(|_| Status::invalid_argument("Invalid node ID"))?;
        
        match self.node.find_successor(id).await {
            Ok(successor) => Ok(Response::new(FindSuccessorResponse {
                successor: Some(NodeInfo {
                    node_id: successor.to_bytes().to_vec(),
                    address: self.node.get_multiaddr(&successor)
                        .ok_or_else(|| Status::internal("Successor address not found"))?
                        .to_string(),
                }),
                success: true,
                error: String::new(),
            })),
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
        let predecessor = self.config.predecessor.lock()
            .map_err(|_| Status::internal("Failed to acquire predecessor lock"))?;
        
        Ok(Response::new(GetPredecessorResponse {
            predecessor: predecessor.as_ref().map(|p| NodeInfo {
                node_id: p.to_bytes().to_vec(),
                address: self.node.get_multiaddr(p)
                    .ok_or_else(|| Status::internal("Predecessor address not found"))?
                    .to_string(),
            }),
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
            timestamp: Utc::now().to_rfc3339(),
        }))
    }

    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let req = request.into_inner();
        let mut storage = self.config.storage.lock()
            .map_err(|_| Status::internal("Failed to acquire storage lock"))?;
        
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
        let target_id = NodeId::from_bytes(&req.target_id)
            .map_err(|_| Status::invalid_argument("Invalid target ID"))?;

        // Lock storage to handle key transfers
        let mut storage = self.config.storage.lock()
            .map_err(|_| Status::internal("Failed to acquire storage lock"))?;
        
        let mut transferred_data = Vec::new();

        // Handle incoming keys if any (for join/leave operations)
        if !req.keys.is_empty() {
            for kv in req.keys {
                storage.insert(Key(kv.key), Value(kv.value));
                transferred_data.push(kv);
            }
            debug!("Stored {} transferred keys", transferred_data.len());
        }

        // If this is a join operation, find keys that should be transferred to the new node
        if req.is_join {
            let mut keys_to_transfer = Vec::new();
            for (key, value) in storage.iter() {
                let key_id = NodeId::from_key(&key.0);
                if self.node.owns_key(&key.0) && key_id.is_between(&self.node.node_id, &target_id) {
                    keys_to_transfer.push(KeyValue {
                        key: key.0.clone(),
                        value: value.0.clone(),
                    });
                    storage.remove(key);
                }
            }
            transferred_data.extend(keys_to_transfer);
            debug!("Transferring {} keys to joining node", transferred_data.len());
        }

        // If this is a leave operation, accept all keys from the leaving node
        if req.is_leave {
            debug!("Accepting {} keys from leaving node", req.keys.len());
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
                    let mut storage = storage.lock()
                        .map_err(|_| Status::internal("Failed to acquire storage lock"))?;
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
            let storage = storage.lock()
                .map_err(|_| Status::internal("Failed to acquire storage lock"))?;
            
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
        Ok(Response::new(Box::pin(output_stream) as Self::RequestHandoffStream))
    }
}