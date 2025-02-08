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
    HandoffRequest, HandoffResponse,
    NodeInfo, KeyValue,
};
use chrono::Utc;
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;
use log::{debug, error};

#[derive(Debug)]
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

    async fn handoff(
        &self,
        request: Request<Streaming<HandoffRequest>>,
    ) -> Result<Response<HandoffResponse>, Status> {
        let mut stream = request.into_inner();
        let mut storage = self.config.storage.lock()
            .map_err(|_| Status::internal("Failed to acquire storage lock"))?;
        let mut count = 0;

        // Process the stream of key-value pairs
        while let Some(req) = stream.next().await {
            let req = req.map_err(|e| Status::internal(format!("Failed to receive data: {}", e)))?;
            
            // Store each key-value pair
            storage.insert(Key(req.key), Value(req.value));
            count += 1;
        }

        Ok(Response::new(HandoffResponse {
            success: true,
            transferred_count: count,
            error: String::new(),
        }))
    }

    // Helper method to stream data to successor during shutdown
    pub async fn transfer_data_to_successor(&self, successor_addr: String) -> Result<(), Status> {
        // Create gRPC client for successor
        let mut client = ChordGrpcClient::new(successor_addr)
            .await
            .map_err(|e| Status::internal(format!("Failed to connect to successor: {}", e)))?;

        // Create channel for streaming
        let (tx, rx) = mpsc::channel(32);
        let storage = self.config.storage.lock()
            .map_err(|_| Status::internal("Failed to acquire storage lock"))?;

        // Spawn task to send data
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            for (key, value) in storage_clone.iter() {
                let _ = tx.send(HandoffRequest {
                    key: key.0.clone(),
                    value: value.0.clone(),
                }).await;
            }
        });

        // Create stream from receiver
        let stream = ReceiverStream::new(rx);

        // Send stream to successor
        client.handoff(stream)
            .await
            .map_err(|e| Status::internal(format!("Failed to transfer data: {}", e)))?;

        Ok(())
    }
}