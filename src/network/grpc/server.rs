use tonic::{Request, Response, Status};
use crate::network::messages::chord::{
    self,
    LookupRequest, LookupResponse,
    PutRequest, PutResponse,
    GetRequest, GetResponse,
    ReplicateRequest, ReplicateResponse,
    KeyValue,
};
use crate::chord::types::{Key, Value};
use crate::proto::chord::chord_node_server::{ChordNode, ChordNodeServer};
use crate::chord::actor::ChordHandle;

pub struct ChordGrpcServer {
    chord_handle: ChordHandle,
}

impl ChordGrpcServer {
    pub fn new(chord_handle: ChordHandle) -> Self {
        Self { chord_handle }
    }
}

#[tonic::async_trait]
impl ChordNode for ChordGrpcServer {
    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let req = request.into_inner();
        match self.chord_handle.lookup(Key(req.key)).await {
            Ok(node) => Ok(Response::new(LookupResponse {
                responsible_node: Some(node.into()),
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

    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        match self.chord_handle.put(Key(req.key), Value(req.value)).await {
            Ok(_) => Ok(Response::new(PutResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(PutResponse {
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
        match self.chord_handle.get(&Key(req.key)).await {
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

    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let req = request.into_inner();
        
        // Store all received key-value pairs
        for kv in req.data {
            self.chord_handle.put(
                Key(kv.key),
                Value(kv.value)
            ).await.map_err(|e| Status::internal(e.to_string()))?;
        }
        
        Ok(Response::new(ReplicateResponse {
            success: true,
            error: String::new(),
        }))
    }
}