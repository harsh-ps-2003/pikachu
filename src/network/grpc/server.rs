use tonic::{Request, Response, Status};
use crate::chord::actor::ChordHandle;
use crate::chord::types::{Key, Value, NodeId};
use crate::network::messages::chord::{
    ChordNode, ChordNodeServer,
    LookupRequest, LookupResponse,
    PutRequest, PutResponse,
    GetRequest, GetResponse,
    JoinRequest, JoinResponse,
    NotifyRequest, NotifyResponse,
    StabilizeRequest, StabilizeResponse,
    FindSuccessorRequest, FindSuccessorResponse,
    GetPredecessorRequest, GetPredecessorResponse,
    HeartbeatRequest, HeartbeatResponse,
    NodeInfo,
};
use crate::network::messages::message::{ReplicateRequest, ReplicateResponse};

#[derive(Debug)]
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

    async fn join(
        &self,
        request: Request<JoinRequest>,
    ) -> Result<Response<JoinResponse>, Status> {
        let req = request.into_inner();
        let node_id = NodeId::from_bytes(&req.node_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        
        match self.chord_handle.join(node_id).await {
            Ok(_) => Ok(Response::new(JoinResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(JoinResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> Result<Response<NotifyResponse>, Status> {
        let req = request.into_inner();
        let node_id = NodeId::from_bytes(&req.node_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        
        match self.chord_handle.notify(node_id).await {
            Ok(_) => Ok(Response::new(NotifyResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(NotifyResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn stabilize(
        &self,
        request: Request<StabilizeRequest>,
    ) -> Result<Response<StabilizeResponse>, Status> {
        match self.chord_handle.stabilize().await {
            Ok(_) => Ok(Response::new(StabilizeResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(StabilizeResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn find_successor(
        &self,
        request: Request<FindSuccessorRequest>,
    ) -> Result<Response<FindSuccessorResponse>, Status> {
        let req = request.into_inner();
        let node_id = NodeId::from_bytes(&req.id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        
        match self.chord_handle.find_successor(node_id).await {
            Ok(successor) => Ok(Response::new(FindSuccessorResponse {
                successor: Some(successor.into()),
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
        request: Request<GetPredecessorRequest>,
    ) -> Result<Response<GetPredecessorResponse>, Status> {
        match self.chord_handle.get_predecessor().await {
            Ok(Some(pred)) => Ok(Response::new(GetPredecessorResponse {
                predecessor: Some(pred.into()),
                success: true,
                error: String::new(),
            })),
            Ok(None) => Ok(Response::new(GetPredecessorResponse {
                predecessor: None,
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(GetPredecessorResponse {
                predecessor: None,
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        // Simple heartbeat response - if we can respond, we're alive
        Ok(Response::new(HeartbeatResponse {
            success: true,
            error: String::new(),
        }))
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