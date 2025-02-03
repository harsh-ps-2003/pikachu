use tonic::{Request, Response, Status};
use crate::network::messages::message::{LookupRequest, ReplicateRequest, ReplicateResponse};
use crate::chord::types::{Key, Value};
use crate::proto::chord::{self, chord_node_server::ChordNodeServer};

pub struct ChordGrpcServer {
    chord_node: ChordNode,
}

#[tonic::async_trait]
impl server::ChordNode for ChordGrpcServer {
    async fn lookup(
        &self,
        request: Request<chord::LookupRequest>,
    ) -> Result<Response<chord::LookupResponse>, Status> {
        let req = request.into_inner();
        match self.chord_node.lookup(req.key).await {
            Ok(node) => Ok(Response::new(chord::LookupResponse {
                responsible_node: Some(node.into()),
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(chord::LookupResponse {
                responsible_node: None,
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
            self.chord_node.put(
                Key(kv.key),
                Value(kv.value)
            ).await.map_err(|e| Status::internal(e.to_string()))?;
        }
        
        Ok(Response::new(ReplicateResponse {
            success: true,
            error: String::new(),
        }))
    }

    // Implement other RPCs...
}