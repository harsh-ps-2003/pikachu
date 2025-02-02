use tonic::{Request, Response, Status};
use crate::chord::ChordNode;
use crate::proto::chord::{self, chord_node_server::ChordNodeServer};

pub struct ChordGrpcServer {
    chord_node: ChordNode,
}

#[tonic::async_trait]
impl chord_node_server::ChordNode for ChordGrpcServer {
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

    // Implement other RPCs...
}