use tonic::transport::Channel;
use crate::network::messages::{
    chord::{LookupRequest, LookupResponse, NodeInfo}, 
    message::{ReplicateRequest, ReplicateResponse}
};
use crate::chord::client::ChordNodeClient;
use crate::error::NetworkError;

pub struct ChordGrpcClient {
    client: ChordNodeClient<Channel>,
}

impl ChordGrpcClient {
    pub async fn new(addr: String) -> Result<Self, NetworkError> {
        let client = ChordNodeClient::connect(addr).await
            .map_err(|e| NetworkError::ConnectionFailed(e.to_string()))?;
        
        Ok(Self { client })
    }

    pub async fn lookup(&mut self, key: Vec<u8>) -> Result<NodeInfo, NetworkError> {
        let response = self.client
            .lookup(LookupRequest {
                key,
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        Ok(response.into_inner().responsible_node.unwrap())
    }

    pub async fn replicate(&mut self, request: ReplicateRequest) -> Result<(), NetworkError> {
        let response = self.client
            .replicate(request)
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(NetworkError::PeerUnreachable(response.error))
        }
    }

    // Implement other RPC methods...
}