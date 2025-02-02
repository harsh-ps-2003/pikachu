use tonic::transport::Channel;
use crate::proto::chord::chord_node_client::ChordNodeClient;
use crate::error::NetworkError;

pub struct ChordGrpcClient {
    client: ChordNodeClient<Channel>,
}

impl ChordGrpcClient {
    pub async fn new(addr: String) -> Result<Self, NetworkError> {
        let client = ChordNodeClient::connect(addr).await
            .map_err(|e| NetworkError::Connection(e.to_string()))?;
        
        Ok(Self { client })
    }

    pub async fn lookup(&mut self, key: Vec<u8>) -> Result<NodeInfo, NetworkError> {
        let response = self.client
            .lookup(LookupRequest {
                key,
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::Rpc(e.to_string()))?;

        Ok(response.into_inner().responsible_node.unwrap())
    }

    // Implement other RPC methods...
}