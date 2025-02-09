use tonic::transport::Channel;
use futures::Stream;
use std::pin::Pin;
use crate::network::messages::chord::{
    chord_node_client::ChordNodeClient,
    JoinRequest, JoinResponse,
    LookupRequest, LookupResponse,
    PutRequest, PutResponse,
    GetRequest, GetResponse,
    NotifyRequest, NotifyResponse,
    StabilizeRequest, StabilizeResponse,
    FindSuccessorRequest, FindSuccessorResponse,
    GetPredecessorRequest, GetPredecessorResponse,
    HeartbeatRequest, HeartbeatResponse,
    ReplicateRequest, ReplicateResponse,
    TransferKeysRequest, TransferKeysResponse,
    HandoffRequest, HandoffResponse,
    NodeInfo, KeyValue,
    GetSuccessorListRequest, GetSuccessorListResponse,
    FixFingerRequest, FixFingerResponse,
};
use crate::error::NetworkError;
use crate::chord::types::{Key, Value};
use tokio::sync::mpsc;
use futures::StreamExt;
use tonic::Request;

pub struct ChordGrpcClient {
    client: ChordNodeClient<Channel>,
    handoff_tx: Option<mpsc::Sender<KeyValue>>,
}

impl ChordGrpcClient {
    pub async fn new(addr: String) -> Result<Self, NetworkError> {
        let client = ChordNodeClient::connect(addr).await
            .map_err(|e| NetworkError::Grpc(format!("Failed to connect: {}", e)))?;
        
        Ok(Self { 
            client,
            handoff_tx: None,
        })
    }

    /// Set the channel for receiving handoff data
    pub fn set_handoff_channel(&mut self, tx: mpsc::Sender<KeyValue>) {
        self.handoff_tx = Some(tx);
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

    pub async fn put(&mut self, request: PutRequest) -> Result<(), NetworkError> {
        let response = self.client
            .put(request)
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(NetworkError::PeerUnreachable(response.error))
        }
    }

    pub async fn get(&mut self, request: GetRequest) -> Result<Option<Value>, NetworkError> {
        let response = self.client
            .get(request)
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?
            .into_inner();

        if response.success {
            Ok(Some(Value(response.value)))
        } else if response.error.as_deref() == Some("Key not found") {
            Ok(None)
        } else {
            Err(NetworkError::PeerUnreachable(response.error.unwrap_or_else(|| "Unknown error".to_string())))
        }
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

    pub async fn find_successor(&mut self, id: Vec<u8>) -> Result<NodeInfo, NetworkError> {
        let response = self.client
            .find_successor(FindSuccessorRequest {
                id,
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        let inner = response.into_inner();
        if inner.success {
            inner.successor.ok_or_else(|| NetworkError::PeerUnreachable("No successor found".to_string()))
        } else {
            Err(NetworkError::PeerUnreachable(inner.error))
        }
    }

    pub async fn transfer_keys(&mut self, request: TransferKeysRequest) -> Result<TransferKeysResponse, NetworkError> {
        let response = self.client
            .transfer_keys(request)
            .await
            .map_err(|e| NetworkError::Grpc(format!("Transfer failed: {}", e)))?;

        Ok(response.into_inner())
    }

    pub async fn notify_transfer(&mut self, keys: Vec<KeyValue>, target_node: NodeInfo) -> Result<(), NetworkError> {
        let response = self.client
            .replicate(ReplicateRequest {
                data: keys,
                source_node: Some(target_node),
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        let inner = response.into_inner();
        if inner.success {
            Ok(())
        } else {
            Err(NetworkError::PeerUnreachable(inner.error))
        }
    }

    /// Stream key-value pairs to target node using efficient handoff
    pub async fn handoff(
        &mut self,
        mut rx: mpsc::Receiver<KeyValue>,
    ) -> Result<HandoffResponse, NetworkError> {
        // Create a stream from our channel receiver that yields KeyValue directly
        let output_stream = async_stream::stream! {
            while let Some(kv) = rx.recv().await {
                yield kv;
            }
        };

        let response = self.client
            .handoff(Request::new(Box::pin(output_stream) as Pin<Box<dyn Stream<Item = KeyValue> + Send>>))
            .await
            .map_err(|e| NetworkError::Grpc(format!("Handoff failed: {}", e)))?;

        Ok(response.into_inner())
    }

    /// Request data handoff from a node and stream the results to a channel
    pub async fn request_handoff(&mut self) -> Result<(), NetworkError> {
        let request = HandoffRequest {
            source_node: None,
            is_shutdown: false,
        };

        let mut stream = self.client
            .request_handoff(request)
            .await
            .map_err(|e| NetworkError::Grpc(format!("Failed to start handoff: {}", e)))?
            .into_inner();

        // Process the stream of key-value pairs
        while let Some(kv) = stream.message().await
            .map_err(|e| NetworkError::Grpc(format!("Error receiving handoff data: {}", e)))? {
            // Send to the channel if available
            if let Some(tx) = &self.handoff_tx {
                tx.send(kv).await
                    .map_err(|_| NetworkError::Grpc("Failed to send key-value pair to channel".into()))?;
            }
        }

        Ok(())
    }

    pub async fn get_predecessor(&mut self) -> Result<GetPredecessorResponse, NetworkError> {
        let response = self.client
            .get_predecessor(GetPredecessorRequest {
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;
        
        Ok(response.into_inner())
    }

    pub async fn notify(&mut self, node_id: NodeId) -> Result<(), NetworkError> {
        let response = self.client
            .notify(NotifyRequest {
                predecessor: Some(NodeInfo {
                    node_id: node_id.to_bytes().to_vec(),
                    address: self.local_addr.clone(),
                }),
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        let inner = response.into_inner();
        if inner.accepted {
            Ok(())
        } else {
            Err(NetworkError::PeerUnreachable(inner.error))
        }
    }

    pub async fn heartbeat(&mut self) -> Result<HeartbeatResponse, NetworkError> {
        let response = self.client
            .heartbeat(HeartbeatRequest {
                sender: None,
                timestamp: chrono::Utc::now().timestamp() as u64,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        Ok(response.into_inner())
    }

    pub async fn stabilize(&mut self) -> Result<(), NetworkError> {
        let response = self.client
            .stabilize(StabilizeRequest {
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        let inner = response.into_inner();
        if inner.success {
            Ok(())
        } else {
            Err(NetworkError::PeerUnreachable(inner.error))
        }
    }

    pub async fn get_successor_list(&mut self) -> Result<Vec<NodeInfo>, NetworkError> {
        let response = self.client
            .get_successor_list(GetSuccessorListRequest {
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        let inner = response.into_inner();
        if inner.success {
            Ok(inner.successors)
        } else {
            Err(NetworkError::PeerUnreachable(inner.error))
        }
    }

    pub async fn fix_finger(&mut self, index: usize) -> Result<(), NetworkError> {
        let response = self.client
            .fix_finger(FixFingerRequest {
                index: index as u32,
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        let inner = response.into_inner();
        if inner.success {
            Ok(())
        } else {
            Err(NetworkError::PeerUnreachable(inner.error))
        }
    }

    // Implement other RPC methods...
}