use crate::chord::types::{Key, NodeId, Value};
use crate::error::NetworkError;
use crate::network::messages::chord::{
    chord_node_client::ChordNodeClient, FindSuccessorRequest, FindSuccessorResponse,
    FixFingerRequest, FixFingerResponse, GetPredecessorRequest, GetPredecessorResponse, GetRequest,
    GetResponse, GetSuccessorListRequest, GetSuccessorListResponse, HandoffRequest,
    HandoffResponse, HeartbeatRequest, HeartbeatResponse, JoinRequest, JoinResponse, KeyValue,
    LookupRequest, LookupResponse, NodeInfo, NotifyRequest, NotifyResponse, PutRequest,
    PutResponse, ReplicateRequest, ReplicateResponse, StabilizeRequest, StabilizeResponse,
    TransferKeysRequest, TransferKeysResponse,
};
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{self as ts};
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use ts::StreamExt as _;
use std::time::Duration;

pub struct ChordGrpcClient {
    client: ChordNodeClient<Channel>,
    handoff_tx: Option<mpsc::Sender<KeyValue>>,
    local_addr: String,
}

impl ChordGrpcClient {
    pub async fn new(addr: String) -> Result<Self, NetworkError> {
        // Ensure the address has the correct scheme
        let addr = if !addr.starts_with("http://") {
            format!("http://{}", addr)
        } else {
            addr
        };

        // Configure the channel with appropriate settings
        let channel = Endpoint::from_shared(addr.clone())
            .map_err(|e| NetworkError::Grpc(format!("Invalid endpoint: {}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .tcp_nodelay(true)
            .connect()
            .await
            .map_err(|e| NetworkError::Grpc(format!("Failed to connect: {}", e)))?;

        let client = ChordNodeClient::new(channel)
            .max_decoding_message_size(16 * 1024 * 1024) // 16MB
            .max_encoding_message_size(16 * 1024 * 1024); // 16MB

        Ok(Self {
            client,
            handoff_tx: None,
            local_addr: addr,
        })
    }

    /// Set the channel for receiving handoff data
    pub fn set_handoff_channel(&mut self, tx: mpsc::Sender<KeyValue>) {
        self.handoff_tx = Some(tx);
    }

    pub async fn lookup(&mut self, key: Vec<u8>) -> Result<NodeInfo, NetworkError> {
        let response = self
            .client
            .lookup(LookupRequest {
                key,
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        Ok(response.into_inner().responsible_node.unwrap())
    }

    pub async fn put(&mut self, request: PutRequest) -> Result<(), NetworkError> {
        let response = self
            .client
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
        let response = self
            .client
            .get(request)
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?
            .into_inner();

        if response.success {
            Ok(Some(Value(response.value)))
        } else if response.error == "Key not found" {
            Ok(None)
        } else {
            Err(NetworkError::PeerUnreachable(response.error))
        }
    }

    pub async fn replicate(&mut self, request: ReplicateRequest) -> Result<(), NetworkError> {
        let response = self
            .client
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
        let response = self
            .client
            .find_successor(FindSuccessorRequest {
                id,
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        let inner = response.into_inner();
        if inner.success {
            inner
                .successor
                .ok_or_else(|| NetworkError::PeerUnreachable("No successor found".to_string()))
        } else {
            Err(NetworkError::PeerUnreachable(inner.error))
        }
    }

    pub async fn transfer_keys(
        &mut self,
        request: TransferKeysRequest,
    ) -> Result<TransferKeysResponse, NetworkError> {
        let response = self
            .client
            .transfer_keys(request)
            .await
            .map_err(|e| NetworkError::Grpc(format!("Transfer failed: {}", e)))?;

        Ok(response.into_inner())
    }

    pub async fn notify_transfer(
        &mut self,
        keys: Vec<KeyValue>,
        target_node: NodeInfo,
    ) -> Result<(), NetworkError> {
        let response = self
            .client
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
        rx: mpsc::Receiver<KeyValue>,
    ) -> Result<HandoffResponse, NetworkError> {
        // Convert the receiver into a stream
        let stream = ts::wrappers::ReceiverStream::new(rx);

        let response = self
            .client
            .handoff(Request::new(
                Box::pin(stream) as Pin<Box<dyn Stream<Item = KeyValue> + Send>>
            ))
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

        let mut stream = self
            .client
            .request_handoff(request)
            .await
            .map_err(|e| NetworkError::Grpc(format!("Failed to start handoff: {}", e)))?
            .into_inner();

        // Process the stream of key-value pairs
        while let Some(kv) = stream
            .message()
            .await
            .map_err(|e| NetworkError::Grpc(format!("Error receiving handoff data: {}", e)))?
        {
            // Send to the channel if available
            if let Some(tx) = &self.handoff_tx {
                tx.send(kv).await.map_err(|_| {
                    NetworkError::Grpc("Failed to send key-value pair to channel".into())
                })?;
            }
        }

        Ok(())
    }

    pub async fn get_predecessor(&mut self) -> Result<GetPredecessorResponse, NetworkError> {
        let response = self
            .client
            .get_predecessor(GetPredecessorRequest {
                requesting_node: None,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        Ok(response.into_inner())
    }

    pub async fn notify(&mut self, node_id: NodeId) -> Result<(), NetworkError> {
        let response = self
            .client
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
        let response = self
            .client
            .heartbeat(HeartbeatRequest {
                sender: None,
                timestamp: chrono::Utc::now().timestamp() as u64,
            })
            .await
            .map_err(|e| NetworkError::PeerUnreachable(e.to_string()))?;

        Ok(response.into_inner())
    }

    pub async fn stabilize(&mut self) -> Result<(), NetworkError> {
        let response = self
            .client
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
        let request = GetSuccessorListRequest {
            requesting_node: Some(NodeInfo {
                node_id: vec![], // This will be filled by the server
                address: self.local_addr.clone(),
            }),
        };

        let response = self
            .client
            .get_successor_list(request)
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
        let response = self
            .client
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

    /// Transfer all data to this node during shutdown
    pub async fn handoff_data(&mut self) -> Result<(), NetworkError> {
        let request = HandoffRequest {
            source_node: Some(NodeInfo {
                node_id: vec![], // Will be filled by server
                address: self.local_addr.clone(),
            }),
            is_shutdown: true,
        };

        let mut stream = self
            .client
            .request_handoff(request)
            .await
            .map_err(|e| NetworkError::Grpc(format!("Failed to start handoff: {}", e)))?
            .into_inner();

        // Process the stream of key-value pairs
        while let Some(kv) = stream
            .message()
            .await
            .map_err(|e| NetworkError::Grpc(format!("Error receiving handoff data: {}", e)))?
        {
            // Send to the channel if available
            if let Some(tx) = &self.handoff_tx {
                tx.send(kv).await.map_err(|_| {
                    NetworkError::Grpc("Failed to send key-value pair to channel".into())
                })?;
            }
        }

        Ok(())
    }

    // Implement other RPC methods...
}
