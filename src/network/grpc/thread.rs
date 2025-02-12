use crate::chord::types::{ChordNode, ThreadConfig};
use crate::error::NetworkError;
use crate::network::grpc::client::ChordGrpcClient;
use crate::network::grpc::server::ChordGrpcServer;
use crate::network::messages::chord::chord_node_server::ChordNodeServer;
use log::{error, info};
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::transport::Server;
use futures::FutureExt;

pub struct GrpcThread {
    node: Arc<ChordNode>,
    config: ThreadConfig,
    shutdown_rx: Option<oneshot::Receiver<()>>,
    ready_tx: Option<oneshot::Sender<()>>,
}

impl GrpcThread {
    pub fn new(
        node: Arc<ChordNode>,
        config: ThreadConfig,
        shutdown_rx: oneshot::Receiver<()>,
        ready_tx: oneshot::Sender<()>,
    ) -> Self {
        Self {
            node,
            config,
            shutdown_rx: Some(shutdown_rx),
            ready_tx: Some(ready_tx),
        }
    }

    pub async fn run(&mut self) -> Result<(), NetworkError> {
        let addr = self.config.local_addr
            .parse()
            .map_err(|e| NetworkError::Grpc(format!("Invalid address: {}", e)))?;

        info!("Starting gRPC server on {}", addr);

        let server = ChordGrpcServer::new(self.node.clone(), self.config.clone());
        
        // Take ownership of shutdown_rx
        let shutdown_rx = self.shutdown_rx.take()
            .ok_or_else(|| NetworkError::Grpc("Shutdown receiver already taken".into()))?;

        // Build and bind the server
        let server = Server::builder()
            .add_service(ChordNodeServer::new(server))
            .serve(addr);

        // Signal that we're ready to accept connections
        if let Some(ready_tx) = self.ready_tx.take() {
            let _ = ready_tx.send(());
        }

        // Run the server and handle shutdown
        tokio::select! {
            result = server => {
                result.map_err(|e| NetworkError::Grpc(format!("Server error: {}", e)))?;
                Ok(())
            }
            _ = shutdown_rx => {
                info!("Received shutdown signal");
                Ok(())
            }
        }
    }
}
