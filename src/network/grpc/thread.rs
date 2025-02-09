use crate::chord::types::{ChordNode, ThreadConfig};
use crate::error::NetworkError;
use crate::network::grpc::client::ChordGrpcClient;
use crate::network::grpc::server::ChordGrpcServer;
use crate::network::messages::chord::chord_node_server::ChordNodeServer;
use log::{error, info, warn};
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::transport::Server;

pub struct GrpcThread {
    node: Arc<ChordNode>,
    config: ThreadConfig,
    shutdown_rx: Option<oneshot::Receiver<()>>,
}

impl GrpcThread {
    pub fn new(
        node: Arc<ChordNode>,
        config: ThreadConfig,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            node,
            config,
            shutdown_rx: Some(shutdown_rx),
        }
    }

    pub async fn run(&mut self) -> Result<(), NetworkError> {
        info!("Starting gRPC server at {}", self.config.local_addr);

        // Create the gRPC server with shared state
        let grpc_server = ChordGrpcServer::new(self.node.clone(), self.config.clone());

        // Get the address to bind to
        let addr = self
            .config
            .local_addr
            .parse()
            .map_err(|e| NetworkError::InvalidAddress(format!("Invalid address: {}", e)))?;

        // Create a channel for shutdown coordination
        let (shutdown_complete_tx, mut shutdown_complete_rx) = oneshot::channel();
        let server_grpc = grpc_server.clone();

        // Take ownership of shutdown_rx
        let shutdown_rx = self.shutdown_rx.take().expect("Shutdown receiver missing");
        let config = self.config.clone();

        // Create the server
        let server = Server::builder()
            .add_service(ChordNodeServer::new(grpc_server))
            .serve_with_shutdown(addr, async move {
                let _ = shutdown_rx.await;
                info!("Received shutdown signal, initiating graceful shutdown");

                // Get successor for data handoff
                let successor_list = config.successor_list.lock().await;

                if let Some(successor) = successor_list.first() {
                    // Get successor's gRPC address directly
                    if let Some(successor_addr) = config.get_node_addr(successor).await {
                        info!("Transferring data to successor at {}", successor_addr);

                        // Create gRPC client and transfer data
                        match ChordGrpcClient::new(successor_addr).await {
                            Ok(mut client) => match client.handoff_data().await {
                                Ok(_) => info!("Successfully transferred data to successor"),
                                Err(e) => error!("Failed to transfer data to successor: {}", e),
                            },
                            Err(e) => error!("Failed to create gRPC client for successor: {}", e),
                        }
                    } else {
                        error!("No address found for successor node");
                    }
                } else {
                    warn!("No successor found during shutdown, data may be lost");
                }

                let _ = shutdown_complete_tx.send(());
            });

        // Start serving
        match server.await {
            Ok(_) => {
                // Wait for shutdown to complete
                let _ = shutdown_complete_rx.await;
                info!("gRPC server stopped gracefully");
                Ok(())
            }
            Err(e) => {
                error!("gRPC server error: {}", e);
                Err(NetworkError::Grpc(e.to_string()))
            }
        }
    }
}
