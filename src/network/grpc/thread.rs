use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::transport::Server;
use crate::chord::types::{ChordNode, ThreadConfig};
use crate::network::messages::chord::chord_node_server::ChordNodeServer;
use crate::network::grpc::server::ChordGrpcServer;
use crate::error::NetworkError;
use log::{info, error, warn};

pub struct GrpcThread {
    node: Arc<ChordNode>,
    config: ThreadConfig,
    shutdown_rx: oneshot::Receiver<()>,
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
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<(), NetworkError> {
        info!("Starting gRPC server at {}", self.config.local_addr);

        // Create the gRPC server with shared state
        let grpc_server = ChordGrpcServer::new(
            self.node.clone(),
            self.config.clone(),
        );

        // Get the address to bind to
        let addr = self.config.local_addr.parse()
            .map_err(|e| NetworkError::InvalidAddress(format!("Invalid address: {}", e)))?;

        // Create a channel for shutdown coordination
        let (shutdown_complete_tx, mut shutdown_complete_rx) = oneshot::channel();
        let server_grpc = grpc_server.clone();

        // Create the server
        let server = Server::builder()
            .add_service(ChordNodeServer::new(grpc_server))
            .serve_with_shutdown(addr, async {
                let _ = self.shutdown_rx.await;
                info!("Received shutdown signal, initiating graceful shutdown");
                
                // Get successor for data handoff
                let successor_list = self.config.successor_list.lock()
                    .map_err(|_| error!("Failed to acquire successor list lock during shutdown"))?;
                
                if let Some(successor) = successor_list.first() {
                    if let Some(successor_addr) = self.node.get_multiaddr(successor) {
                        info!("Transferring data to successor at {}", successor_addr);
                        
                        // Convert multiaddr to gRPC address
                        if let Ok(grpc_addr) = self.node.to_grpc_addr(&successor_addr) {
                            match server_grpc.transfer_data_to_successor(grpc_addr).await {
                                Ok(_) => info!("Successfully transferred data to successor"),
                                Err(e) => error!("Failed to transfer data to successor: {}", e),
                            }
                        } else {
                            error!("Failed to convert successor address to gRPC format");
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