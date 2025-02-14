use crate::chord::types::{ChordNode, ThreadConfig};
use crate::error::NetworkError;
use crate::network::grpc::client::ChordGrpcClient;
use crate::network::grpc::server::ChordGrpcServer;
use crate::network::messages::chord::chord_node_server::ChordNodeServer;
use futures::FutureExt;
use log::{error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::transport::Server;

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

    pub async fn run(mut self) -> Result<(), NetworkError> {
        let addr = self
            .config
            .local_addr
            .parse()
            .map_err(|e| NetworkError::Grpc(format!("Failed to parse address: {}", e)))?;

        info!("Starting gRPC server on {}", addr);

        let server = ChordGrpcServer::new(self.node.clone(), self.config.clone());

        let shutdown_rx = self
            .shutdown_rx
            .take()
            .expect("shutdown_rx should be available");

        let server = Server::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .add_service(ChordNodeServer::new(server));

        if let Some(ready_tx) = self.ready_tx.take() {
            let _ = ready_tx.send(());
        }

        match server
            .serve_with_shutdown(addr, shutdown_rx.map(|_| ()))
            .await
        {
            Ok(_) => {
                info!("gRPC server shut down gracefully");
                Ok(())
            }
            Err(e) => {
                error!("gRPC server encountered a fatal error: {:?}", e);
                Err(NetworkError::Grpc(format!("Server error: {}", e)))
            }
        }
    }
}
