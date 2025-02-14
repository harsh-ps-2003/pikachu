pub mod client;
pub mod server;
pub mod thread;

pub use client::ChordGrpcClient;
pub use server::ChordGrpcServer;
pub use thread::GrpcThread;

/// Configuration for a Chord peer node
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Optional gRPC port (random if not specified)
    pub grpc_port: Option<u16>,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self { grpc_port: None }
    }
}
