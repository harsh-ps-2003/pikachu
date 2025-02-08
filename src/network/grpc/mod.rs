pub mod client;
pub mod server;
pub mod thread;

pub use client::ChordGrpcClient;
pub use server::ChordGrpcServer;
pub use thread::GrpcThread;

pub struct PeerConfig {
    pub grpc_port: Option<u16>,  // Optional gRPC port (random if not specified)
    // ... other config options ...
}