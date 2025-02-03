pub mod client;
pub mod server;

pub struct PeerConfig {
    pub grpc_port: Option<u16>,  // Optional gRPC port (random if not specified)
    // ... other config options ...
}