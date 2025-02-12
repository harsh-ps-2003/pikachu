use clap::{Parser, Subcommand};
use log::{error, info};
use pikachu::{
    chord::{
        types::{
            NodeId, SharedFingerTable, SharedPredecessor, SharedStorage, SharedSuccessorList,
            ThreadConfig,
        },
        workers::{
            run_finger_maintainer, run_predecessor_checker, run_stabilize_worker,
            run_successor_maintainer,
        },
    },
    error::NetworkError,
    network::{grpc::PeerConfig, node::ChordPeer},
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(name = "pikachu")]
#[command(about = "A Chord DHT implementation in Rust")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a new bootstrap node (first node in the network)
    #[command(name = "start-bootstrap")]
    StartBootstrap {
        /// Optional gRPC port (random if not specified)
        #[arg(short = 'p', long = "port")]
        port: Option<u16>,
    },
    /// Join an existing Chord network through a bootstrap node
    #[command(name = "join")]
    Join {
        /// Local gRPC port for this node (random if not specified)
        #[arg(short = 'p', long = "port")]
        port: Option<u16>,
        /// Port of the bootstrap node to connect to
        #[arg(short = 'b', long = "bootstrap-port")]
        bootstrap_port: u16,
        /// Bootstrap node host (default: 127.0.0.1)
        #[arg(short = 'n', long = "host", default_value = "127.0.0.1")]
        host: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), String> {
    // Initialize logging with timestamp
    env_logger::Builder::from_default_env()
        .format_timestamp_millis()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::StartBootstrap { port } => {
            info!("Initializing bootstrap node...");
            
            // Create peer configuration
            let config = PeerConfig {
                grpc_port: port,
            };

            // Create and initialize peer
            let mut peer = ChordPeer::new(config)
                .await
                .map_err(|e| format!("Failed to create peer: {}", e))?;

            let node_port = peer.get_port();
            info!("Starting bootstrap node on port {}", node_port);

            // Initialize as first node in the network
            if let Err(e) = peer.create_network().await {
                error!("Failed to create network: {}", e);
                return Err(format!("Failed to create network: {}", e));
            }

            info!("Successfully created new network as bootstrap node");
            info!("Bootstrap node is running on port: {}", node_port);
            info!("Other nodes can join using: cargo run join -b {} -p <PORT>", node_port);

            // Run the node until interrupted
            if let Err(e) = peer.run().await {
                error!("Bootstrap node error: {}", e);
                return Err(format!("Bootstrap node error: {}", e));
            }

            info!("Bootstrap node shut down gracefully");
            Ok(())
        }
        Commands::Join { port, bootstrap_port, host } => {
            info!("Initializing node to join network...");
            
            // Create peer configuration
            let config = PeerConfig {
                grpc_port: port,
            };

            // Create and initialize peer
            let mut peer = ChordPeer::new(config)
                .await
                .map_err(|e| format!("Failed to create peer: {}", e))?;

            let node_port = peer.get_port();
            info!("Starting node on port {}", node_port);

            // Give bootstrap node time to initialize if it was just started
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Construct bootstrap address with proper format
            let bootstrap_addr = format!("{}:{}", host, bootstrap_port);
            info!("Attempting to join network through bootstrap node: {}", bootstrap_addr);

            // Join the network
            if let Err(e) = peer.join(bootstrap_addr).await {
                error!("Failed to join network: {}", e);
                return Err(format!("Failed to join network: {}", e));
            }

            info!("Successfully joined the network");
            info!("Node is running on port: {}", node_port);

            // Run the node until interrupted
            if let Err(e) = peer.run().await {
                error!("Node error: {}", e);
                return Err(format!("Node error: {}", e));
            }

            info!("Node shut down gracefully");
            Ok(())
        }
    }
}
