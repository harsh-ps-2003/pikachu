use clap::{Parser, Subcommand};
use log::{error, info, warn, LevelFilter};
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
};
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
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Define localhost constant
const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Helper function to convert SocketAddr to gRPC URL
fn to_grpc_url(addr: SocketAddr) -> String {
    format!("http://{}", addr)
}

/// Setup file-based logging with the given port number
fn setup_logging(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let log_file = format!("{}.log", port);
    
    // Create a file appender
    let file_appender = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S%.3f)} {l} {t} - {m}{n}")))
        .build(log_file)?;

    // Build the logger configuration
    let config = Config::builder()
        .appender(Appender::builder().build("file", Box::new(file_appender)))
        .build(Root::builder().appender("file").build(LevelFilter::Debug))?;

    // Initialize the logger
    log4rs::init_config(config)?;

    Ok(())
}

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
    let cli = Cli::parse();

    match cli.command {
        Commands::StartBootstrap { port } => {
            // Create peer configuration
            let config = PeerConfig { grpc_port: port };

            // Create and initialize peer
            let mut peer = ChordPeer::new(config)
                .await
                .map_err(|e| format!("Failed to create peer: {}", e))?;

            let node_port = peer.get_port();
            
            // Setup logging for this node
            setup_logging(node_port).map_err(|e| format!("Failed to setup logging: {}", e))?;
            
            let node_addr = SocketAddr::new(LOCALHOST, node_port);
            info!("Starting bootstrap node on {}", node_addr);

            // Initialize as first node in the network
            if let Err(e) = peer.create_network().await {
                error!("Failed to create network: {}", e);
                return Err(format!("Failed to create network: {}", e));
            }

            info!("Successfully created new network as bootstrap node");
            info!("Bootstrap node is running on: {}", node_addr);
            info!(
                "Other nodes can join using: cargo run join -b {} -p <PORT>",
                node_port
            );

            // Run the node until interrupted
            if let Err(e) = peer.run().await {
                error!("Bootstrap node error: {}", e);
                return Err(format!("Bootstrap node error: {}", e));
            }

            info!("Bootstrap node shut down gracefully");
            Ok(())
        }
        Commands::Join {
            port,
            bootstrap_port,
            host,
        } => {
            // Create peer configuration
            let config = PeerConfig { grpc_port: port };

            // Create and initialize peer
            let mut peer = ChordPeer::new(config)
                .await
                .map_err(|e| format!("Failed to create peer: {}", e))?;

            let node_port = peer.get_port();
            
            // Setup logging for this node
            setup_logging(node_port).map_err(|e| format!("Failed to setup logging: {}", e))?;
            
            let node_addr = SocketAddr::new(LOCALHOST, node_port);
            info!("Starting node on {}", node_addr);

            // Construct bootstrap address
            let bootstrap_addr = SocketAddr::new(LOCALHOST, bootstrap_port);
            info!(
                "Attempting to join network through bootstrap node: {}",
                bootstrap_addr
            );

            // Join the network using proper gRPC URL
            if let Err(e) = peer.join(to_grpc_url(bootstrap_addr)).await {
                error!("Failed to join network: {}", e);
                return Err(format!("Failed to join network: {}", e));
            }

            info!("Successfully joined the network");
            info!("Node is running on: {}", node_addr);

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
