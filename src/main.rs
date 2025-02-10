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

const DEFAULT_NETWORK_BITS: u32 = 160; // Use 160-bit IDs by default (SHA-1)
const MIN_NETWORK_BITS: u32 = 32; // Minimum allowed network size
const MAX_NETWORK_BITS: u32 = 256; // Maximum allowed network size

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a new Chord node
    Start {
        /// Optional gRPC port (random if not specified)
        #[arg(short, long)]
        port: Option<u16>,

        /// Bootstrap node address (if joining existing network)
        #[arg(short, long)]
        bootstrap: Option<String>,

        /// Number of bits for node IDs (32-256, default: 160)
        #[arg(short, long, default_value_t = DEFAULT_NETWORK_BITS)]
        network_bits: u32,
    },
}

fn validate_network_bits(bits: u32) -> Result<(), String> {
    if bits < MIN_NETWORK_BITS || bits > MAX_NETWORK_BITS {
        Err(format!(
            "Network bits must be between {} and {}",
            MIN_NETWORK_BITS, MAX_NETWORK_BITS
        ))
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    // Initialize logging
    env_logger::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            port,
            bootstrap,
            network_bits,
        } => {
            // Validate network bits
            validate_network_bits(network_bits)?;

            // Create peer configuration with network size
            let config = PeerConfig {
                grpc_port: port,
                network_bits: Some(network_bits),
            };

            // Create and initialize peer
            let mut peer = ChordPeer::new(config)
                .await
                .map_err(|e| format!("Failed to create peer: {}", e))?;

            // If bootstrap address provided, join network
            if let Some(bootstrap_addr) = bootstrap {
                info!("Joining network through bootstrap node: {}", bootstrap_addr);
                match peer.join(bootstrap_addr).await {
                    Ok(_) => info!("Successfully joined the network"),
                    Err(e) => {
                        error!("Failed to join network: {}", e);
                        return Err(format!("Failed to join network: {}", e));
                    }
                }
            } else {
                info!("Starting as bootstrap node");
                // Initialize as first node in the network
                match peer.create_network().await {
                    Ok(_) => info!("Successfully created new network"),
                    Err(e) => {
                        error!("Failed to create network: {}", e);
                        return Err(format!("Failed to create network: {}", e));
                    }
                }
            }

            // Run the peer
            match peer.run().await {
                Ok(_) => info!("Peer shutdown gracefully"),
                Err(e) => {
                    error!("Peer error: {}", e);
                    return Err(format!("Peer error: {}", e));
                }
            }
        }
    }

    Ok(())
}
