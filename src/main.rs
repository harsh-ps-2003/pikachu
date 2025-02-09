use clap::{Parser, Subcommand};
use log::{error, info};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use pikachu::{
    network::{node::ChordPeer, grpc::PeerConfig},
    chord::{
        types::{NodeId, ThreadConfig, SharedFingerTable, SharedStorage, SharedPredecessor, SharedSuccessorList},
        workers::{run_stabilize_worker, run_predecessor_checker, run_finger_maintainer, run_successor_maintainer},
    },
    error::NetworkError,
};

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
    },
}

#[tokio::main]
async fn main() -> Result<(), String> {
    // Initialize logging
    env_logger::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start { port, bootstrap } => {
            // Create peer configuration
            let config = PeerConfig {
                grpc_port: port,
            };

            // Create and initialize peer
            let mut peer = ChordPeer::new(config)
                .await
                .map_err(|e| format!("Failed to create peer: {}", e))?;

            // If bootstrap address provided, join network
            if let Some(bootstrap_addr) = bootstrap {
                info!("Joining network through bootstrap node: {}", bootstrap_addr);
                peer.join(bootstrap_addr).await
                    .map_err(|e| format!("Failed to join network: {}", e))?;
            } else {
                info!("Starting as bootstrap node");
            }

            // Run the peer
            peer.run()
                .await
                .map_err(|e| format!("Peer error: {}", e))?;
        }
    }

    Ok(())
}
