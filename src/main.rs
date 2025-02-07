use clap::{Parser, Subcommand};
use libp2p::Multiaddr;
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info")
    );

    let cli = Cli::parse();

    match cli.command {
        Commands::Start { port, bootstrap } => {
            // Create peer config
            let config = PeerConfig {
                grpc_port: port,
            };

            // Create and start peer
            let mut peer = ChordPeer::new(config).await
                .map_err(|e| format!("Failed to create peer: {}", e))?;

            // Initialize shared state
            let finger_table = Arc::new(Mutex::new(peer.get_finger_table().clone()));
            let storage = Arc::new(Mutex::new(HashMap::new()));
            let predecessor = Arc::new(Mutex::new(None));
            let successor_list = Arc::new(Mutex::new(Vec::new()));

            // Create thread configuration
            let local_addr = format!("http://127.0.0.1:{}", peer.get_port());
            let thread_config = ThreadConfig::new(
                local_addr.clone(),
                finger_table.clone(),
                storage.clone(),
                predecessor.clone(),
                successor_list.clone(),
            );

            // If bootstrap address provided, join network
            if let Some(bootstrap_addr) = bootstrap {
                info!("Joining network through bootstrap node: {}", bootstrap_addr);
                
                // Parse bootstrap address
                let addr = Multiaddr::from_str(&bootstrap_addr)
                    .map_err(|e| format!("Invalid bootstrap address: {}", e))?;
                
                peer.join(addr).await
                    .map_err(|e| format!("Failed to join network: {}", e))?;
            } else {
                info!("Starting as bootstrap node");
            }

            // Spawn worker threads
            let stabilize_handle = tokio::spawn(run_stabilize_worker(thread_config.clone()));
            let predecessor_handle = tokio::spawn(run_predecessor_checker(thread_config.clone()));
            let finger_handle = tokio::spawn(run_finger_maintainer(thread_config.clone()));
            let successor_handle = tokio::spawn(run_successor_maintainer(thread_config.clone()));

            // Run the peer and wait for all tasks
            let peer_handle = tokio::spawn(async move {
                if let Err(e) = peer.run().await {
                    error!("Peer error: {}", e);
                }
            });

            // Wait for all tasks to complete
            tokio::select! {
                _ = stabilize_handle => error!("Stabilize worker exited"),
                _ = predecessor_handle => error!("Predecessor checker exited"),
                _ = finger_handle => error!("Finger maintainer exited"),
                _ = successor_handle => error!("Successor maintainer exited"),
                _ = peer_handle => error!("Peer exited"),
            }

            std::process::exit(1);
        }
    }

    Ok(())
}
