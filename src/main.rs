use clap::{Parser, Subcommand};
use libp2p::Multiaddr;
use log::{error, info};
use std::str::FromStr;
use pikachu::{
    network::{node::ChordPeer, grpc::PeerConfig},
    chord::types::NodeId,
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

            // Run the peer
            if let Err(e) = peer.run().await {
                error!("Peer error: {}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
