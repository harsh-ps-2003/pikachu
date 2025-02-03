use crate::chord::actor::{ChordActor, ChordHandle, ChordMessage};
use crate::network::messages::message::Message;
use crate::error::*;
use crate::PeerConfig;
use libp2p::Swarm;
use libp2p::{
    futures::StreamExt,
    mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId, Multiaddr
};
use log::{debug, error, info, warn};
use std::error::Error as StdError;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::{io, select, time::sleep};
use crate::chord::{routing::{ChordRoutingBehaviour, ChordRoutingEvent}, types::NodeId};
use crate::network::client::ChordGrpcClient;
use crate::network::server::ChordGrpcServer;

#[derive(NetworkBehaviour)]
struct PeerBehaviour {
    chord_routing: ChordRoutingBehaviour,
    mdns: mdns::tokio::Behaviour, // For local peer discovery
}

pub struct ChordPeer {
    chord_handle: ChordHandle,
    swarm: Swarm<PeerBehaviour>,
}

impl ChordPeer {
    pub async fn new(config: PeerConfig) -> Result<Self, NetworkError> {
        let grpc_port = config.grpc_port.unwrap_or_else(|| get_random_port());
        let grpc_addr = format!("http://127.0.0.1:{}", grpc_port);
        
        let swarm = create_swarm()?;
        let peer_id = swarm.local_peer_id();
        let node_id = NodeId::from_peer_id(peer_id);
        
        let (chord_handle, chord_actor) = ChordHandle::new(
            node_id,
            grpc_port,
            grpc_addr,
        );
        
        // Start the gRPC server
        let server = ChordGrpcServer::new(chord_handle.clone());
        tokio::spawn(async move {
            server.serve(format!("127.0.0.1:{}", grpc_port)).await
        });
        
        // Start the chord actor
        tokio::spawn(async move {
            chord_actor.run().await;
        });
        
        Ok(Self {
            chord_handle,
            swarm,
        })
    }

    pub async fn run(&mut self) -> Result<(), NetworkError> {
        let listen_address = "/ip4/0.0.0.0/tcp/0".parse()
            .map_err(|e| NetworkError::Network(format!("Invalid listen address: {}", e)))?;

        self.swarm.listen_on(listen_address)
            .map_err(|e| NetworkError::Network(format!("Failed to listen: {}", e)))?;

        loop {
            select! {
                event = self.swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(PeerBehaviourEvent::ChordRouting(event)) => {
                        match event {
                            ChordRoutingEvent::SuccessorUpdated(peer) => {
                                info!("Successor updated: {}", peer);
                                self.handle_successor_update(peer).await?;
                            }
                            ChordRoutingEvent::RouteFound(target, next_hop) => {
                                debug!("Route found for {}: next hop {}", target, next_hop);
                                self.forward_request(target, next_hop).await?;
                            }
                        }
                    }
                    SwarmEvent::Behaviour(PeerBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, addr) in list {
                            info!("Discovered peer: {} at {}", peer_id, addr);
                            self.handle_discovered_peer(peer_id, addr).await?;
                        }
                    }
                    SwarmEvent::Behaviour(PeerBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, _) in list {
                            warn!("Peer expired: {}", peer_id);
                            // Handle peer expiration in chord routing
                            self.swarm.behaviour_mut().chord_routing.handle_peer_expired(&peer_id);
                        }
                    },
                    _ => {}
                },
                _ = sleep(Duration::from_secs(30)) => {
                    self.stabilize_chord_network().await?;
                }
            }
        }
    }

    async fn stabilize_chord_network(&mut self) -> Result<(), NetworkError> {
        // Update finger table
        for i in 0..8 {
            let target = self.calculate_finger_id(i);
            self.swarm.behaviour_mut().chord_routing
                .find_successor(target);
        }

        // Run Chord stabilization
        self.chord_handle.stabilize().await?;
        Ok(())
    }

    async fn handle_successor_update(&mut self, peer: PeerId) -> Result<(), NetworkError> {
        let node_id = NodeId::from_peer_id(&peer);
        self.chord_handle.update_successor(node_id).await?;
        Ok(())
    }

    async fn forward_request(&mut self, target: NodeId, next_hop: PeerId) -> Result<(), NetworkError> {
        // Get the gRPC address for the next_hop
        let addr = self.get_node_address(next_hop)?;
        
        // Create gRPC client
        let mut client = ChordGrpcClient::new(addr).await?;
        
        // Forward the request
        client.lookup(target.to_bytes()).await?;
        
        Ok(())
    }

    async fn handle_discovered_peer(&mut self, peer_id: PeerId, addr: Multiaddr) -> Result<(), NetworkError> {
        let node_id = NodeId::from_peer_id(&peer_id);
        
        // Extract gRPC port from multiaddr (assuming it's included in peer discovery)
        if let Some(grpc_port) = extract_grpc_port(&addr) {
            let grpc_addr = format!("http://127.0.0.1:{}", grpc_port);
            
            // Update the node's address mapping
            self.chord_handle.update_node_address(node_id, grpc_addr).await?;
        }
        
        // Add to Chord routing
        self.swarm.behaviour_mut().chord_routing.handle_peer_discovered(peer_id);
        
        // Notify Chord actor about the new node
        self.chord_handle.notify(node_id).await?;
        
        Ok(())
    }
}

fn create_swarm() -> Result<Swarm<PeerBehaviour>, Box<dyn StdError>> {
    let swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let chord_routing = ChordRoutingBehaviour::new();
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            
            Ok(PeerBehaviour {
                chord_routing,
                mdns,
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    Ok(swarm)
}

// Helper function to extract gRPC port from multiaddr
fn extract_grpc_port(addr: &Multiaddr) -> Option<u16> {
    for proto in addr.iter() {
        if let Protocol::Tcp(port) = proto {
            return Some(port);
        }
    }
    None
}

// Helper function to get a random available port
fn get_random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}
