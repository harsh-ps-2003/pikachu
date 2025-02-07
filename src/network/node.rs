use crate::chord::actor::{ChordActor, ChordHandle, ChordMessage};
use crate::error::*;
use crate::network::grpc::PeerConfig;
use libp2p::{
    Swarm,
    futures::StreamExt,
    mdns::{self, Event as MdnsEvent},
    noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId, Multiaddr,
    multiaddr::Protocol,
};
#[allow(unused_imports)]
use libp2p::swarm::derive_prelude::*;
use log::{debug, error, info, warn};
use std::error::Error as StdError;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::{io, select, time::sleep};
use crate::chord::{
    routing::{ChordRoutingBehaviour, ChordRoutingEvent}, 
    types::NodeId
};
use crate::network::grpc::{client::ChordGrpcClient, server::ChordGrpcServer};
use crate::chord::CHORD_PROTOCOL;
use std::collections::HashMap;

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "ComposedEvent")]
pub struct PeerBehaviour {
    chord_routing: ChordRoutingBehaviour,
    mdns: mdns::tokio::Behaviour,
}

// Define the combined event type
#[derive(Debug)]
pub enum ComposedEvent {
    ChordRouting(ChordRoutingEvent),
    Mdns(MdnsEvent),
}

impl From<ChordRoutingEvent> for ComposedEvent {
    fn from(event: ChordRoutingEvent) -> Self {
        ComposedEvent::ChordRouting(event)
    }
}

impl From<MdnsEvent> for ComposedEvent {
    fn from(event: MdnsEvent) -> Self {
        ComposedEvent::Mdns(event)
    }
}

pub struct ChordPeer {
    chord_handle: ChordHandle,
    swarm: Swarm<PeerBehaviour>,
    // Track discovered peers and their addresses
    discovered_peers: HashMap<NodeId, Multiaddr>,
}

impl ChordPeer {
    pub async fn new(config: PeerConfig) -> Result<Self, NetworkError> {
        let mut swarm = create_swarm()?;

        // Listen on the configured port
        let listen_addr = format!("/ip4/0.0.0.0/tcp/{}", config.port)
            .parse()
            .map_err(|e| NetworkError::ConfigError(e.to_string()))?;
        swarm.listen_on(listen_addr)?;
        
        let peer_id = *swarm.local_peer_id();
        let node_id = NodeId::from_peer_id(&peer_id);
        
        // Use the actual listening address from mDNS
        let listen_addr = swarm.listeners().next()
            .ok_or_else(|| NetworkError::ConfigError("No listening address".into()))?
            .clone();
        
        let (chord_handle, chord_actor) = ChordHandle::new(node_id, listen_addr);
        
        tokio::spawn(async move {
            chord_actor.run().await;
        });
        
        Ok(Self {
            chord_handle,
            swarm,
            discovered_peers: HashMap::new(),
        })
    }

    pub async fn run(&mut self) -> Result<(), NetworkError> {
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(60));
        
        loop {
            select! {
                _ = cleanup_interval.tick() => {
                    self.cleanup_stale_addresses().await?;
                }
                event = self.swarm.next() => match event {
                    Some(SwarmEvent::Behaviour(ComposedEvent::ChordRouting(event))) => {
                        match event {
                            ChordRoutingEvent::SuccessorUpdated(peer) => {
                                info!("Successor updated: {}", peer);
                                self.handle_successor_update(peer).await?;
                            }
                            ChordRoutingEvent::RouteFound(target, next_hop) => {
                                debug!("Route found for {}: next hop {}", target, next_hop);
                                self.forward_request(target, next_hop).await?;
                            }
                            _ => {}
                        }
                    }
                    Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                        // Verify protocol version
                        if let Some(protocols) = self.swarm.behaviour().supported_protocols(&peer_id) {
                            if !protocols.contains(&CHORD_PROTOCOL.to_vec()) {
                                warn!("Peer {} doesn't support Chord protocol", peer_id);
                                self.swarm.disconnect(peer_id)?;
                            }
                        }
                    }
                    SwarmEvent::Behaviour(ComposedEvent::Mdns(MdnsEvent::Discovered(peers))) => {
                        for (peer_id, addr) in peers {
                            info!("Discovered peer: {} at {}", peer_id, addr);
                            self.handle_discovered_peer(peer_id, addr).await?;
                        }
                    }
                    SwarmEvent::Behaviour(ComposedEvent::Mdns(MdnsEvent::Expired(peers))) => {
                        for (peer_id, _) in peers {
                            warn!("Peer expired: {}", peer_id);
                            self.handle_peer_expired(&peer_id)?;
                        }
                    }
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
        
        // Forward the request - convert NodeId to Vec<u8>
        client.lookup(target.to_bytes().to_vec()).await?;
        
        Ok(())
    }

    /// Get gRPC address for a node. Since we're using mDNS, we assume localhost
    fn get_grpc_address(&self, node_id: &NodeId) -> Result<String, NetworkError> {
        let addr = self.discovered_peers.get(node_id)
            .ok_or_else(|| NetworkError::PeerNotFound(format!("No address for node {}", node_id)))?;
            
        // Extract port from multiaddr
        let mut port = None;
        
        for proto in addr.iter() {
            if let Protocol::Tcp(p) = proto {
                port = Some(p);
                break;
            }
        }
        
        match port {
            Some(p) => Ok(format!("http://127.0.0.1:{}", p)),
            None => Err(NetworkError::InvalidAddress(
                format!("No TCP port in address: {}", addr)
            ))
        }
    }

    async fn handle_discovered_peer(&mut self, peer_id: PeerId, addr: Multiaddr) -> Result<(), NetworkError> {
        let node_id = NodeId::from_peer_id(&peer_id);
        
        // Store the discovered peer
        self.discovered_peers.insert(node_id, addr.clone());
        
        // Update Chord routing
        self.swarm.behaviour_mut().chord_routing.handle_peer_discovered(peer_id);
        
        // Notify Chord actor about the new peer
        self.chord_handle.discovered_peer(node_id).await?;
        
        Ok(())
    }

    async fn handle_peer_expired(&mut self, peer_id: &PeerId) -> Result<(), NetworkError> {
        let node_id = NodeId::from_peer_id(peer_id);
        
        // Remove from discovered peers
        self.discovered_peers.remove(&node_id);
        
        // Update Chord routing
        self.swarm.behaviour_mut().chord_routing.handle_peer_expired(peer_id);
        
        // Notify Chord actor
        self.chord_handle.peer_expired(node_id).await?;
        
        Ok(())
    }

    async fn cleanup_stale_addresses(&mut self) -> Result<(), NetworkError> {
        let mut departed_nodes = Vec::new();
        
        // Check which nodes are no longer reachable
        for (node_id, addr) in &self.state.node_addresses {
            match ChordState::to_grpc_addr(addr)
                .and_then(|addr| ChordGrpcClient::new(&addr).await
                    .map_err(|e| ChordError::InvalidRequest(e.to_string()))) 
            {
                Ok(_) => {}, // Node is alive
                Err(_) => departed_nodes.push(*node_id),
            }
        }

        // Handle each departed node
        for node_id in departed_nodes {
            self.chord_handle.notify_node_left(node_id).await?;
        }

        Ok(())
    }

    async fn get_all_node_addresses(&mut self) -> Result<Vec<(NodeId, String)>, NetworkError> {
        let (send, recv) = oneshot::channel();
        self.chord_handle
            .sender
            .send(ChordMessage::GetAllNodeAddresses {
                respond_to: send,
            })
            .await
            .map_err(|_| NetworkError::Internal("Failed to send message".into()))?;
        
        recv.await
            .map_err(|_| NetworkError::Internal("Failed to receive response".into()))?
            .map_err(|e| NetworkError::Internal(e.to_string()))
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
        .with_swarm_config(|c| {
            c.with_idle_connection_timeout(Duration::from_secs(60))
             .with_protocol_version(CHORD_PROTOCOL)
        })
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
