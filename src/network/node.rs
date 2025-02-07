use crate::chord::actor::{ChordActor, ChordHandle, ChordMessage};
use crate::error::*;
use crate::network::grpc::PeerConfig;
use libp2p::{
    Swarm, SwarmBuilder,
    Transport,
    futures::StreamExt,
    mdns::{self, Event as MdnsEvent},
    noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId, Multiaddr,
    multiaddr::Protocol,
    core::upgrade,
    identity,
};
use libp2p::swarm::derive_prelude::*;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::{select, time::sleep};
use crate::chord::{
    routing::{ChordRoutingBehaviour, ChordRoutingEvent}, 
    types::NodeId,
    CHORD_PROTOCOL,
};
use crate::network::grpc::{client::ChordGrpcClient, server::ChordGrpcServer};

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "ComposedEvent")]
pub struct PeerBehaviour {
    chord_routing: ChordRoutingBehaviour,
    mdns: mdns::tokio::Behaviour,
}

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
    discovered_peers: HashMap<NodeId, Multiaddr>,
}

impl ChordPeer {
    pub async fn new(config: PeerConfig) -> Result<Self, NetworkError> {
        // Generate new key pair
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        let node_id = NodeId::from_peer_id(&local_peer_id);

        // Create transport
        let transport = Transport::new(
            tcp::Config::default(),
            noise::Config::new(&local_key)?,
            yamux::Config::default(),
        )?;

        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new(&local_key)?,
                yamux::Config::default(),
            )?
            .with_behaviour(|key| {
                Ok(Behaviour {
                    chord: ChordRoutingBehaviour::new(),
                    mdns: mdns::tokio::Behaviour::new(
                        mdns::Config::default(),
                        key.public().to_peer_id(),
                    )?,
                })
            })?
            .build();

        // Listen on all interfaces and whatever port the OS assigns
        let port = config.grpc_port.unwrap_or_else(|| get_random_port());
        let listen_addr = format!("/ip4/0.0.0.0/tcp/{}", port)
            .parse()
            .map_err(|e| NetworkError::ConfigError(e.to_string()))?;
        
        swarm.listen_on(listen_addr)?;

        // Create ChordHandle and actor
        let (chord_handle, chord_actor) = ChordHandle::new(
            node_id,
            port,
            format!("http://127.0.0.1:{}", port),
        );

        // Spawn actor
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
        let mut stabilize_interval = tokio::time::interval(Duration::from_secs(30));
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            select! {
                swarm_event = self.swarm.next() => match swarm_event {
                    Some(SwarmEvent::Behaviour(ComposedEvent::Mdns(MdnsEvent::Discovered(peers)))) => {
                        for (peer_id, addr) in peers {
                            info!("Discovered peer: {} at {}", peer_id, addr);
                            self.handle_discovered_peer(peer_id, addr).await?;
                        }
                    }
                    Some(SwarmEvent::Behaviour(ComposedEvent::Mdns(MdnsEvent::Expired(peers)))) => {
                        for (peer_id, _) in peers {
                            warn!("Peer expired: {}", peer_id);
                            self.handle_peer_expired(&peer_id).await?;
                        }
                    }
                    Some(SwarmEvent::Behaviour(ComposedEvent::ChordRouting(event))) => {
                        self.handle_chord_event(event).await?;
                    }
                    Some(SwarmEvent::NewListenAddr { address, .. }) => {
                        info!("Listening on {}", address);
                    }
                    Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                        debug!("Connected to {}", peer_id);
                    }
                    Some(SwarmEvent::ConnectionClosed { peer_id, .. }) => {
                        debug!("Disconnected from {}", peer_id);
                    }
                    _ => {}
                },
                _ = stabilize_interval.tick() => {
                    self.stabilize_chord_network().await?;
                }
                _ = cleanup_interval.tick() => {
                    self.cleanup_stale_addresses().await?;
                }
            }
        }
    }

    async fn handle_chord_event(&mut self, event: ChordRoutingEvent) -> Result<(), NetworkError> {
        match event {
            ChordRoutingEvent::SuccessorUpdated(peer) => {
                let node_id = NodeId::from_peer_id(&peer);
                self.chord_handle.update_successor(node_id).await?;
            }
            ChordRoutingEvent::RouteFound(target, next_hop) => {
                self.forward_request(target, next_hop).await?;
            }
            _ => {}
        }
        Ok(())
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

    fn calculate_finger_id(&self, index: u8) -> NodeId {
        // Implementation needed based on your Chord protocol
        unimplemented!()
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
        
        // Notify Chord actor
        let (send, recv) = oneshot::channel();
        self.chord_handle
            .sender
            .send(ChordMessage::PeerDiscovered {
                node_id,
                respond_to: send,
            })
            .await
            .map_err(|_| NetworkError::Internal("Failed to send peer discovered message".into()))?;

        recv.await
            .map_err(|_| NetworkError::Internal("Failed to receive response".into()))?
            .map_err(|e| NetworkError::Internal(e.to_string()))?;
        
        Ok(())
    }

    async fn handle_peer_expired(&mut self, peer_id: &PeerId) -> Result<(), NetworkError> {
        let node_id = NodeId::from_peer_id(peer_id);
        
        // Remove from discovered peers
        self.discovered_peers.remove(&node_id);
        
        // Update Chord routing
        self.swarm.behaviour_mut().chord_routing.handle_peer_expired(peer_id);
        
        // Notify Chord actor
        let (send, recv) = oneshot::channel();
        self.chord_handle
            .sender
            .send(ChordMessage::NodeUnavailable {
                node: node_id,
                respond_to: send,
            })
            .await
            .map_err(|_| NetworkError::Internal("Failed to send node unavailable message".into()))?;

        recv.await
            .map_err(|_| NetworkError::Internal("Failed to receive response".into()))?
            .map_err(|e| NetworkError::Internal(e.to_string()))?;
        
        Ok(())
    }

    async fn cleanup_stale_addresses(&mut self) -> Result<(), NetworkError> {
        let mut departed_nodes = Vec::new();
        
        // Check which nodes are no longer reachable
        for (node_id, _) in &self.discovered_peers {
            if let Ok(addr) = self.get_grpc_address(node_id) {
                match ChordGrpcClient::new(&addr).await {
                    Ok(_) => {}, // Node is alive
                    Err(_) => departed_nodes.push(*node_id),
                }
            }
        }

        // Handle each departed node
        for node_id in departed_nodes {
            let (send, recv) = oneshot::channel();
            self.chord_handle
                .sender
                .send(ChordMessage::NodeUnavailable {
                    node: node_id,
                    respond_to: send,
                })
                .await
                .map_err(|_| NetworkError::Internal("Failed to send node unavailable message".into()))?;

            recv.await
                .map_err(|_| NetworkError::Internal("Failed to receive response".into()))?
                .map_err(|e| NetworkError::Internal(e.to_string()))?;
        }

        Ok(())
    }

    async fn forward_request(&mut self, target: NodeId, next_hop: PeerId) -> Result<(), NetworkError> {
        // Get the gRPC address for the next_hop
        let addr = self.get_grpc_address(&next_hop)?;
        
        // Create gRPC client
        let mut client = ChordGrpcClient::new(addr).await?;
        
        // Forward the request - convert NodeId to Vec<u8>
        client.lookup(target.to_bytes().to_vec()).await?;
        
        Ok(())
    }
}

// Helper function to get a random available port
fn get_random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}
