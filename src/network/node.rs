use crate::chord::actor::{ChordActor, ChordHandle, ChordMessage};
use crate::error::*;
use crate::network::grpc::PeerConfig;
use futures::StreamExt;
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed, upgrade},
    identity,
    mdns::{self, Event as MdnsEvent},
    noise, tls,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
    Multiaddr, PeerId, Transport, SwarmBuilder,
};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use crate::chord::{
    routing::{ChordRoutingBehaviour, ChordRoutingEvent},
    types::{NodeId, ChordNode, KEY_SIZE, Key, Value},
    CHORD_PROTOCOL,
};
use crate::network::grpc::{client::ChordGrpcClient, server::ChordGrpcServer};

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "ChordEvent")]
pub struct PeerBehaviour {
    chord_routing: ChordRoutingBehaviour,
    mdns: mdns::tokio::Behaviour,
}

#[derive(Debug)]
pub enum ChordEvent {
    ChordRouting(ChordRoutingEvent),
    Mdns(MdnsEvent),
}

impl From<ChordRoutingEvent> for ChordEvent {
    fn from(event: ChordRoutingEvent) -> Self {
        ChordEvent::ChordRouting(event)
    }
}

impl From<MdnsEvent> for ChordEvent {
    fn from(event: MdnsEvent) -> Self {
        ChordEvent::Mdns(event)
    }
}

pub struct ChordPeer {
    chord_handle: ChordHandle,
    swarm: libp2p::Swarm<PeerBehaviour>,
    discovered_peers: HashMap<NodeId, Multiaddr>,
    chord_node: ChordNode,
}

impl ChordPeer {
    pub async fn new(config: PeerConfig) -> Result<Self, NetworkError> {
        // Create a random PeerId
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        let node_id = NodeId::from_peer_id(&local_peer_id);

        // Create the swarm
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new(&local_key).map_err(|e| NetworkError::Transport(e.to_string()))?,
                yamux::Config::default,
            )
            .map_err(|e| NetworkError::Transport(format!("Failed to create TCP transport: {}", e)))?
            .with_behaviour(|key| {
                Ok(PeerBehaviour {
                    chord_routing: ChordRoutingBehaviour::new(NodeId::from_peer_id(&key.public().to_peer_id())),
                    mdns: mdns::tokio::Behaviour::new(
                        mdns::Config::default(),
                        key.public().to_peer_id(),
                    )?,
                })
            })
            .map_err(|e| NetworkError::Transport(format!("Failed to create behaviour: {}", e)))?
            .build();

        // Listen on all interfaces and whatever port the OS assigns
        let port = config.grpc_port.unwrap_or_else(|| get_random_port());
        swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{}", port).parse()
            .map_err(|e| NetworkError::Transport(format!("Invalid multiaddr: {}", e)))?)?;

        // Initialize ChordNode
        let local_addr = format!("http://127.0.0.1:{}", port);
        let chord_node = ChordNode::new(node_id, local_addr.clone()).await;

        // Create ChordHandle and actor
        let (chord_handle, chord_actor) = ChordHandle::new(
            node_id,
            port,
            local_addr,
        );

        // Spawn actor
        tokio::spawn(async move {
            chord_actor.run().await;
        });

        Ok(Self {
            chord_handle,
            swarm,
            discovered_peers: HashMap::new(),
            chord_node,
        })
    }

    pub async fn join(&mut self, bootstrap_addr: Multiaddr) -> Result<(), NetworkError> {
        // Extract gRPC address from bootstrap multiaddr
        let bootstrap_port = get_port_from_multiaddr(&bootstrap_addr)?;
        let bootstrap_grpc = format!("http://127.0.0.1:{}", bootstrap_port);

        // Join the Chord network
        self.chord_node.join_network(Some(bootstrap_grpc))
            .await
            .map_err(NetworkError::Chord)?;

        // Share state with worker threads
        let thread_config = self.chord_node.get_shared_state();
        
        // Spawn worker threads
        tokio::spawn(run_stabilize_worker(thread_config.clone()));
        tokio::spawn(run_predecessor_checker(thread_config.clone()));
        tokio::spawn(run_finger_maintainer(thread_config.clone()));
        tokio::spawn(run_successor_maintainer(thread_config.clone()));

        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), NetworkError> {
        let mut stabilize_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            select! {
                event = self.swarm.select_next_some() => {
                    match event {
                        SwarmEvent::Behaviour(ChordEvent::Mdns(MdnsEvent::Discovered(peers))) => {
                            for (peer_id, addr) in peers {
                                info!("Discovered peer: {} at {}", peer_id, addr);
                                self.handle_peer_discovered(peer_id, addr).await?;
                            }
                        }
                        SwarmEvent::Behaviour(ChordEvent::Mdns(MdnsEvent::Expired(peers))) => {
                            for (peer_id, _addr) in peers {
                                warn!("Peer expired: {}", peer_id);
                                self.handle_peer_expired(peer_id).await?;
                            }
                        }
                        SwarmEvent::Behaviour(ChordEvent::ChordRouting(event)) => {
                            self.handle_chord_event(event).await?;
                        }
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!("Listening on {}", address);
                        }
                        _ => {}
                    }
                }
                _ = stabilize_interval.tick() => {
                    self.stabilize().await?;
                }
            }
        }
    }

    async fn handle_peer_discovered(&mut self, peer_id: PeerId, addr: Multiaddr) -> Result<(), NetworkError> {
        let node_id = NodeId::from_peer_id(&peer_id);
        self.discovered_peers.insert(node_id, addr);
        self.swarm.behaviour_mut().chord_routing.handle_peer_discovered(peer_id);
        self.chord_handle.notify_peer_discovered(node_id)
            .await
            .map_err(|e| NetworkError::Chord(e))?;
        Ok(())
    }

    async fn handle_peer_expired(&mut self, peer_id: PeerId) -> Result<(), NetworkError> {
        let node_id = NodeId::from_peer_id(&peer_id);
        self.discovered_peers.remove(&node_id);
        self.swarm.behaviour_mut().chord_routing.handle_peer_expired(&peer_id);
        self.chord_handle.notify_peer_expired(node_id)
            .await
            .map_err(|e| NetworkError::Chord(e))?;
        Ok(())
    }

    async fn handle_chord_event(&mut self, event: ChordRoutingEvent) -> Result<(), NetworkError> {
        match event {
            ChordRoutingEvent::SuccessorUpdated(peer) => {
                let node_id = NodeId::from_peer_id(&peer);
                self.chord_handle.update_successor(node_id)
                    .await
                    .map_err(|e| NetworkError::Chord(e))?;
            }
            ChordRoutingEvent::RouteFound(target, next_hop) => {
                self.forward_request(target, next_hop).await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn stabilize(&mut self) -> Result<(), NetworkError> {
        // Update finger table
        for i in 0..KEY_SIZE {
            let target = self.calculate_finger_id(i);
            self.swarm.behaviour_mut().chord_routing.find_successor(target);
        }

        // Run Chord stabilization
        self.chord_handle.stabilize()
            .await
            .map_err(|e| NetworkError::Chord(e))?;
        Ok(())
    }

    fn calculate_finger_id(&self, index: usize) -> NodeId {
        let node_id = NodeId::from_peer_id(&self.swarm.local_peer_id());
        node_id.get_finger_id(index)
    }

    async fn forward_request(&mut self, target: NodeId, next_hop: PeerId) -> Result<(), NetworkError> {
        if let Some(addr) = self.discovered_peers.get(&NodeId::from_peer_id(&next_hop)) {
            let grpc_addr = format!("http://127.0.0.1:{}", get_port_from_multiaddr(addr)?);
            let mut client = ChordGrpcClient::new(grpc_addr.clone())
                .await
                .map_err(|e| NetworkError::Grpc(format!("Failed to create gRPC client: {}", e)))?;
            client.lookup(target.to_bytes().to_vec())
                .await
                .map_err(|e| NetworkError::Grpc(format!("Failed to forward lookup request: {}", e)))?;
        }
        Ok(())
    }

    pub async fn store_value(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), NetworkError> {
        let key = Key(key);
        let value = Value(value);
        let key_id = NodeId::from_bytes(&key.0);
        
        // Find the node responsible for this key
        self.swarm.behaviour_mut().chord_routing.find_successor(key_id);
        
        // Wait for the route to be found
        while let Some(event) = self.swarm.next().await {
            match event {
                SwarmEvent::Behaviour(ChordEvent::ChordRouting(ChordRoutingEvent::RouteFound(target_id, next_hop))) => {
                    if target_id == key_id {
                        // Forward the store request to the responsible node
                        if let Some(addr) = self.discovered_peers.get(&NodeId::from_peer_id(&next_hop)) {
                            let grpc_addr = format!("http://127.0.0.1:{}", get_port_from_multiaddr(addr)?);
                            let mut client = ChordGrpcClient::new(&grpc_addr)
                                .await
                                .map_err(|e| NetworkError::Grpc(format!("Failed to create gRPC client: {}", e)))?;
                            
                            client.store(key.0, value.0)
                                .await
                                .map_err(|e| NetworkError::Grpc(format!("Failed to store value: {}", e)))?;
                            
                            return Ok(());
                        }
                    }
                }
                SwarmEvent::Behaviour(ChordEvent::ChordRouting(ChordRoutingEvent::RoutingError(_, msg))) => {
                    return Err(NetworkError::Chord(ChordError::RoutingError(msg)));
                }
                _ => {}
            }
        }
        
        Err(NetworkError::Chord(ChordError::OperationFailed("Failed to find responsible node".into())))
    }

    pub async fn retrieve_value(&mut self, key: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        let key = Key(key);
        let key_id = NodeId::from_bytes(&key.0);
        
        // Find the node responsible for this key
        self.swarm.behaviour_mut().chord_routing.find_successor(key_id);
        
        // Wait for the route to be found
        while let Some(event) = self.swarm.next().await {
            match event {
                SwarmEvent::Behaviour(ChordEvent::ChordRouting(ChordRoutingEvent::RouteFound(target_id, next_hop))) => {
                    if target_id == key_id {
                        // Forward the retrieve request to the responsible node
                        if let Some(addr) = self.discovered_peers.get(&NodeId::from_peer_id(&next_hop)) {
                            let grpc_addr = format!("http://127.0.0.1:{}", get_port_from_multiaddr(addr)?);
                            let mut client = ChordGrpcClient::new(&grpc_addr)
                                .await
                                .map_err(|e| NetworkError::Grpc(format!("Failed to create gRPC client: {}", e)))?;
                            
                            let value = client.lookup(key.0)
                                .await
                                .map_err(|e| NetworkError::Grpc(format!("Failed to retrieve value: {}", e)))?;
                            
                            return Ok(value);
                        }
                    }
                }
                SwarmEvent::Behaviour(ChordEvent::ChordRouting(ChordRoutingEvent::RoutingError(_, msg))) => {
                    return Err(NetworkError::Chord(ChordError::RoutingError(msg)));
                }
                _ => {}
            }
        }
        
        Err(NetworkError::Chord(ChordError::OperationFailed("Failed to find responsible node".into())))
    }
}

fn get_random_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn get_port_from_multiaddr(addr: &Multiaddr) -> Result<u16, NetworkError> {
    use libp2p::multiaddr::Protocol;
    
    for proto in addr.iter() {
        if let Protocol::Tcp(port) = proto {
            return Ok(port);
        }
    }
    
    Err(NetworkError::InvalidAddress(format!("No TCP port in address: {}", addr)))
}
