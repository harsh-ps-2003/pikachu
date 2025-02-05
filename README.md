# Pikachu 

A Byzantine Fault Tolerant Distributed Hash Table implementation.

### System Design

PeerId is provided by libp2p and is used at the networking layer for peer discovery, connection establishment, and secure communication. When you run a node, libp2p generates a unique PeerId for that instance. NodeId is derived from the PeerId and is used for the overlay routing in the Chord DHT. The Chord protocol uses NodeIds to determine the position of each node within the ring and to decide which node is responsible for a particular key. Every node runs its own gRPC server which is bound to an address such as "http://127.0.0.1:{port}". This server is the entry point for application-level (DHT) requests.

The mapping from NodeId → gRPC address is essential because while the Chord protocol works with NodeIds to locate the correct node in the DHT (via finger tables and routing logic), it doesn't by itself specify how to reach that node over the network. In our local terminal setup, each node gets a unique gRPC server port. When a node needs to forward a Put/Get or key-transfer request, it looks up the responsible node's NodeId in its locally maintained mapping to obtain the correct gRPC address. Then it uses that address to create a gRPC client and forward the request. mDNS is used for automatic peer discovery on the local network. When a node discovers another node via mDNS, it learns the peer's libp2p identity (its PeerId) and any advertised network endpoints.

Since the NodeId is derived from the PeerId, you can compute the NodeId from the discovered PeerId. At the same time, the advertised network address (or the gRPC port from a multiaddr) is used to build the mapping: NodeId → gRPC address.
This lets each node maintain an up-to-date address mapping so that routing (e.g., contacting a successor or transferring keys) can be done over gRPC.

In summary:
1. The Chord protocol routes operations (lookup, Put, Get, key transfer) based on NodeIds
2. Each node's position in the DHT ring revolves around its NodeId
3. While you pinpoint the responsible node in the ring using NodeId comparisons, you still need to contact that node over the network
4. The gRPC address provides the necessary connection endpoint
5. This separation of logical overlay versus physical network address provides flexibility for different deployment scenarios

gRPC Server on Every Node
What it does:
Each node in the network launches its own gRPC server. The server listens on a unique address (for example, "http://127.0.0.1:{port}") and exposes RPC endpoints such as lookup and replicate (among others).
Role in DHT:
The gRPC server serves incoming DHT requests from other nodes. For instance, if node A is not responsible for a key, another node (say, node B) can forward a lookup or replication request to node A by calling these RPC methods.
Code Reference:
In src/network/grpc/server.rs, the ChordGrpcServer struct implements the gRPC service (via Tonic). The lookup method and the replicate method illustrate how incoming requests are handled:
When a lookup is received, ChordGrpcServer calls chord_node.lookup(req.key) to perform the DHT operation and responds with the result.
Similarly, on receiving a replicate request, it iterates over the key-value pairs and stores them using chord_node.put.
gRPC Client on Every Node
What it does:
In addition to running a server, each node can initiate its own gRPC clients to communicate with other nodes. When a node determines that it is not responsible for a given key or needs to forward a DHT operation (like key transfer, Put/Get, etc.), it creates a gRPC client (using, for example, the ChordGrpcClient from src/network/grpc/client.rs).
Role in DHT:
The client connects to the other node's gRPC server using the network address obtained previously (via mDNS or from an address mapping like NodeId → gRPC address). This enables a node to forward requests over a secure and structured RPC mechanism.
Combination in p2p:
Every node is both an echo-server and a caller—ready to handle incoming requests on its server port and, when needed, capable of making outgoing calls to peers. This dual role supports effective p2p communication where nodes directly exchange DHT protocol messages.
---
How gRPC Fits in the p2p Communication Flow
1. Peer Discovery
Nodes use mDNS for local discovery. When a node learns of a new peer (by its libp2p PeerId), it also extracts the advertised gRPC address (port on localhost).
Building the Overlay
The discovered PeerId is transformed into a NodeId, which is used in constructing the overlay (i.e., creating the Chord ring, filling finger tables, etc.).
Simultaneously, each node stores a mapping from its NodeId to its gRPC address. This mapping makes it possible to look up the correct connection endpoint when needing to forward a message.
3. Operations & Request Forwarding
Example (Lookup/Put/Get):
When a node receives a high-level DHT operation (say, a Put request), it calculates which node in the ring should handle the key.
If it determines that it isn't the responsible node, it looks up the target node's gRPC address from its mapping.
It then creates a gRPC client, connects to that address, and issues the appropriate RPC call (such as lookup or replicate) on the target node's gRPC server.
The same pattern applies for other DHT operations (including key transfers during node joins/leaves).

Summary
Dual Role:
Every node runs a gRPC server to handle incoming requests and also acts as a client to forward requests to other nodes as necessary.
Separation of Concerns:
The logical overlay (managed by the Chord DHT using NodeIds) is decoupled from the physical network communication (facilitated via gRPC addresses). This design enables efficient, type-safe, and secure p2p communication.
Code Integration:
Server Side: See ChordGrpcServer in src/network/grpc/server.rs which implements the RPCs.
Client Side: See ChordGrpcClient (not shown in this snippet, but existing in src/network/grpc/client.rs) that is used by the Chord actor to forward requests.
This bidirectional setup using gRPC ensures that our nodes can seamlessly discover each other and handle distributed operations, all while benefiting from gRPC's robust communication framework and the structured routing of a Chord DHT.
Feel free to ask for further clarification or additional details on any part of this architecture!

The key changes made:
Added protocol version checking in connection establishment
Integrated CHORD_PROTOCOL into the swarm configuration
Added protocol verification when establishing connections
Used protocol identifier in routing behavior
This ensures that:
Only nodes speaking the Chord protocol can connect
Protocol versioning is properly handled
Connections are properly tagged with the protocol identifier
The network layer can distinguish Chord traffic from other protocols
The protocol identifier helps in:
Protocol negotiation during connection establishment
Versioning of the Chord protocol
Isolation from other protocols running on the same network
Future protocol upgrades and compatibility checks
This implementation provides a clean separation between different protocols while ensuring that Chord nodes can properly identify and communicate with each other.

### Demo

`pikachu init` can be used to start the nodes! Use the command to start the bootstrap node. Open another terminal instance and then use the same command to add as many nodes as you want. If you close the terminal instance, the nodes will leave the system.

Use `pikachu PUT <KEY> <VALUE>` to store key-value pair in the DHT, and `pikachu GET <KEY>` to retrieve the value.

### Something extra...

This is a very close implementation of the infamous Chord DHT with some twists :

* It's BFT now :)
* Traditional Chord doesn't specify any encryption, leaving communication vulnerable to man-in-the-middle attacks or data tampering. Noise Protocol Framework has been used to tackle this which automatically handles peer authentication, reducing the risk of Sybil attacks
* [mDNS](https://datatracker.ietf.org/doc/html/rfc6762) has been used for local peer discovery

### Reference

Thanks to the authors of these wonderful research papers for inspiring me for this personal project :

* [Chord](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf)
* [Towards Practical Communication in Byzantine-Resistant DHTs](https://www.cs.purdue.edu/homes/akate/publications/RobustP2P.pdf)
* [Making Chord Robust to Byzantine Attacks](https://www.cs.unm.edu/~saia/papers/swarm.pdf)
* [Comparing Performance of DHTs under churn](https://pdos.csail.mit.edu/~strib/docs/dhtcomparison/dhtcomparison-iptps04.pdf)

### Future Work

Some cool things that can be further done :

* Make this BFT DHT privacy-preserving as well - [Add Query Privacy to Robust DHTs](https://arxiv.org/pdf/1107.1072)
That would be cool :)

### Disclaimer 

This project was undertaken to deepen my understanding of decentralized p2p networks after taking the course EE698C at IIT Kanpur.

Not designed to be used in production!
