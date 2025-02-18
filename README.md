# Pikachu 

An asynchronous multi-dimensional non-persistent in-memory Byzantine Fault Tolerant Distributed Hash Table implementation.

### Demo

To start the bootstrap node, run `cargo run start-bootstrap -p <BOOTSTRAP_NODE_PORT>`. Check whether the bootstrap node is working properly or not by curling the gRPC server of the bootstrap node `lsof -i :<NODE_PORT>`. Then join this bootstrap node to make a chord network `cargo run join -b 8001 -p <NODE_PORT>`. The nodes will automatically detect each other, and start forming the chord network. Take a look at `<NODE_PORT>.log` files for the logs. 

To put the key-value pair in the DHT use `cargo run -p <NODE_PORT> -k <KEY> -v <VALUE>` and similarly to get it back from DHT use `cargo run get -p <NODE_PORT> -k <KEY>`.

Just `^C` for graceful shutdown of node, and then close the terminal instance.

And when you only spawn 2/3 nodes, you will see a lot of failing routing table updates. Chord requires a sufficient number of nodes, spread across the ID space, to populate the finger tables effectively. With only one node, most `find_successor` calls will fail because there's no other node to point to. With two nodes, you'll have some entries, but many will still be missing. Don't worry :)

And yes, aren't there too many logs when you use `RUST_LOG=debug` with the command!

### System Design

Its not crash fault tolerant.

The length of the successor/predecessor lists should be typically r=O(logN) but I have kept it a constant 3 because in a local setup, I am not going to spawn that many nodes!

gRPC handoff -> rx_grpc -> forward_task -> tx_process -> rx_process -> storage

1. Recursive Lookup
How It Works

    The querying node sends the request to the closest preceding node (according to the finger table).
    That node processes the request and forwards it to the next closest node.
    This process continues until the responsible node is found, which then returns the value.

Advantages

✅ Lower latency in ideal conditions:

    Since nodes forward the request directly, the lookup can be faster in a low-latency network.
    The request follows a single path through the network, reducing the number of back-and-forth messages.

✅ Less burden on the querying node:

    The node initiating the request does not need to track intermediate responses.
    The lookup is handled by the network itself.

Disadvantages

❌ Higher risk of failure propagation:

    If a node along the lookup path fails, the query may be lost.
    There is no way for the initiating node to retry from the last known node unless redundancy is built in.

❌ Potentially higher network congestion:

    If nodes are overloaded, they might become bottlenecks when processing multiple lookup requests.

### Something extra...

This is a very close implementation of the infamous Chord DHT with a twist, I have made it BFT using SMPC protocol.

### Reference

Thanks to the authors of these wonderful research papers for inspiring me for this personal project :

* [Chord](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf)
* [How to make Chord correct](https://arxiv.org/pdf/1502.06461)
* [A Statistical Theory of Chord under Churn](https://arxiv.org/pdf/cs/0501069)
* [Atomic Data Access in DHTs](https://groups.csail.mit.edu/tds/papers/Lynch/lncs02.pdf)
* [Making Chord Robust to Byzantine Attacks](https://www.cs.unm.edu/~saia/papers/swarm.pdf)
* [Towards Practical Communication in Byzantine-Resistant DHTs](https://www.cs.purdue.edu/homes/akate/publications/RobustP2P.pdf)
* [Building p2p systems with Chord, a Distributed Lookup Service](https://www.cs.princeton.edu/courses/archive/spr05/cos598E/bib/dabek-chord.pdf)
* [Comparing Performance of DHTs under churn](https://pdos.csail.mit.edu/~strib/docs/dhtcomparison/dhtcomparison-iptps04.pdf)
* [Design and Analysis in Structures p2p systems](https://dcatkth.github.io/thesis/sameh_thesis.pdf)

### Some Improvements...

Some cool things that can be further done :

* Enable TLS in gRPC communication for better security
* Make this BFT DHT privacy-preserving as well - [Add Query Privacy to Robust DHTs](https://arxiv.org/pdf/1107.1072)
That would be cool :)
* Sybil attacks could poison the network as nodes can join without authentication
* Adding some testcontainer-based property tests would be cool!

### Disclaimer 

This project was undertaken to deepen my understanding of decentralized p2p networks after taking the course EE698C at IIT Kanpur.

Not designed to be used in production!
