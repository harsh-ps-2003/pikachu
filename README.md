# Pikachu 

An asynchronous multi-dimensional in-memory non-persistent Distributed Hash Table implementation.

### Demo

To start the bootstrap node, run `cargo run start-bootstrap -p <BOOTSTRAP_NODE_PORT>`. If the port `-p` is omitted, a random available port might be used. Check whether the bootstrap node is working properly or not by curling the gRPC server of the bootstrap node `lsof -i :<NODE_PORT>`.

Then join this bootstrap node to make a chord network `cargo run join -b <BOOTSTRAP_NODE_PORT> -p <NODE_PORT>`. Similar to starting, if the local port `-p` is omitted, a random one might be assigned. The nodes will automatically detect each other, and start forming the chord network.

Take a look at `<NODE_PORT>.log` files for the logs. You can control the log verbosity using the `RUST_LOG` environment variable (e.g., `RUST_LOG=debug cargo run ...` for detailed logs, defaults to `info`).

To put the key-value pair in the DHT use `cargo run put -p <NODE_PORT> -k <KEY> -v <VALUE>`. This connects to the node running on `127.0.0.1:<NODE_PORT>`.
Similarly to get it back from DHT use `cargo run get -p <NODE_PORT> -k <KEY>`.

Just `^C` for graceful shutdown of node, and then close the terminal instance.

And when you only spawn 2/3 nodes, you will see a lot of failing routing table updates. Chord requires a sufficient number of nodes, spread across the ID space, to populate the finger tables effectively. With only one node, most `find_successor` calls will fail because there's no other node to point to. With two nodes, you'll have some entries, but many will still be missing. Don't worry :)

And yes, aren't there too many logs when you use `RUST_LOG=debug` with the command!

### System Design

> [!WARNING]
> This implementation is **not crash fault tolerant**. Node failures can lead to data loss or network instability.

#### Successor/Predecessor List Size

The Chord protocol typically recommends successor/predecessor list sizes (`r`) on the order of O(log N), where N is the number of nodes. For simplicity in this local development setup, `r` is fixed to a small constant (e.g., 3), as a large number of nodes is not anticipated.

#### Lookup Mechanism

This implementation uses **Recursive Lookups** for finding the node responsible for a given key.

**How it Works:**

1.  **Initiation:** The querying node identifies the closest preceding node to the target key ID in its finger table.
2.  **Forwarding:** It sends the lookup request to that node.
3.  **Processing:** The receiving node checks if *it* is the successor. If not, it finds the closest preceding node in *its* finger table and forwards the request again.
4.  **Repetition:** This forwarding continues hop-by-hop closer to the target ID.
5.  **Resolution:** The node ultimately responsible for the key identifies itself and returns the resultback to the node that called it.
6.  **Return Path:** Each node in the chain then receives this response and forwards it back up the call stack to the node that originally requested it from them. The response effectively retraces the request path in reverse.

To understand in the architecture in detail, [read all the chapters](/architecture/). 

<!-- ### Something extra...

This is a very close implementation of the infamous Chord DHT with a twist, I have made it BFT using SMPC protocol. -->

### Things to Further think about...

* Instead of hopping the response back, we should communicate directly. Using the DHT primarily to find the responsible node's address and then communicating directly is often more efficient than having the response hop back through the entire chain.
* Need a careful review to ensure correctness under various conditions (e.g., joining an empty vs. populated network, concurrent joins).
* Ensuring that key transfers during joins/stabilization are atomic or at least robust against failures is crucial for data consistency. If any node along the forwarding path fails, the lookup request can be lost. Without built-in retry mechanisms at each hop or by the originator, the request fails.
* Intermediate nodes handle both their own load and forwarded requests, potentially creating bottlenecks.

### Reference

Thanks to the authors of these wonderful research papers for inspiring me for this personal project :

* [Chord](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf)
* [How to make Chord correct](https://arxiv.org/pdf/1502.06461)
* [A Statistical Theory of Chord under Churn](https://arxiv.org/pdf/cs/0501069)
* [Atomic Data Access in DHTs](https://groups.csail.mit.edu/tds/papers/Lynch/lncs02.pdf)
* [Making Chord Robust to Byzantine Attacks](https://www.cs.unm.edu/~saia/papers/swarm.pdf)
* [Towards Practical Communication in Byzantine-Resistant DHTs](https://www.cs.purdue.edu/homes/akate/publications/RobustP2P.pdf)
* [A survey of DHT security](https://dl.acm.org/doi/pdf/10.1145/1883612.1883615)
* [Building p2p systems with Chord, a Distributed Lookup Service](https://www.cs.princeton.edu/courses/archive/spr05/cos598E/bib/dabek-chord.pdf)
* [Comparing Performance of DHTs under churn](https://pdos.csail.mit.edu/~strib/docs/dhtcomparison/dhtcomparison-iptps04.pdf)
* [Design and Analysis in Structures p2p systems](https://dcatkth.github.io/thesis/sameh_thesis.pdf)

### Some Improvements...

Some cool things that can be further done :

* Make this CFT via proper replication factor
* Enable TLS in gRPC communication for better security
* Make this BFT
* Make this DHT privacy-preserving as well - [Add Query Privacy to Robust DHTs](https://arxiv.org/pdf/1107.1072)
That would be cool :)
* Sybil attacks could poison the network as nodes can join without authentication
* Adding some property tests would be cool!

### Disclaimer 

This project was undertaken to deepen my understanding of Decentralized P2P networks after taking the course EE698C at IIT Kanpur.

Not designed to be used in production!
