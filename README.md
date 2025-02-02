# Pikachu 

A Byzantine Fault Tolerant Distributed Hash Table implementation.

### System Design

```mermaid
flowchart TD
    subgraph "Libp2p Networking"
        A[Peer Object]
        B[PeerId<br/>(Libp2p Identity)]
    end

    subgraph "Chord Overlay (DHT)"
        C[NodeId<br/>(Derived from PeerId)]
        D[Chord Ring]
        E[Finger Tables<br/>& Routing]
        F[Key-Value Storage]
    end

    subgraph "Key Processing"
        G[Key (Arbitrary Data)]
        H[Hashed Key<br/>(NodeId from Key)]
    end

    %% Relationships
    A --> B
    B -- "Hash (SHA256)" --> C
    C -- "Placed on" --> D
    D -- "Utilizes" --> E
    E -- "Routes queries to" --> F

    G --> H
    H -- "Compare with nodes in ring" --> D

    %% Notes
    note right of C
      NodeId for routing \n and placement
      in the DHT overlay
    end note

    note left of B
      PeerId used for \n
      connection management
    end note
```

### Demo

`pikachu init` can be used to start the nodes! Use the command to start the bootstrap node. Open another terminal instance and then use the same command to add as many nodes as you want. If you close the terminal instance, the nodes will leave the system.

Use `pikachu PUT <KEY> <VALUE>` to store key-value pair in the DHT, and `pikachu GET <KEY>` to retrieve the value.

### Something extra...

This is a very close implementation of the infamous Chord DHT with some twists :

* It's BFT now :)
* Traditional Chord doesn’t specify any encryption, leaving communication vulnerable to man-in-the-middle attacks or data tampering. Noise Protocol Framework has been used to tackle this which automatically handles peer authentication, reducing the risk of Sybil attacks
* Instead of TCP/UDP, gRPC has been used

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
