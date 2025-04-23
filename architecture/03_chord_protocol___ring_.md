# Chapter 3: Chord Protocol & Ring

Welcome back! In [Chapter 2: Node](02_node_.md), we learned about the individual computers, or **Nodes**, that make up our `pikachu` network. We saw that each node stores some data, listens for requests, and talks to other nodes.

But how do these nodes organize themselves? If you ask one node to store (`put`) some data like `("pokemon_name", "Squirtle")`, how does it figure out *which* node in the whole network should actually keep that piece of information? Similarly, when you ask to retrieve (`get`) "pokemon_name", how does the node you contact find the right node that holds "Squirtle"?

Asking every single node would be incredibly slow and inefficient, especially in a network with thousands or millions of nodes! We need a clever system for organization and finding things quickly.

## The Problem: Organizing Nodes and Data

Imagine you have a group of librarians ([Node](02_node_.md)s) and a huge collection of books (key-value data). You need a system so that:

1.  **Clear Responsibility:** Every book has a *specific* librarian responsible for it.
2.  **Efficient Finding:** When someone wants a book, any librarian can quickly figure out which librarian is responsible for that book without having to ask everyone else.

This system needs to work even as new librarians join or existing ones leave.

## The Solution: The Chord Protocol and the Ring

`pikachu` uses a specific set of rules called the **Chord protocol** to solve this organization problem. The core idea of Chord is to arrange all the nodes in a logical **circle**, like numbers on a clock face or people sitting around a giant round table. This is often called the **Chord Ring**.

**How it Works:**

1.  **IDs are Everything:** Remember how both data keys (like `"pokemon_name"`) and nodes themselves get unique IDs from a hash function? (Covered in [Chapter 1: Distributed Hash Table (DHT)](01_distributed_hash_table__dht__.md) and [Chapter 2: Node](02_node_.md)). These IDs are usually large numbers within a fixed range (e.g., from 0 up to 2<sup>160</sup>-1 for a 160-bit hash).
2.  **Placing on the Ring:** Imagine this range of numbers arranged in a circle. Each node is placed on the circle according to its **Node ID**. Each data key is also conceptually placed on this circle according to its **hashed Key ID**.
3.  **Ordering:** Nodes are ordered around the ring based on their IDs, increasing in the clockwise direction.

```mermaid
graph TD
    subgraph Chord Ring (Conceptual Circle)
        direction CLOCKWISE
        N10(Node ID 10) --> N35(Node ID 35)
        N35 --> N60(Node ID 60)
        N60 --> N90(Node ID 90)
        N90 --> N10 -- Wrap Around --> N10
    end

    style N10 fill:#f9d,stroke:#333,stroke-width:2px
    style N35 fill:#ccf,stroke:#333,stroke-width:2px
    style N60 fill:#dfd,stroke:#333,stroke-width:2px
    style N90 fill:#fec,stroke:#333,stroke-width:2px
```

In this simple example (using small IDs 0-99 for clarity), we have four nodes with IDs 10, 35, 60, and 90 arranged clockwise on the ring.

## Who Stores What? The Successor Rule

Now that nodes are arranged on the ring, Chord defines a simple rule to determine which node is responsible for which data key:

> A data key (identified by its ID) is stored on the **first node** encountered when moving **clockwise** around the ring starting from the key's ID position. This node is called the key's **successor**.

Let's refine this: A node is responsible for all the key IDs that fall between its direct counter-clockwise neighbor (its *predecessor*) and itself.

**Analogy: Assigned Seats at the Round Table**

Imagine people sitting at a round table with numbered seats (0-99).
*   Alice sits at seat 10.
*   Bob sits at seat 35.
*   Charlie sits at seat 60.
*   David sits at seat 90.

If someone brings item #25, who takes care of it? Moving clockwise from 25, the first person we meet is Bob at 35. So, Bob stores item #25.
If item #70 arrives? Clockwise from 70, we first meet David at 90. David stores item #70.
If item #5 arrives? Clockwise from 5, we first meet Alice at 10. Alice stores item #5.
If item #95 arrives? Clockwise from 95, we wrap around the circle and first meet Alice at 10. Alice stores item #95.

So, based on the successor rule:
*   Node 10 is responsible for key IDs (91-99 and 0-10).
*   Node 35 is responsible for key IDs (11-35).
*   Node 60 is responsible for key IDs (36-60).
*   Node 90 is responsible for key IDs (61-90).

```mermaid
graph TD
    subgraph Chord Ring (with Data Responsibility)
        direction CLOCKWISE
        N10(Node ID 10<br>Stores Keys 91-10) --> N35(Node ID 35<br>Stores Keys 11-35)
        N35 --> N60(Node ID 60<br>Stores Keys 36-60)
        N60 --> N90(Node ID 90<br>Stores Keys 61-90)
        N90 --> N10 -- Wrap Around --> N10

        subgraph Data Keys
          K5[Key ID 5] --> N10
          K25[Key ID 25] --> N35
          K70[Key ID 70] --> N90
          K95[Key ID 95] --> N10
        end
    end

    style N10 fill:#f9d,stroke:#333,stroke-width:2px
    style N35 fill:#ccf,stroke:#333,stroke-width:2px
    style N60 fill:#dfd,stroke:#333,stroke-width:2px
    style N90 fill:#fec,stroke:#333,stroke-width:2px
    style K5 stroke:#333,stroke-dasharray: 5 5
    style K25 stroke:#333,stroke-dasharray: 5 5
    style K70 stroke:#333,stroke-dasharray: 5 5
    style K95 stroke:#333,stroke-dasharray: 5 5

```

## Why is the Ring Useful?

This ring structure and the successor rule provide a clear way to assign responsibility. Crucially, it also gives nodes a way to *find* the responsible node.

If Node 10 receives a request to `get` data with Key ID 70, it knows:
1.  Key ID 70 is greater than its own ID (10).
2.  It's not responsible for Key ID 70 (it only handles up to 10 and things wrapping around from 91).
3.  The responsible node must be *further along* the ring in the clockwise direction.

Node 10 doesn't necessarily know immediately that Node 90 is responsible, but it knows the *direction* to search. It can ask its immediate neighbor (Node 35) or use clever shortcuts (which we'll see in [Chapter 4: Routing & Data Location (Finger Table / Successor List)](04_routing___data_location__finger_table___successor_list__.md)) to quickly get closer to the target ID 70.

## Using `pikachu` with the Chord Ring

You don't interact with the ring directly. You still just talk to any node using `put` and `get`:

```bash
# Ask Node 10 (running on port 8001) to store ("color", "blue")
# Assume hash("color") results in Key ID 25
cargo run put -p 8001 -k "color" -v "blue"

# Ask Node 60 (running on port 8003) to retrieve "color"
# Assume hash("color") is still Key ID 25
cargo run get -p 8003 -k "color"
# Expected Output: Value: "blue"
```

**What happens "under the hood" for the `put` command:**

1.  Your command contacts Node 10 (at port 8001).
2.  Node 10 hashes the key `"color"` to get its ID (e.g., 25).
3.  Node 10 looks at the Chord ring structure it knows about. It determines that Key ID 25 should be stored on its successor, which is Node 35 (at port 8002, perhaps).
4.  Node 10 sends a message to Node 35, telling it to store the key-value pair `("color", "blue")`.
5.  Node 35 receives the message and stores the data locally.
6.  Node 10 might receive confirmation and signals success back to your command line.

**What happens "under the hood" for the `get` command:**

1.  Your command contacts Node 60 (at port 8003).
2.  Node 60 hashes `"color"` to get Key ID 25.
3.  Node 60 checks its view of the Chord ring. It knows it's responsible for keys 36-60, so it doesn't have Key ID 25.
4.  It needs to find the node responsible for 25 (the successor of 25). It uses its routing information (which we'll explore in [Chapter 4](04_routing___data_location__finger_table___successor_list__.md) and [Chapter 5](05_recursive_lookup_.md)) to figure out how to reach Node 35. This might involve asking other nodes.
5.  Eventually, the request reaches Node 35.
6.  Node 35 finds `"color"` in its local storage and sends the value `"blue"` back along the path the request came from.
7.  Node 60 receives the value and sends it back to your command line.

The key takeaway is that the Chord ring provides the map, and the Chord protocol provides the rules for navigating that map to find the correct destination for data storage and retrieval.

## Maintaining the Ring: Successors

The most fundamental piece of information each node needs for Chord to work is knowing its immediate clockwise neighbor: its **successor**.

```mermaid
graph TD
    subgraph Simple Successor Links
        direction CLOCKWISE
        N10(Node 10) -- Successor --> N35(Node 35)
        N35 -- Successor --> N60(Node 60)
        N60 -- Successor --> N90(Node 90)
        N90 -- Successor --> N10
    end
```

Nodes constantly check if their successor is still running and update this pointer if needed. When new nodes join or existing ones leave, these successor pointers (and other related pointers like predecessors) are updated through a process called **stabilization**, which we'll cover in [Chapter 6: Network Joining & Stabilization](06_network_joining___stabilization__.md). This keeps the ring structure intact even as the network changes.

## Conclusion

You've learned about the **Chord protocol** and its central organizing principle: the **Chord Ring**. This logical ring structure, based on hashed IDs, provides a consistent way to:

*   Arrange nodes in an ordered circle.
*   Assign responsibility for data keys to specific nodes (using the **successor** rule).
*   Provide a foundation for efficiently finding where data is stored.

Think of the Chord Ring as the map of the `pikachu` DHT world. Knowing this map structure is essential. But just knowing your immediate neighbor (successor) isn't always the fastest way to get across town (especially in a very large ring). How do nodes take shortcuts to find data much faster? That's where more advanced routing information comes in.

**Next:** [Chapter 4: Routing & Data Location (Finger Table / Successor List)](04_routing___data_location__finger_table___successor_list__.md)
