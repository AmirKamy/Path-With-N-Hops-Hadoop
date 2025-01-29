# Path with N Hop

## Problem Description

Download the graph dataset of Google Web from the following link:  
[Web-Google Dataset](http://snap.stanford.edu/data/web-Google.html)

Each row in the dataset contains two numbers indicating the connection between two web pages. The first page links to the second. These page numbers represent nodes, and the relationships between the pages represent edges.

You need to implement a program that identifies all nodes that are reachable from each node in the dataset within `n` steps. The value of `n` can be any number.

### Example:  
#### Dataset content:
A B

A C

B C

B D

C E

D F

E D

For `n = 3`, you need to determine all nodes reachable from each node within three steps. For example:

- A -> C -> E -> D
- A -> B -> C -> E
- A -> B -> D -> F
- B -> C -> E -> D
- C -> E -> D -> F

#### Output:
A -> D E F

B -> D

C -> F



## Solution Explanation

### **Phase 1: Graph Construction**

First, we create a structure where for each node, we list all its neighbors.

#### Mapper:
The `Mapper` here processes the input to create edges. For example, if a line in the input says "B -> A", the `Mapper` will output "A" as the key and "B" as the value. This allows the graph's edges to be stored as key-value pairs, making it easier to process in the next phase.

#### Reducer:
The `Reducer` aggregates all values (neighbors) associated with a given node and creates a list of neighbors for that node. For instance, if node A is connected to nodes B and C, the output will be `A -> B,C`. If there are duplicate neighbors, they are removed by using a `Set`. This results in a clean list of neighbors for each node, which is then used in the next phase.

### **Phase 2: BFS Execution**

In the second phase, we perform BFS to compute the reachable nodes within `n` steps.

#### Mapper:
In this phase, the `Mapper` simply forwards the data from the previous phase without any changes. It prepares the data for the reducer by keeping the graph structure intact.

#### Reducer:
The `Reducer` plays the main role in this phase. It first extracts the list of neighbors of the current node and processes any BFS messages. If a node has reached `n` hops, it writes the result to the output. Otherwise, it forwards the BFS messages to its neighbors to be processed in the next iteration.

At each iteration, the BFS explores new paths and maintains the graph structure for future iterations. The final output will list all nodes that can be reached within `n` hops from each node.

## Final Output Format

The final output is written in the following format:
Node -> reachable nodes within N steps
Where `N` is the number of hops defined in the problem (in the example, `N = 3`).

---

This project leverages MapReduce for distributed processing, which makes it suitable for handling large graphs efficiently across a cluster of machines.


## Contributors ✨
- [Mohammad Matin EbrahimiToodeshki](https://github.com/MohammadMatin-EbrahimiToodeshki)
- [Amir Hossein Kamranpour](https://github.com/AmirKamy)
- [Amir Hossein Moayedinia](https://github.com/AmirHMN)

Want to contribute? Feel free to open an issue or a pull request! 🚀
