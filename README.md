# MIT 6.824 Distributed Systems Labs

This repository contains my solutions for the MIT 6.824 Distributed Systems course labs (Spring 2022). The course is designed to impart a deep understanding of the design, implementation, and intricacies of distributed systems.

## Course Information
- **Course Website:** [MIT 6.824 Distributed Systems Spring 2022](http://nil.csail.mit.edu/6.824/2022/schedule.html)

## Labs Overview

### Lab 1: MapReduce (finished)
- **Description:** Implementing the MapReduce programming model for parallel data processing.
- **Key Concepts:** Parallel computing, large-scale data processing.
- **Similar Industry Middleware:** Hadoop MapReduce, Apache Spark.

### Lab 2: Raft Consensus Algorithm (finished)
- **Subsections:**
  - **Lab 2A: Raft Leader Election** (finished)
    - Implementing leader election within the Raft consensus framework.
  - **Lab 2B: Raft Log Entries Append** (finished)
    - Managing log entries, replication, and maintaining consistency among followers.
  - **Lab 2C: Raft State Persistence** (finished)
    - Ensuring the durability and automatic recoverability of the Raft states in case server encounters fail-stop faults.
  - **Lab 2D: Raft Log Compaction** (finished)
    - Implementing log compaction in Raft to handle growing log sizes and reduce time to recover states on restarts.
- **Similar Industry Middleware:** Apache ZooKeeper, etcd.

### Lab 3: Fault-tolerant Key/Value Service (finished)
- **Subsections:**
  - **Lab 3A: Key/Value Service Without Snapshots** (finished)
    - Building a key/value store on Raft to achieve strong consistency among replicated state machines, featured with mechanisms for duplication detection and handling of stale requests.
  - **Lab 3B: Key/Value Service With Snapshots** (finished)
    - Enhancing the key/value store with snapshot functionality for efficient state management.
- **Similar Industry Middleware:** Redis, Amazon DynamoDB.

### Lab 4: Sharded Key/Value Service (finished)
- **Subsections:**
  - **Part A: The Shard Controller** (finished)
    - Developing a shard controller to manage configurations and shard assignments among replica groups.
  - **Part B: Sharded Key/Value Server** (finished)
    - Creating a sharded, fault-tolerant key/value storage system, ensuring linearizable client interaction even amidst configuration changes.
  - **Additional Challenges:** (finished)
    - Implementing garbage collection of state and managing client requests during configuration transitions.
- **Similar Industry Middleware:** MongoDB Sharded Clusters, Google Cloud Bigtable.

## How to Run

Please refer to the official course website (http://nil.csail.mit.edu/6.824/2022/schedule.html)
Logging by default is turned off. For detailed logging related to clients, services or Raft instances, consider to switch the corresponding Debug variable to true.

## Implementation Details 

Lab 1 has been provided documentation. Lab 2, 3, and 4 has been finished and documentation are still unfinished.
Also, substantial comments are supplied in my code to explain how different scenarios are handled in my design, and why alternative designs might be problematic, etc.

## Skills and Concepts Acquired

- Comprehensive understanding of distributed systems principles and their practical applications.
- Mastery in how strong consensus, fault tolerance, performance are achieved and traded off with each other in Raft.
- Hands-on experience in designing scalable systems akin to industry-standard distributed systems.
- Development expertise in Go, especially for programming in concurrency.


