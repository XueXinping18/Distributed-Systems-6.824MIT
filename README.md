# MIT 6.824 Distributed Systems Labs

This repository contains my solutions for the MIT 6.824 Distributed Systems course labs (Spring 2022). The course is designed to impart a deep understanding of the design, implementation, and intricacies of distributed systems.

## Course Information
- **Course Website:** [MIT 6.824 Distributed Systems Spring 2022](http://nil.csail.mit.edu/6.824/2022/schedule.html)

## Labs Overview

### Lab 1: MapReduce
- **Description:** Implementing the MapReduce programming model for parallel data processing.
- **Key Concepts:** Parallel computing, large-scale data processing.
- **Similar Industry Middleware:** Hadoop MapReduce, Apache Spark.

### Lab 2: Raft Consensus Algorithm
- **Subsections:**
  - **Lab 2A: Raft Leader Election**
    - Implementing leader election within the Raft consensus framework.
  - **Lab 2B: Raft Log Entries Append**
    - Managing log entries, replication, and maintaining consistency among followers.
  - **Lab 2C: Raft State Persistence**
    - Ensuring the durability and reliability of the Raft state across restarts.
  - **Lab 2D: Raft Log Compaction**
    - Implementing log compaction in Raft to handle growing log sizes and improve efficiency.
- **Similar Industry Middleware:** Apache ZooKeeper, etcd.

### Lab 3: Fault-tolerant Key/Value Service
- **Subsections:**
  - **Lab 3A: Key/Value Service Without Snapshots**
    - Building a key/value store based on Raft without the use of snapshots for state persistence.
  - **Lab 3B: Key/Value Service With Snapshots**
    - Enhancing the key/value store with snapshot functionality for efficient state management.
- **Similar Industry Middleware:** Redis, Amazon DynamoDB.

### Lab 4: Sharded Key/Value Service
- **Subsections:**
  - **Part A: The Shard Controller**
    - Developing a shard controller to manage configurations and shard assignments among replica groups.
  - **Part B: Sharded Key/Value Server**
    - Creating a sharded, fault-tolerant key/value storage system, ensuring linearizable client interaction even amidst configuration changes.
  - **Additional Challenges:**
    - Implementing garbage collection of state and managing client requests during configuration transitions.
- **Similar Industry Middleware:** MongoDB Sharded Clusters, Google Cloud Bigtable.

## How to Run

Please refer to the official course website (http://nil.csail.mit.edu/6.824/2022/schedule.html)

## Implementation Details 

Each section has its own documentation for implementation. Please refer to them under documents directory if interested.

## Skills and Concepts Acquired

- Comprehensive understanding of distributed systems principles and their practical applications.
- Hands-on experience with Raft consensus, fault tolerance, sharding, and state management.
- Proficiency in designing scalable systems akin to industry-standard distributed systems.
- Development expertise in Go, especially for programming in concurrency.


