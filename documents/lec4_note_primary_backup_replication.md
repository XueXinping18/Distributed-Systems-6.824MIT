# Lecture 4: Primary/Backup Replication

## Failures Addressed by Replication

### "Fail-stop" Failure of a Single Replica

- **Causes**: Fan malfunction, CPU overheating, power or network disconnection, software running out of disk space. Generally, these are hardware problems.

### Limitations in Handling Failures

- Ineffective against hardware defects, software bugs, or human configuration errors.
- Failures often not "fail-stop", particularly in the case of software bugs, reducing the effectiveness of replication.
- Failures can be correlated, potentially causing simultaneous crashes across replicas.
- Some failures are detectable, for instance, through checksums.

### Handling Large-Scale Disasters

- Effectiveness against disasters like earthquakes or city-wide power failures depends on the geographical separation of replicas.

## Challenges in Primary/Backup Replication

### Determining Primary Failure

- Distinguishing between actual failure and network partition.
- Network partitions treated as failures can lead to a "split-brain" situation, which is resolved by applying the "majority rule" principle as in different protocols.

### Synchronizing Primary and Backup

- Implementing changes in a sequential order and managing non-determinism.

### Failover Process

- Identifying which backup should take over based on which has the most recent state.

## Main Replication Approaches

### State Transfer

- The primary replica executes the service and sends the new state to backups, possibly using periodic checkpoints.
- Pros: Simpler in design.
- Cons: Transfer can be slow if the state is large.

### Replicated State Machine

- Clients send operations to the primary, which then sequences these operations and sends them to backups.
- All replicas execute all operations in the same sequence.
- Ensures identical end states if the start states, operations, and their order are identical and deterministic.
- Pros: Generally generates less network traffic.
- Cons: More complex to implement correctly.

### Level of Operations to Replicate

#### Application-Level Operations

- Involves the application in the fault tolerance scheme, making it non-transparent at the current application level.
- Better performance as less performace cost from output rule (onhold sending output to client until receiving acknowledgement from backup).

#### Machine-Level Operations (Instruction-Level)

- Completely transparent to the application; the application is not modified.
- VMware FT utilizes virtual machines for machine-level replication, appearing as a single machine to clients.

### Handling Deterministic Operations in Fault Tolerance

- Both primary and backup hold identical binary codes of Linux and the application (i.e., identical initial states).
- Deterministic operations are executed independently by the backup based on its code.
- The backup executes codes only after receiving information from the primary about the exact location of the next interrupt, which causes lagging. Necessary to make the state machines completely identical.

### Non-Determinism at Instruction Level

#### Sources

- Non-deterministic operations (e.g., system calls involving time).
- Timing of interrupts.
- Multi-core operations on the same memory location.

#### Solutions

- Hypervisor pre-examines and alters non-deterministic operations in the Linux binary to trigger traps into the hypervisor when executed.
- Interrupts are also trapped into the hypervisor, which then sends logs to the backup.
- Use of multi-core machines is restricted. (not applicable to multi-core)

### Output Rule in Fault Tolerance

- The primary sends response acknowledgments to clients only after receiving acknowledgment from the backup.
- This ensures state consistency but at the cost of performance.

### Role of the Arbitrate Server

- Acts as the persistent storage for the primary and backup (akin to a disk).
- Decides the next primary in the event of a failure using an atomic test-and-set operation.
- The connection between the hypervisors and this server is crucial, making it a single point of failure. No replication on arbitrate server.

### Handling Network Partition vs. Fail-Stop

- Both scenarios the alive hypervisor believes the other side fails and send request for being new primary.
- Hypervisors send a test-and-set operation to the arbitrate server to determine eligibility as the new primary.
- The first hypervisor to succeed becomes the new primary. The second fails and terminates itself.

### Repair Process (manuel repair requires, not automatic)

- In the event of a "0" response (indicating failed attempt to become new primary), the hypervisor terminates itself for manual repairs. Failed machine also terminates itself.
- Repair is manual to keep the lagged terminated machine in sync with the running primary. No client service is provided during repairs.
- After repairs, the backup rejoins the system, and the repair program notifies the arbitrate server to update its status, allowing the appointment of new primary.

### Limitations and Tradeoffs

- Consist of a single point of failure: the arbitrade server. No replication on that server.
- The replica of virtual machine, for strong consistency, does not server the client requests at all which might be useful in  increasing the read performance. The replication is only for the purpose of fault tolerance without the effect of increasing read throughput.
- Potential duplicated reply from both virtual machine to the client. If the primary fails right after it replies to the client and the failover happens, the new primary will still have to send the reply to the client because it does not know whether the old primary has sent it or not. This is not problematic as duplicated replies are normal in the scenario of TCP. Given the identical state between the old primary and new primary, the sequence number of the duplicated replies will be the same. TCP can handle such duplicates.