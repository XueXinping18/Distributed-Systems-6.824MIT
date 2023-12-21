## Lab 1: MapReduce Implementation Overview

### Core Components:
- **Map and Reduce Functions**: Implemented as concurrent operations. Multiple map and reduce tasks are processed in parallel, leveraging Go's concurrency primitives.
- **Task Handling**: The `Worker` function orchestrates the workflow, continuously requesting tasks, processing them based on their type (Map or Reduce), and communicating task completion back to the coordinator.

### Concurrency Handling:
- **Worker Registration**: Each worker registers with the coordinator, ensuring a managed and synchronized start for task distribution.
- **Task Distribution and Management**: Workers independently request tasks from the coordinator. The coordinator tracks the status of each task (unscheduled, ongoing, finished) for efficient task management and reassignment in case of failures or timeouts.
- **Data Sharing and Access**: Channels are used for task distribution to avoid race conditions and ensure thread-safe communication between the coordinator and workers.
- **Parallel Task Execution**: Workers execute map and reduce tasks concurrently, significantly improving the throughput and efficiency of the system.
- **File Handling**: Temporary files are used for intermediate data storage during map tasks, with atomic renaming ensuring consistent and safe updates without conflicts.

### Error Handling and Fault Tolerance:
- **Timeout Mechanism**: Tasks are monitored for completion within a specified timeout period. Tasks not completed in time are rescheduled, enhancing the system's resilience to worker failures.
- **Graceful Exit**: Workers are informed to exit once all tasks are completed, ensuring a clean and orderly shutdown of the system.

### Other Design consideration
- **Timeout Thread**: Every time the coordinator assigned a task to a worker, it will update the last assigned time of that task to be the current time and open a thread that sleeps for 10 seconds. After the thread wakes up, it will check if the task is still not finished and not reassigned within 10 seconds. If so, the thread re-adds the task into the channel for reschedule later on and marks the task as unscheduled. The double check of previously assigned time ensures that no task is reassigned only 10 seconds after it is previously assigned.
- **Close Channel after Set Update Stage**: Locks are not used when the stage changes. Instead, it is perfectly fine not to use lock because the open empty channel of previous stage will  block the new request from the workers to be assigned with any new tasks. Only assigned after variables regarding the current stage have been updated. If we change the order, there is a risk of a request for task being served before the stage is updated (maybe due to interrupt), and the task in the new stage is finished and reported to coordinator  while the coordinator still believes it is in the old stage.