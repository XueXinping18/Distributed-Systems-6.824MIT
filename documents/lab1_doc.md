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

