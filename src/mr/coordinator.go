package mr

import (
	"log"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type TaskType int
type TaskState int

const (
	MAP TaskType = iota
	REDUCE
	EXIT
)
const (
	UNSCHEDULED TaskState = iota
	ONGOING
	FINISHED
)
const (
	TIMEOUT time.Duration = 10 * time.Second
)

// for logging the stages and TaskObj types
func (t TaskType) String() string {
	switch t {
	case 0:
		return "MAP"
	case 1:
		return "REDUCE"
	case 2:
		return "EXIT"
	}
	return "UNKNOWN"
}

type Coordinator struct {
	currentStage      TaskType // Golang does not support volatile or Atomic integer type, so use lock
	mapTaskTracker    TaskTracker
	reduceTaskTracker TaskTracker
	workerTracker     WorkerTracker
	done              atomic.Bool
	mu                sync.Mutex // only for currentStage
}

type WorkerTracker struct {
	nWorker int
	mu      sync.Mutex
	// exited and nExitedWorker might have race conditions
	exited        []bool
	nExitedWorker int
}
type TaskTracker struct {
	taskType       TaskType
	nTask          int
	tasks          []Task
	taskToSchedule chan int
	// taskStatus and nFinishedTask might have race conditions
	mu            sync.Mutex
	taskStatus    []TaskStatus
	nFinishedTask int
}
type TaskStatus struct {
	taskId    int
	state     TaskState // UNSCHEDULED | ONGOING | FINISHED
	startTime time.Time
	workerId  int
}

type Task struct {
	Type         TaskType // MAP | REDUCE | EXIT
	TaskId       int
	Filename     string // only used by MAP type
	NumOfBuckets int    // only used by MAP type for hashing
	NumOfFiles   int    // only used by REDUCE type for iterating
}

type WorkerInfo struct {
	WorkerId int
}

// RPC handlers for the worker to call.

// register the worker to allocate a worker id and increment the number of worker
func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	defer c.workerTracker.mu.Unlock()
	c.workerTracker.mu.Lock()
	reply.WorkerId = c.workerTracker.nWorker
	c.workerTracker.nWorker++
	c.workerTracker.exited = append(c.workerTracker.exited, false)
	log.Printf("Worker id %d assigned.\n", reply.WorkerId)
	return nil
}
func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	// attempt to read from Map Channel, no race condition using channel
	taskId, ok := <-c.mapTaskTracker.taskToSchedule
	if ok {
		mapTask := c.mapTaskTracker.tasks[taskId]
		// assign the TaskObj by reference. Go will send the object instead of pointer in RPC
		reply.TaskObj = &mapTask
		// update TaskObj status atomically
		updateTaskStatusPostAssignment(&c.mapTaskTracker, args.WorkerId, taskId)
		log.Printf("Map task %d scheduled to worker %d!\n", taskId, args.WorkerId)
		return nil
	}
	// map channel is closed, attempt to read from Reduce channel
	taskId, ok = <-c.reduceTaskTracker.taskToSchedule
	if ok {
		reduceTask := c.reduceTaskTracker.tasks[taskId]
		// assign the TaskObj by reference. Go will send the object instead of pointer in RPC
		reply.TaskObj = &reduceTask
		// update TaskObj status atomically
		updateTaskStatusPostAssignment(&c.reduceTaskTracker, args.WorkerId, taskId)
		log.Printf("Reduce task %d scheduled to worker %d!\n", taskId, args.WorkerId)
		return nil
	}
	// Both channels closed, the job is done, exit
	reply.TaskObj = &Task{Type: EXIT}
	log.Printf("Inform worker %d to exit!\n", args.WorkerId)
	return nil
}

// finish a TaskObj, update the status
func (c *Coordinator) TaskFinished(args *FinishedArgs, reply *FinishedReply) error {
	c.mu.Lock()
	currentStage := c.currentStage
	c.mu.Unlock()
	if args.Type != currentStage {
		log.Fatalf("Mismatch between current stage "+currentStage.String()+
			" and the TaskObj type "+args.Type.String()+" reported by worker %d\n", args.WorkerId)
		return nil
	}
	switch args.Type {
	case MAP:
		finished := updateTaskStatusPostFinish(&c.mapTaskTracker, args.TaskId, args.WorkerId)
		if finished {
			// Set stage first, close the channel next because longer blocking does no harm while start to read
			// new channel before the stage flags are set might be problematic
			c.mu.Lock()
			c.currentStage = REDUCE
			c.mu.Unlock()
			close(c.mapTaskTracker.taskToSchedule)
			log.Println("The MAP stage has been completed. Next: REDUCE")
		}
	case REDUCE:
		finished := updateTaskStatusPostFinish(&c.reduceTaskTracker, args.TaskId, args.WorkerId)
		if finished {
			// Set stage first, close the channel next because longer blocking does no harm
			c.mu.Lock()
			c.currentStage = EXIT
			c.mu.Unlock()
			close(c.reduceTaskTracker.taskToSchedule)
			log.Println("The REDUCE stage has been completed. Next: EXIT")
			// flag done to suggest the channels are closed without having to block to query
			c.done.Store(true)
		}
	default:
		log.Fatalf("Bad TaskObj type reported being finished: " + args.Type.String() + "\n")
		return nil
	}
	return nil
}

// update the TaskObj status atomically after a TaskObj is finished, return true if the stage is also finished
func updateTaskStatusPostFinish(taskTracker *TaskTracker, taskId int, workerId int) bool {
	defer taskTracker.mu.Unlock()
	taskTracker.mu.Lock()
	taskStatus := &taskTracker.taskStatus[taskId]
	if taskStatus.state == FINISHED {
		log.Printf("Task %d of stage "+taskTracker.taskType.String()+
			" finished again and ignored by coordinator", taskId)
		return false
	}
	taskTracker.nFinishedTask++
	taskStatus.state = FINISHED
	taskStatus.workerId = workerId
	if taskTracker.nFinishedTask == taskTracker.nTask {
		return true
	}
	return false
}

// atomically update the TaskObj status after the TaskObj is assigned to a worker
func updateTaskStatusPostAssignment(taskTracker *TaskTracker, workerId int, taskId int) {
	defer taskTracker.mu.Unlock()
	taskTracker.mu.Lock()
	taskStatus := &taskTracker.taskStatus[taskId]
	taskStatus.startTime = time.Now()
	taskStatus.state = ONGOING
	taskStatus.workerId = workerId
	go taskTracker.timeoutToReschedule(taskId, TIMEOUT)
}

// Wait some time and reschedule if not finished or rescheduled
func (tracker *TaskTracker) timeoutToReschedule(taskId int, timeout time.Duration) {
	time.Sleep(timeout)
	defer tracker.mu.Unlock()
	tracker.mu.Lock()
	now := time.Now()
	timeoutTime := tracker.taskStatus[taskId].startTime.Add(timeout)
	// reschedule only if the state after 10 sec is still ONGOING
	// neither finished nor already rescheduled
	// also the last time the TaskObj assigned must before the current time minus 10 sec
	if tracker.taskStatus[taskId].state == ONGOING &&
		(now.After(timeoutTime) || now.Equal(timeoutTime)) {
		tracker.taskToSchedule <- taskId
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("A new goroutine is started to listen worker requests!")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.done.Load()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// instantiate and initialize the coordinator
	c := Coordinator{
		currentStage: MAP,
		mapTaskTracker: TaskTracker{
			taskType:       MAP,
			nTask:          len(files),
			tasks:          make([]Task, len(files)),
			taskToSchedule: make(chan int),
			taskStatus:     make([]TaskStatus, len(files)),
			nFinishedTask:  0,
		},
		reduceTaskTracker: TaskTracker{
			taskType:       REDUCE,
			nTask:          nReduce,
			tasks:          make([]Task, nReduce),
			taskToSchedule: make(chan int),
			taskStatus:     make([]TaskStatus, nReduce),
			nFinishedTask:  0,
		},
		workerTracker: WorkerTracker{
			nWorker:       0,
			exited:        make([]bool, 0),
			nExitedWorker: 0,
		},
	}
	c.mapTaskTracker.initializeTasksAndStatus(MAP, files, nReduce)
	c.reduceTaskTracker.initializeTasksAndStatus(REDUCE, files, nReduce)
	// background threads to supply tasks to the channel
	go c.mapTaskTracker.supplyTasksToChannel()
	go c.reduceTaskTracker.supplyTasksToChannel()
	log.Println("Coordinator is successfully instantiated!")
	// expose the server to the socket, change the states of coordinator and reschedule accordingly
	c.server()
	return &c
}

// initialize the slice fields in taskTracker
func (tracker *TaskTracker) initializeTasksAndStatus(taskType TaskType, files []string, nReduce int) {
	for i := 0; i < tracker.nTask; i++ {
		// initialize TaskObj status
		statusPointer := &tracker.taskStatus[i]
		statusPointer.state = UNSCHEDULED
		statusPointer.taskId = i
		// -1 denotes no worker associated
		statusPointer.workerId = -1

		// initialize TaskObj type
		taskPointer := &tracker.tasks[i]
		taskPointer.TaskId = i
		taskPointer.Type = taskType
		taskPointer.NumOfBuckets = nReduce
		taskPointer.NumOfFiles = len(files)
		if taskType == MAP {
			taskPointer.Filename = files[i]
		}
	}
}

// supply tasks of a tracker to its channel
func (tracker *TaskTracker) supplyTasksToChannel() {
	for i := 0; i < tracker.nTask; i++ {
		tracker.taskToSchedule <- i
	}
}
