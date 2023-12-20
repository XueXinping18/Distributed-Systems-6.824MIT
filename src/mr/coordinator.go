package mr

import (
	"log"
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
	mapTaskTracker    TaskTracker
	reduceTaskTracker TaskTracker
	currentStage      TaskType
	workerTracker     WorkerTracker
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
	Type     TaskType // MAP | REDUCE | EXIT
	TaskId       int
	Filename     string // only used by MAP type
	NumOfBuckets int    // only used by MAP type for hashing
	NumOfFiles   int    // only used by REDUCE type for iterating
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// register the worker to allocate a worker id and increment the number of worker
func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	defer c.workerTracker.mu.Unlock()
	c.workerTracker.mu.Lock()
	reply.workerId = c.workerTracker.nWorker
	c.workerTracker.nWorker++
	c.workerTracker.exited = append(c.workerTracker.exited, false)
	log.Printf("Worker id %d assigned.\n", reply.workerId)
	return nil
}
func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	// attempt to read from Map Channel
	taskId, ok := <-c.mapTaskTracker.taskToSchedule
	if ok {
		mapTask := c.mapTaskTracker.tasks[taskId]
		// assign the task by reference. Go will send the object instead of pointer in RPC
		reply.task = &mapTask
		// update task status atomically
		updateTaskStatusPostAssignment(&c.mapTaskTracker, args.workerId, taskId)
		log.Printf("Map request %d scheduled to worker %d!\n", taskId, args.workerId)
		return nil
	}
	// map channel is closed, attempt to read from Reduce channel
	taskId, ok = <-c.reduceTaskTracker.taskToSchedule
	if ok {
		reduceTask := c.reduceTaskTracker.tasks[taskId]
		// assign the task by reference. Go will send the object instead of pointer in RPC
		reply.task = &reduceTask
		// update task status atomically
		updateTaskStatusPostAssignment(&c.reduceTaskTracker, args.workerId, taskId)
		log.Printf("Reduce request %d scheduled to worker %d!\n", taskId, args.workerId)
		return nil
	}
	// Both channels closed, the job is done, exit
	reply.task = &Task{Type: EXIT}
	log.Printf("Inform worker %d to exit!\n", args.workerId)
	return nil
}

// finish a task, update the status
func (c *Coordinator) TaskFinished(args *FinishedArgs, reply *FinishedReply) error {
	if args.taskType != c.currentStage {
		log.Fatalf("Mismatch between current stage "+c.currentStage.String()+
			" and the task type "+args.taskType.String()+" reported by worker %d\n", args.workerId)
		return os.ErrInvalid
	}
	switch args.taskType {
	case MAP:
		finished := updateTaskStatusPostFinish(&c.mapTaskTracker, args.taskId, args.workerId)
		if finished {
			// Set stage first, close the channel next because longer blocking does no harm while start to read
			// new channel before the stage flags are set might be problematic
			c.currentStage = REDUCE
			close(c.mapTaskTracker.taskToSchedule)
			log.Println("The MAP stage has been completed. Next: REDUCE")
		}
	case REDUCE:
		finished := updateTaskStatusPostFinish(&c.reduceTaskTracker, args.taskId, args.workerId)
		if finished {
			c.currentStage = EXIT
			close(c.reduceTaskTracker.taskToSchedule)
			log.Println("The REDUCE stage has been completed. Next: EXIT")
		}
	default:
		log.Fatalf("Bad task type reported being finished: " + args.taskType.String() + "\n")
		return os.ErrInvalid
	}
	return nil
}

// update the task status atomically after a task is finished, return true if the stage is also finished
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

// atomically update the task status after the task is assigned to a worker
func updateTaskStatusPostAssignment(taskTracker *TaskTracker, workerId int, taskId int) {
	defer taskTracker.mu.Unlock()
	taskTracker.mu.Lock()
	taskStatus := &taskTracker.taskStatus[taskId]
	taskStatus.startTime = time.Now()
	taskStatus.state = ONGOING
	taskStatus.workerId = workerId
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
