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
	taskType   TaskType // MAP | REDUCE | EXIT
	taskId     int
	filename   string // only used by MAP type
	numOfFiles int    // only used by REDUCE type
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
		log.Printf("Map request %d scheduled to worker %d!", taskId, args.workerId)
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
		log.Printf("Reduce request %d scheduled to worker %d!", taskId, args.workerId)
		return nil
	}
	// Both channels closed, the job is done, exit
	reply.task = &Task{taskType: EXIT}
	log.Printf("Inform worker %d to exit!", args.workerId)
	return nil
}

// atomically update the task status after the task is assigned to a worker
func updateTaskStatusPostAssignment(taskTracker *TaskTracker, workerId int, taskId int) {
	taskTracker.mu.Lock()
	taskStatus := &taskTracker.taskStatus[taskId]
	taskStatus.startTime = time.Now()
	taskStatus.state = ONGOING
	taskStatus.workerId = workerId
	taskTracker.mu.Unlock()
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
