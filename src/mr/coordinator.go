package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	// Your definitions here.
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
	nMap int
	mapTasks []TaskStatus
	nReduce int
	reduceTasks []TaskStatus
	currentStage int 
	workerTracker WorkerTracker
	mu sync.mutex	
}
type WorkerTracker struct{
	nWorker int
	exited []bool
	nExitedWorker int
}
type TaskTracker struct{
	mu sync.mutex
	nTask int
	tasks []Task
	taskStatus []TaskStatus
	unscheduledTasks []int
	nFinishedTask int	
}
type TaskStatus struct{
	taskId int	
	task Task
	state int
	startTime time.Time
	workerId int 
}
type Task interface{
	doTask(Worker) error
}
type ExitTask struct{
}
type MapTask struct{
	taskId int
	filename string
}
type ReduceTask struct{
	taskId int
	numOfFiles int
}
// Your code here -- RPC handlers for the worker to call.


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
