package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// declare arguments and reply for registering the worker to the coordinator
type RegisterArgs struct {
}
type RegisterReply struct {
	workerId int
}

// declare arguments and reply for request a new task
type RequestArgs struct {
	workerId int
}
type RequestReply struct {
	task *Task
}
type FinishedArgs struct {
	workerId int
	taskId   int
	taskType TaskType
}
type FinishedReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
