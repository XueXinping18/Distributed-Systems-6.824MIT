package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// declare arguments and reply for registering the worker to the coordinator
type RegisterArgs struct {
}
type RegisterReply struct {
	WorkerId int
}

// declare arguments and reply for request a new TaskObj
type RequestArgs struct {
	WorkerId int
}
type RequestReply struct {
	TaskObj *Task
}

// declare arguments and reply format for informing the finish of a TaskObj
type FinishedArgs struct {
	WorkerId int
	TaskId   int
	Type     TaskType
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
