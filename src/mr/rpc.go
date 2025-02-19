package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Args struct {
	RequestType RequestType
	TaskNumber  int
}

type Reply struct {
	TaskType   TaskType
	Filename   string
	TaskNumber int
	NReduce    int
	NMaps      int
}

// Add your RPC definitions here.
func MakeArgs() *Args {
	arg := Args{}

	// Default values for Args
	arg.RequestType = INVALID_REQUEST
	arg.TaskNumber = -1

	return &arg
}

func MakeReply() *Reply {
	reply := Reply{}

	// Default values for Args
	reply.TaskType = INVALID_TASK
	reply.Filename = "-"
	reply.TaskNumber = -1
	reply.NReduce = -1
	reply.NMaps = -1

	return &reply
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
