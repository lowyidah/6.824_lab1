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

type ReadyDoneArgs struct {
	// these indicate task completed
	// either "m" or "r" or "d"
	// if task_id is -1, no task completed
	TASK_TYPE string
	TASK_ID   int
}

type ReadyDoneReply struct {
	// these indicate task to do
	// either "m" or "r", "d"
	TASK_TYPE string
	TASK_ID   int
	FILE      string
	// indicates number of type of task not being done
	NMR int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
