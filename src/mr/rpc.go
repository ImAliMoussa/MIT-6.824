package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type MRCommand uint8

const (
	MAP MRCommand = iota
	REDUCE
	WAIT
	EXIT
)

type WorkerRequest struct{}

type WorkerResponse struct {
	CommandType MRCommand
	FileToMap   string
	ReducePos   int
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
