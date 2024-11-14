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

type Job int64

const (
	Map Job = iota
	Reduce
	Wait
)

// Add your RPC definitions here.
type FetchTask struct {
	//empty
}

type RespondTask struct {
	FileName string
	FileIndex int
	Task Job
	NumWork int
	Files []string

}

type RespondReport struct {
	Done bool
	FileIndex int
	Task Job
	Files map[int][]string
}

type FetchReport struct {
	//empty
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
