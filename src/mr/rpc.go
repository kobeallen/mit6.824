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

// Add your RPC definitions here.
type AllocateWorkerRequest struct {
	// Client process id.
	ClientPid int
}

type AllocateWorkerResponse struct {
	// Map or Reduce task id. Map client uses it to build intermediate output file.
	Id int
	// Maximum number of reducer configured in Master server. Client uses it for module operation.
	NumReducer int
	// Map's FilePath is for input. Reduce's FilePath is for output.
	FilePath string
}

type DoneRequest struct {
	// Map's FilePath is for input. Reduce's FilePath is for output. FilePath and ClientPid are used to find the
	// corresponding task and mark the task as done.
	FilePath string
	// Client process id. FilePath and ClientPid are used to find the corresponding task and mark the task as done.
	ClientPid int
	// Some reduce tasks might not be used because its key is not generated. Map client use this field to indicate
	// which reduce key(bucket) is used and send back to Master to mark.
	Buckets []int
}

type DoneResponse struct {
}

type IsDoneRequest struct {}

type IsDoneResponse struct {
	// Indicate the whole map or reduce is done.
	Done bool
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
