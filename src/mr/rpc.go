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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
type MapReduceService interface {
	PollTask(args *PollTaskRequest, reply *PollTaskResponse) error
	TryCompleteTask(args *TryCompleteTaskRequest, reply *TryCompleteTaskResponse) error
	ConfirmCompleteTask(args *ConfirmCompleteTaskRequest,reply *ConfirmCompleteTaskResponse) error
	HearBeat(args *HeartBeatRequest, reply *HeartBeatResponse) error
}
type PollTaskRequest struct {
	WorkerID string
}
type PollTaskResponse struct {
	InputPath string
	TaskType  int32
	TaskID    int32
	NReduce   int32
}


type TryCompleteTaskRequest struct {
	TaskID   int32
	TaskType int32
	WorkerID string
}
type TryCompleteTaskResponse struct {
	InputPath string
	TaskType  int32
	TaskID    string
}

type ConfirmCompleteTaskRequest struct {
	TaskID   int32
	TaskType int32
	WorkerID string
}
type ConfirmCompleteTaskResponse struct {
	InputPath string
	TaskType  int32
	TaskID    string
}


type HeartBeatRequest struct {
	WorkerID string
}
type HeartBeatResponse struct {
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
