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

// 获取任务

type AskTaskArgs struct {
}

type AskTaskReply struct {
	Done     bool
	TaskType TaskType // 1:map 2:reduce
	Index    int64
	FileName string
}

type TaskType int32

const (
	TaskWait       TaskType = 0
	TaskTypeMap    TaskType = 1
	TaskTypeReduce TaskType = 2
)

// 完成任务

type FinishTaskArgs struct {
	TaskType    TaskType
	Index       int64
	ResFileName string
}

type FinishTaskReply struct {
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
