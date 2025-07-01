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

type TaskType int

// 任务类型
const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
	TaskTypeWait
	TaskTypeExit
)

const IntermediateFilePattern = "mr-%d-%d"

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

type ReportTaskArgs struct {
	JobType TaskType
	JobNum  int
}

type ReportTaskTaskReply struct {
	Ack bool
}

// // Add your RPC definitions here.
// type Tasks interface {
// 	Run()
// }
// type MapTask struct{}
// type ReduceTask struct{}

type Request struct {
}

type Response struct {
	FileName  string //文件名
	ReduceNum int    //分为几个reduce文件
	JobNum    int
	JobType   TaskType //任务类型  jobType
	MapNum    int
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
