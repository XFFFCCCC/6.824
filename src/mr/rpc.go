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

// 任务类型
const (
	TaskTypeMap    = "map"
	TaskTypeReduce = "reduce"
	TaskTypeWait   = "wait"
	TaskTypeExit   = "exit"
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

// Add your RPC definitions here.

type Tasks interface {
	Run()
}
type MapTask struct{}
type ReduceTask struct{}

// 读取文件 分片 生成中间文件
func (m MapTask) run() {

}

// 合并中间文件 输出结果
func (r ReduceTask) run() {

}

type Request struct {
}

type Response struct {
	fileName  string //文件名
	reduceNum int    //分为几个reduce文件
	mapNum    int    //map数量  	//第几个map任务？  一起有几个map任务
	jobType   string //任务类型  jobType
	reduceId  int    //第几个reduce任务
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
