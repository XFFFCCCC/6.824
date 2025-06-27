package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task struct {
	Filename  string
	TaskId    int
	State     int //   0idle   1 running  2完成
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	Files       []string //输入文件列表
	NReduce     int      //Reduce 任务数量
	MapTasks    []Task
	ReduceTasks []Task
	Stage       string //"map","reduce","done"
}

// 判断目前的状态  ，先硬编码
func (c Coordinator) isMapStage() bool {
	return c.Stage == "map"
}

func (c Coordinator) isReduceStage() bool {
	return c.Stage == "reduce"
}

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

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// 如何知道map任务结束了
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 如何给worker分配map 或者reduce
func (c *Coordinator) GetTask(request *Request, response *Response) error {
	if c.isMapStage() {
		for i, task := range c.MapTasks {
			if task.State == 0 {
				//分配任务
				response.fileName = task.Filename
				response.jobType = TaskTypeMap
				response.mapNum = i
				response.reduceNum = c.NReduce
			} else if task.State == 1 {
				response.jobType = TaskTypeWait
			}
		}
		//这个地方应该协程睡眠
	} else if c.isReduceStage() {
		for i, task := range c.ReduceTasks {
			if task.State == 0 {
				//分配任务
				response.jobType = TaskTypeReduce
				response.reduceNum = i
				response.reduceNum = c.NReduce
			} else if task.State == 1 {
				response.jobType = TaskTypeWait
			}
		}
	} else {
		//携程退出
		response.jobType = TaskTypeExit
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:       files,
		NReduce:     nReduce,
		Stage:       "map",
		MapTasks:    make([]Task, len(files)),
		ReduceTasks: make([]Task, nReduce),
	}

	for i, fname := range files {
		c.MapTasks[i] = Task{
			TaskId:   i,
			Filename: fname,
			State:    0, //idle
		}
	}

	for i := range nReduce {
		c.ReduceTasks[i] = Task{
			TaskId: i,
			State:  0,
		}
	}

	// Your code here.

	c.server()

	//启动后台任务监控线程(例如超时处理和阶段切换)

	return &c
}
