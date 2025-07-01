package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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

	Files   []string //输入文件列表
	NReduce int      //Reduce 任务数量
	Tasks   []Task
	// ReduceTasks []Task
	Stage string //"map","reduce","done"
	mu    sync.Mutex
}

// 判断目前的状态  ，先硬编码
func (c *Coordinator) isMapStage() bool {
	return c.Stage == "map"
}

func (c *Coordinator) isReduceStage() bool {
	return c.Stage == "reduce"
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isMapStage() {
		for i, task := range c.Tasks {
			// log.Print(i, task.State, "Map")
			if task.State == 0 {
				//分配任务
				// log.Print(i, task.State, "Map")
				c.Tasks[i].State = 1
				response.FileName = task.Filename
				response.JobType = TaskTypeMap
				response.JobNum = i
				response.ReduceNum = c.NReduce
				c.Tasks[i].StartTime = time.Now()
				response.MapNum = len(c.Files)
				return nil
			}
		}

		//这个地方应该协程睡眠
	} else if c.isReduceStage() {
		for i, task := range c.Tasks {
			// log.Print(i, task.State, "reduce")
			if task.State == 0 {
				//分配任务
				c.Tasks[i].State = 1
				c.Tasks[i].StartTime = time.Now()
				response.JobType = TaskTypeReduce
				response.JobNum = i
				response.ReduceNum = c.NReduce
				response.MapNum = len(c.Files)
				return nil
			}
		}

	} else {
		response.JobType = TaskTypeExit
		return nil
	}

	response.JobType = TaskTypeWait
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Tasks[args.JobNum].State = 2
	reply.Ack = true
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

func (c *Coordinator) allTasksDone(task []Task) bool {
	for _, t := range task {
		if t.State != 2 {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false
	if c.isMapStage() && c.allTasksDone(c.Tasks) {
		c.Stage = "reduce"
		c.Tasks = make([]Task, c.NReduce)
		for i := 0; i < c.NReduce; i++ {
			c.Tasks[i] = Task{
				TaskId: i,
				State:  0,
			}
		}
	} else if c.isReduceStage() && c.allTasksDone(c.Tasks) {
		c.Stage = "done"
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// fmt.Print(files)
	c := Coordinator{
		Files:   files,
		NReduce: nReduce,
		Stage:   "map",
		Tasks:   make([]Task, len(files)),
	}

	for i, fname := range files {
		c.Tasks[i] = Task{
			TaskId:   i,
			Filename: fname,
			State:    0, //idle
		}
	}

	// for i := range nReduce {
	// 	c.ReduceTasks[i] = Task{
	// 		TaskId: i,
	// 		State:  0,
	// 	}
	// }

	// Your code here.

	c.server()
	go func() {
		for {
			time.Sleep(time.Second)
			c.mu.Lock()
			if c.Stage == "done" {
				c.mu.Unlock()
				break
			}
			checkTimeout := func(t Task) bool {
				return t.State == 1 && time.Since(t.StartTime) > 10*time.Second
			}

			for i := range c.Tasks {
				if checkTimeout(c.Tasks[i]) {
					// log.Printf("Map task %d timed out. Reassigning...\n", i)
					c.Tasks[i].State = 0
				}
			}

			//c.Done()
			c.mu.Unlock()
		}
	}()

	//启动后台任务监控线程(例如超时处理和阶段切换)

	return &c
}

//对于判断当前阶段有两个思路，第一个是在GetTask函数中判断当前阶段的任务是否完成
// 还一个思路是开协程来做，我是用的第二种方式

//TODO
// 崩溃恢复没实现  超时任务重试
// 代码最后优化   Task这个应该可以精简
//支持 TaskTypeExit	   Worker 能退出

//并发优化  明天实现
