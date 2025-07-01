package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// map  返回的是Kay value
// reduce 返回的是string
// 1.map 2 reduce
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		response := doHeartBeat()
		// log.Printf("Worker:receive coordinator's hearbeat %v\n", response)
		switch response.JobType {
		case TaskTypeMap:
			doMapTask(mapf, response)
		case TaskTypeReduce:
			doReduceTask(reducef, response)
		case TaskTypeWait:
			time.Sleep(time.Second)
		case TaskTypeExit: //完成任务
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}
	// Your worker implementation here.

	//写到文件中去

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func doHeartBeat() Response {
	request := Request{}
	response := Response{}
	ok := call("Coordinator.GetTask", &request, &response)

	if ok {
		// fmt.Println(response.FileName)
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n"
	} else {
		fmt.Printf("call failed!\n")
	}

	return response
}

// 如何保证容错
func doMapTask(mapf func(string, string) []KeyValue, response Response) {

	reduceNum := response.ReduceNum // 任务数量
	filePath := response.FileName   //文件名字
	mapNum := response.JobNum
	//读取文件
	content, err := os.ReadFile(filePath)
	if err != nil {
		//panic 或者log
		log.Fatalf("ReadFile failed %v:%v", filePath, err)
	}

	kva := mapf(filePath, string(content))

	tmpFiles := make([]*os.File, reduceNum)
	tempFileNames := make([]string, reduceNum)
	encoders := make([]*json.Encoder, reduceNum)

	for i := 0; i < reduceNum; i++ {
		tempFileNames[i] = fmt.Sprintf("mr-%d-%d-tmp-", mapNum, i)
		file, err := os.CreateTemp(".", tempFileNames[i])
		if err != nil {
			log.Fatalf("create temp file failed: %v", err)
		}
		// defer file.Close()
		// log.Printf("Temp file created: %s", file.Name()) // 👈 打印出来
		tmpFiles[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	for _, kv := range kva {
		reduceID := ihash(kv.Key) % reduceNum
		if err := encoders[reduceID].Encode(&kv); err != nil {
			log.Fatalf("encode failed for key %v:%v", kv.Key, err)
		}
	}
	for i := 0; i < reduceNum; i++ {
		// TODO: 加入写入重试机制，避免因磁盘问题丢失结果
		if err := os.Rename(tmpFiles[i].Name(), fmt.Sprintf("mr-%d-%d", mapNum, i)); err != nil {
			log.Fatalf("rename failed for %v:%v", tmpFiles[i].Name(), err)
		}

	}

	reportTaskArgs := ReportTaskArgs{
		JobType: TaskTypeMap,
		JobNum:  response.JobNum,
	}
	var reply ReportTaskTaskReply
	//是不是要有重试机制
	maxRetry := 3
	for i := 0; i < maxRetry; i++ {
		ok := call("Coordinator.ReportTaskDone", &reportTaskArgs, &reply)
		if ok && reply.Ack {
			break
		}
		log.Printf("Retrying ReportTaskDone... attempt %d", i+1)
		time.Sleep(1 * time.Second)
	}

	for _, f := range tmpFiles {
		f.Close()
	}
}

func doReduceTask(reducef func(string, []string) string, response Response) {

	// reduceId := response.reduceNum //reduce 任务数量
	mapNum := response.MapNum
	taskNum := response.JobNum
	kvMap := make(map[string][]string)
	for i := 0; i < mapNum; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, taskNum)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open file %v:%v", fileName, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
		file.Close() //
	}

	//2 获取并排序所有key
	var keys []string
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	tempFileNames := fmt.Sprintf("mr-out-%d-tmp-", taskNum)
	tempFile, err := os.CreateTemp(".", tempFileNames)
	if err != nil {
		log.Fatalf("cannot create file %v:%v", tempFile, err)
	}
	defer tempFile.Close()

	for _, k := range keys {
		v := reducef(k, kvMap[k])
		fmt.Fprintf(tempFile, "%v %v\n", k, v)
	}

	if err := os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", taskNum)); err != nil {
		log.Fatalf("rename file failded %v:%v", tempFileNames, err)
	}
	reportTaskArgs := ReportTaskArgs{
		JobType: TaskTypeReduce,
		JobNum:  response.JobNum,
	}
	//是不是要有重试机制

	var reply ReportTaskTaskReply

	maxRetry := 3
	for i := 0; i < maxRetry; i++ {
		ok := call("Coordinator.ReportTaskDone", &reportTaskArgs, &reply)
		if ok && reply.Ack {
			break
		}
		log.Printf("Retrying ReportTaskDone... attempt %d", i+1)
		time.Sleep(1 * time.Second)
	}
}

// 告诉coordinate我任务完成了

// func doSave(para Response, data interface{}) {

// 	//创建临时文件
// 	tmpFile, err := os.CreateTemp(".", "mr-out-*")
// 	if err != nil {
// 		panic("create file mr-out-* err")
// 	}
// 	tmpName := tmpFile.Name()
// 	defer tmpFile.Close()
// 	defer os.Remove(tmpName)

// 	contentStr, _ := data.(string)

// 	_, err = tmpFile.Write([]byte(contentStr))
// 	if err != nil {
// 		panic("save data err" + err.Error())
// 	}

// 	//map 类型
// 	if para.jobType == TaskTypeMap {
// 		newPath = fmt.Sprintf(IntermediateFilePattern, para.mapNum)
// 	}
// 	//reduce 类型
// 	if para.jobType == TaskTypeReduce {
// 		newPath := fmt.Sprintf("mr-out-%d", para.reduceId)
// 	}

// 	os.Rename("./temp")
// }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
