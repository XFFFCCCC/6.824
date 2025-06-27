package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
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
	response := doHeartBeat()

	for {
		switch response.jobType {
		case TaskTypeMap:
			doMapTask(mapf, response)
		case TaskTypeReduce:
			doReduceTask(reducef, response)
		case TaskTypeWait:
			time.Sleep(1 * time.Second)
		case TaskTypeExit: //完成任务
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v"), response.jobType)
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
	ok := call("Coordinator.Example", &request, &response)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", response.Y)
	} else {
		fmt.Printf("call failed!\n")
	}

	return response
}

// 如何保证容错
func doMapTask(mapf func(string, string) []KeyValue, response Response) {
	reduceNum := response.reduceNum // 任务数量
	filePath := response.fileName   //文件名字
	mapNum := response.jobNum
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
		tempFileNames[i] = fmt.Sprintf("mr-%d-%d-tmp-*", mapNum, i)
		file, err := os.CreateTemp(".", tempFileNames[i])
		if err != nil {
			log.Fatalf("create temp file failed: %v", err)
		}
		defer file.Close()
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
		os.Rename(tempFileNames[i], fmt.Sprint("mr-%d-%d", mapNum, i))
	}
}

func doReduceTask(reducef func(string, []string) string, response Response) {
	reduceId := response.reduceId //reduce 任务数量
	mapNum := response.mapNum
	// newPath="mr-"+(string)mapNum+(string)reduceNum
	content, err := os.ReadFile(newPath)
	// if err!=
	//用map存放key->[]values
	// key string value 切片
	intermediate := []KeyValue{}
	//遍历所有map任务生成的文件
	for i := 0; i < mapNum; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceId)
		content, err := os.ReadFile(filename)
		if err != nil {
			continue
		}

		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, " ", 2)
			if len(parts) != 2 {
				continue
			}
			key := parts[0]
			value := parts[1]
			intermediate[key] = append(intermediate[key], value)
		}
	}

	//将key排序，可选 ，方便测试一致
	var keys []string
	for k := range intermediate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var resultBuilder string.Builder
	for _, key := range keyes {
		values := intermediate[key]
		output := reducef(key, values)
		resultBuilder.WriteString(fmt.Sprintf("%v %v\n", key, output))
	}
	//写入最终输出文件(如 mr-out-<reduceID> )
	doSave(response, resultBuilder.String())
}

func doSave(para Response, data interface{}) {

	//创建临时文件
	tmpFile, err := os.CreateTemp(".", "mr-out-*")
	if err != nil {
		panic("create file mr-out-* err")
	}
	tmpName := tmpFile.Name()
	defer tmpFile.Close()
	defer os.Remove(tmpName)

	contentStr, _ := data.(string)

	_, err = tmpFile.Write([]byte(contentStr))
	if err != nil {
		panic("save data err" + err.Error())
	}

	//map 类型
	if para.jobType == TaskTypeMap {
		newPath = fmt.Sprintf(IntermediateFilePattern, para.mapNum)
	}
	//reduce 类型
	if para.jobType == TaskTypeReduce {
		newPath := fmt.Sprintf("mr-out-%d", para.reduceId)
	}

	os.Rename("./temp")
}

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
