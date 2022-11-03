package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
const HEART_BEAT_INTERVAL = 2

const ERROR_ALL_TASK_COMPLETE = "All Task Complete"
const ERROR_NO_SUCH_WORKER = "no such worker"
const ERROR_TASK_ALREADY_COMPLETED = "task Already completed"
const ERROR_TASK_IS_COMPLETING_BY_OTHERS = "task is completing by others"
const ERROR_SHOULD_NOT_HAPPEN = "error should not happen"
const ERROR_NO_IDLE_TASK="no idle task"

const OUTPUT_PREFIX_REDUCE = "mr-out-"

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

var WorkerID string

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	WorkerID = "worker-" + uuid.New().String() + "-worker"
	client := NewMapReduceServiceClient()
	heartBeatRequest := &HeartBeatRequest{
		WorkerID: WorkerID,
	}
	//CallExample(client)
	err := client.HearBeat(heartBeatRequest, &HeartBeatResponse{})
	if err != nil {
		log.Fatal("HeartBeatError:", err)
	}
	//heartBeat goroutine
	doneCh := make(chan bool)
	go func() {
		time.Sleep(HEART_BEAT_INTERVAL * time.Second)
		timeTicker := time.NewTicker(HEART_BEAT_INTERVAL * time.Second)
		for {
			select {
			case <-timeTicker.C:
				err := client.HearBeat(heartBeatRequest, &HeartBeatResponse{})
				if err != nil {
					log.Fatal("HeartBeatError:", err)
				}
			case <-doneCh:
				return
			}
		}
	}()


	
	for {
		select{
		case <-doneCh:
			break
		default:
			pollTaskRequest := &PollTaskRequest{
				WorkerID: WorkerID,
			}
			//[knowledge]Reponse是不可以赋初值的,如果附初值，rpc调用后这个初值将不会改变
			pollTaskResponse := &PollTaskResponse{
				//TaskID: 110,
			}
			log.Printf("poll task from worker %v",WorkerID)
			err := client.PollTask(pollTaskRequest, pollTaskResponse)
			if err != nil {
				switch err.Error() {
				case ERROR_ALL_TASK_COMPLETE:
					log.Println("All Task Complete, exit")
					doneCh <- true
					return
				case ERROR_NO_SUCH_WORKER:
					log.Println("no such worker")
					time.Sleep(HEART_BEAT_INTERVAL * time.Second)
					continue
				case ERROR_NO_IDLE_TASK:
					log.Println("no idle task")
					time.Sleep(time.Second)
					continue
				default:
					log.Fatalf("worker poll Task Failed:%v", err)
				}
			}
			log.Printf("inputPath=%v,taskType=%v,taskID=%v,NReduce=%v",
				pollTaskResponse.InputPath,
				pollTaskResponse.TaskType,
				pollTaskResponse.TaskID,
				pollTaskResponse.NReduce)
			//TODO:按说出错处理应该在这处理，内层的都网上抛出
			if pollTaskResponse.TaskType == TASK_TYPE_MAP {
				DealWithMapTask(client, pollTaskResponse, mapf)
			} else if pollTaskResponse.TaskType == TASK_TYPE_REDUCE {
				//读入，执行reducf，将结果写入文件
				DealWithReduceTask(client, pollTaskResponse, reducef)
			} else {
				log.Fatal("wrong Task Type")
			}	
		}
	}

}

func DealWithMapTask(client *MapReduceServiceClient, taskInfo *PollTaskResponse, mapf func(string, string) []KeyValue) error {
	//读取文件，问文件执行mapf
	file, err := os.Open(taskInfo.InputPath)
	if err != nil {
		log.Fatalf("cannot open %v:%v", taskInfo.InputPath, err.Error())
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskInfo.InputPath)
	}

	kvs := mapf(taskInfo.InputPath, string(content))
	log.Println("mapf finished")

	completeTaskRequest := &TryCompleteTaskRequest{
		TaskID:   taskInfo.TaskID,
		TaskType: taskInfo.TaskType,
		WorkerID: WorkerID,
	}
	for {
		err = client.TryCompleteTask(completeTaskRequest, &TryCompleteTaskResponse{})
		if err != nil {
			if err.Error() == ERROR_TASK_ALREADY_COMPLETED {
				return nil
			}
			if err.Error() == ERROR_TASK_IS_COMPLETING_BY_OTHERS {
				time.Sleep(2 * time.Second)
				continue
			}
			log.Fatal(err)
		}
		break
	}

	err = writeKVsTofile(int(taskInfo.TaskID), taskInfo.NReduce, kvs)
	if err != nil {
		log.Fatal(err)
	}

	confirmRequest := &ConfirmCompleteTaskRequest{
		TaskID:   taskInfo.TaskID,
		TaskType: taskInfo.TaskType,
		WorkerID: WorkerID,
	}
	err = client.ConfirmCompleteTask(confirmRequest, &ConfirmCompleteTaskResponse{})
	if err != nil {
		log.Fatalf("Confirm err:%v", err)
	}
	return nil
}

// for sorting by key.
type ByKey []*KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func DealWithReduceTask(client *MapReduceServiceClient, taskInfo *PollTaskResponse, reducef func(string, []string) string) error {
	//TODO： 每个Map的每个输出都写到单独的文件里面mr-x-y
	//现在ReduceTask的输入应该是一组文件
	kvs := readReduceTaskInputFromFile(int(taskInfo.TaskID))
	sort.Sort(ByKey(kvs))

	outputName := OUTPUT_PREFIX_REDUCE + strconv.Itoa(int(taskInfo.TaskID))
	//如果有之前的outputName，应该删除
	//检查脚本会处理所有mr-out*，所以临时文件不能以mr-out开头
	ofile, err := os.Create(WorkerID + outputName)
	if err != nil {
		log.Fatalf("Create %v error:%v", outputName, err)
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		//log.Printf("key %v is processing", kvs[i].Key)
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	ofile.Close()

	//准备提交这个task
	completeTaskRequest := &TryCompleteTaskRequest{
		TaskID:   taskInfo.TaskID,
		TaskType: taskInfo.TaskType,
		WorkerID: WorkerID,
	}
	for {
		err = client.TryCompleteTask(completeTaskRequest, &TryCompleteTaskResponse{})
		if err != nil {
			if err.Error() == ERROR_TASK_ALREADY_COMPLETED {
				log.Printf("%v removing intermediate file", WorkerID)
				files,err:=filepath.Glob(WorkerID + "*")
				if err!=nil{
					log.Printf("Glob error:%v",err)
				}
				for _,file:=range files{
					os.Remove(file)
				}
				return nil
			}
			if err.Error() == ERROR_TASK_IS_COMPLETING_BY_OTHERS {
				time.Sleep(2 * time.Second)
				continue
			}
			log.Fatal(err)
		}
		break
	}

	os.Rename(WorkerID+outputName, outputName)
	confirmRequest := &ConfirmCompleteTaskRequest{
		TaskID:   taskInfo.TaskID,
		TaskType: taskInfo.TaskType,
		WorkerID: WorkerID,
	}
	err = client.ConfirmCompleteTask(confirmRequest, &ConfirmCompleteTaskResponse{})
	if err != nil {
		log.Fatalf("Confirm err:%v", err)
	}
	return nil
}

// 为什么收不到返回值？[answer] 因为没有操作reply
func readReduceTaskInputFromFile(taskID int) []*KeyValue {
	parttern := INTERMEDIATE_FILE_PREFIX + "-[0-9]-" + strconv.Itoa(taskID)
	fileNames, _ := filepath.Glob(parttern)

	var kvs []*KeyValue
	for _, name := range fileNames {
		log.Printf("opening %v", name)
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("Cannot open %v: %v", name, err)
		}

		var kvsFromFile []*KeyValue
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&kvsFromFile)
		if err != nil {
			log.Fatalf("Decode %v error:%v", name, err.Error())
		}

		kvs = append(kvs, kvsFromFile...)
		file.Close()
	}

	// for _, name := range fileNames {
	// 	os.Remove(name)
	// }
	return kvs
}

const INTERMEDIATE_FILE_PREFIX = "mr"

func writeKVsTofile(TaskID int, nReduce int32, kvs []KeyValue) error {
	var encoders []*json.Encoder
	for i := 0; i < int(nReduce); i++ {
		file, err := os.OpenFile(INTERMEDIATE_FILE_PREFIX+"-"+strconv.Itoa(TaskID)+"-"+strconv.Itoa(i),
			os.O_CREATE|os.O_APPEND|os.O_RDWR,
			0666)
		if err != nil {
			return err
		}
		defer file.Close()

		encoders = append(encoders, json.NewEncoder(file))
	}

	var KVMatrix [10][]*KeyValue

	for idx, _ := range kvs {
		//log.Printf("key=%v", kvs[idx].Key)
		ReduceNo := ihash(kvs[idx].Key) % int(nReduce)
		KVMatrix[ReduceNo] = append(KVMatrix[ReduceNo], &kvs[idx])
	}
	for i := 0; i < int(nReduce); i++ {
		encoders[i].Encode(KVMatrix[i])
	}
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample(client *MapReduceServiceClient) {

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
	//ok := call("Coordinator.Example", &args, &reply)
	err := client.Example(&args, &reply)
	if err == nil {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed! %v\n", err)
	}
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

type MapReduceServiceClient struct {
	*rpc.Client
}

func NewMapReduceServiceClient() *MapReduceServiceClient {
	socketName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", socketName)
	if err != nil {
		log.Fatal("Dialing err:", err)
	}
	return &MapReduceServiceClient{c}
}

var _ MapReduceService = (*MapReduceServiceClient)(nil)

func (c *MapReduceServiceClient) HearBeat(args *HeartBeatRequest, reply *HeartBeatResponse) error {
	return c.Client.Call("Coordinator.HearBeat", args, reply)
}

func (c *MapReduceServiceClient) TryCompleteTask(args *TryCompleteTaskRequest, reply *TryCompleteTaskResponse) error {
	return c.Client.Call("Coordinator.TryCompleteTask", args, reply)
}

func (c *MapReduceServiceClient) ConfirmCompleteTask(args *ConfirmCompleteTaskRequest, reply *ConfirmCompleteTaskResponse) error {
	return c.Client.Call("Coordinator.ConfirmCompleteTask", args, reply)
}

func (c *MapReduceServiceClient) PollTask(args *PollTaskRequest, reply *PollTaskResponse) error {
	return c.Client.Call("Coordinator.PollTask", args, reply)
}

func (c *MapReduceServiceClient) Example(args *ExampleArgs, reply *ExampleReply) error {
	return c.Client.Call("Coordinator.Example", args, reply)
}

// 以追加写的方式写入文件，Linux kernel已经保证了追加写是原子操作
func WriteKVToFile(fileName string, KVs []KeyValue) error {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE, 0666)
	if err == nil {
		return err
	}
	defer file.Close()

	//write := bufio.NewWriter(file)
	decoder := json.NewEncoder(file)
	err = decoder.Encode(KVs)
	return err
}
