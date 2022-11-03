package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TASK_TYPE_MAP = 1
const TASK_TYPE_REDUCE = 2

const STAGE_INIT = 0
const STAGE_MAP = 1
const STAGE_REDUCE = 2
const STAGE_COMPLETE = 3

const TASK_STATUS_IDLE = 0
const TASK_STATUS_PROCESSING = 1
const TASK_STATUS_COMPLETING = 2
const TASK_STATUS_COMPETED = 3

const TIME_LIMIT = 10

type Coordinator struct {
	// Your definitions here.
	Workers         map[string]*WorkerInfo
	MapTaskInfos    []TaskInfo
	ReduceTaskInfos []TaskInfo
	stage           int32 //0 表示MapStage,1表示ReduceStage,2表示Complete

	/**
	使用一个任务计数的坏处就是，如果一个任务完成了两次，那程序还会进入下一个阶段
	*/
	MapCompleteNum    int
	ReduceCompleteNum int

	// MapTaskStatus    int32
	// ReduceTaskStatus int32
	// MapTaskMask      int32
	// ReduceTaskMask   int32

	lock   sync.Mutex
	stageLock sync.RWMutex

	//如果在两个地方都设置接受doneCh信号，那么就需要输入2个message，因为一个接收者消费一个
	doneCh chan bool
}

var _ MapReduceService = (*Coordinator)(nil)

type TaskInfo struct {
	TaskID     int32
	InputPath  string
	TaskType   int32
	Status     int32 //0 idle,1=processing,2=finished
	Executor   string
	Completor  string
	StartTime  time.Time
	NotifyTime time.Time
}

type WorkerInfo struct {
	WorkerID          string
	TaskID            int32
	LastHeartBeatTime time.Time
	TaskType          int32
}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) PollTask(args *PollTaskRequest, reply *PollTaskResponse) error {
	log.Printf("--PollTask-- args.WorkerID=%v", args.WorkerID)
	now := time.Now()
	c.lock.Lock()

	//一个值会被修改，那么它在另一个地方被读的时候也会产生Data race
	//所以给stageLock单独加了一个读写锁
	//现在解锁的顺序是不对的，可以运行吗[answer] 可以正常运行
	c.stageLock.RLock()
	defer c.stageLock.RUnlock()
	if c.stage == STAGE_COMPLETE {
		c.lock.Unlock()
		return fmt.Errorf(ERROR_ALL_TASK_COMPLETE)
	}

	if _, ok := c.Workers[args.WorkerID]; !ok {
		c.lock.Unlock()
		return fmt.Errorf(ERROR_NO_SUCH_WORKER)
	}


	worker := c.Workers[args.WorkerID]

	var idleTask *TaskInfo
	if c.stage == STAGE_MAP {
		for idx, task := range c.MapTaskInfos {
			log.Printf("Map:id= %v ,task Status is %v\n",idx,task.Status)
			if task.Status == TASK_STATUS_IDLE {
				idleTask = &c.MapTaskInfos[idx]
				break
			}
		}
	} else {
		for idx, task := range c.ReduceTaskInfos {
			log.Printf("Reduce: id = %v,task Status is %v\n",idx,task.Status)
			if task.Status == TASK_STATUS_IDLE {
				idleTask = &c.ReduceTaskInfos[idx]
				break
			}
		}
	}

	if idleTask == nil {
		c.lock.Unlock()
		return fmt.Errorf(ERROR_NO_IDLE_TASK)
	}

	idleTask.Status = TASK_STATUS_PROCESSING
	idleTask.StartTime = now
	idleTask.Executor = args.WorkerID
	worker.TaskID = idleTask.TaskID
	worker.TaskType = idleTask.TaskType
	c.lock.Unlock()

	//PollTaskResponse := &PollTaskResponse{}
	reply.TaskID = idleTask.TaskID
	reply.InputPath = idleTask.InputPath
	reply.TaskType = idleTask.TaskType
	//reply = PollTaskResponse
	reply.NReduce = int32(len(c.ReduceTaskInfos))
	return nil
}

func (c *Coordinator) TryCompleteTask(args *TryCompleteTaskRequest, reply *TryCompleteTaskResponse) error {
	log.Println("--TryCompleteTask %v from %v--", args.TaskID, args.WorkerID)
	c.lock.Lock()
	defer c.lock.Unlock()

	var task *TaskInfo
	if args.TaskType == TASK_TYPE_MAP {
		task = &c.MapTaskInfos[args.TaskID]
	} else {
		task = &c.ReduceTaskInfos[args.TaskID]
	}
	if task.Status == TASK_STATUS_COMPETED {
		return fmt.Errorf(ERROR_TASK_ALREADY_COMPLETED)
	}

	if len(task.Completor) != 0 && task.Completor != args.WorkerID {
		return fmt.Errorf(ERROR_TASK_IS_COMPLETING_BY_OTHERS)
	}
	task.NotifyTime = time.Now()
	task.Completor = args.WorkerID
	return nil
}

func (c *Coordinator) ConfirmCompleteTask(args *ConfirmCompleteTaskRequest, reply *ConfirmCompleteTaskResponse) error {
	log.Printf("--ConfirmCompleteTask from %v taskType=%v",args.WorkerID,args.TaskType)
	var task *TaskInfo
	c.lock.Lock()
	defer c.lock.Unlock()

	if args.TaskType == TASK_TYPE_MAP {
		task = &c.MapTaskInfos[args.TaskID]
	} else {
		task = &c.ReduceTaskInfos[args.TaskID]
	}

	if task.Status == TASK_STATUS_COMPETED {
		return fmt.Errorf(ERROR_TASK_ALREADY_COMPLETED)
	}

	if len(task.Completor) != 0 && task.Completor != args.WorkerID {
		return fmt.Errorf(ERROR_SHOULD_NOT_HAPPEN) //其实可能发生，这点要考虑启动
	}

	task.Status = TASK_STATUS_COMPETED
	if args.TaskType == TASK_TYPE_MAP {
		c.MapCompleteNum++

		if c.MapCompleteNum == len(c.MapTaskInfos) {
			//在持有读锁的情况下加写锁
			c.stageLock.Lock()
			c.stage = STAGE_REDUCE
			c.stageLock.Unlock()
		}
	} else {
		c.ReduceCompleteNum++
		log.Printf("ReduceCompleteNum=%v",c.ReduceCompleteNum)
		if c.ReduceCompleteNum == len(c.ReduceTaskInfos) {
			c.stageLock.Lock()
			c.stage = STAGE_COMPLETE
			c.stageLock.Unlock()


			log.Println("All Reduce task finished, exit after 3 seconds")
			go func() {
				time.Sleep(3 * time.Second)
				log.Println("exit now")
				c.doneCh <- true
			}()
		}
	}
	return nil
}

func (c *Coordinator) HearBeat(args *HeartBeatRequest, reply *HeartBeatResponse) error {
	log.Printf("--HearBeat from %v", args.WorkerID)
	currtime := time.Now()

	//即使是修改Map的不同value，也会判定成成Data Race，其实这个不用加的
	c.lock.Lock()
	defer c.lock.Unlock()
	worker, ok := c.Workers[args.WorkerID]
	if !ok {
		worker = &WorkerInfo{
			WorkerID: args.WorkerID,
		}
	}
	worker.LastHeartBeatTime = currtime
	c.Workers[args.WorkerID] = worker
	return nil
}

func backGroundCheck(c *Coordinator, ch chan bool) error {
	timerTicker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timerTicker.C:
			now := time.Now()
			workerFailureCheck(c, now)
			overtimeCheck(c, now)

		case <-ch:
			ch<-true
			log.Println("backGrounCheck exit")
			return nil
		}
	}
}

func workerFailureCheck(c *Coordinator, now time.Time) {
	c.lock.Lock()
	for _, worker := range c.Workers {
		if worker.LastHeartBeatTime.Add(TIME_LIMIT*time.Second).Before(now){			 
			delete(c.Workers,WorkerID)			 
		}
	}
	c.lock.Unlock()
}

func overtimeCheck(c *Coordinator, now time.Time) {
	//TODO:确认一下正常更改吗
	//不可以。task
	//var tasks []TaskInfo
	//tasks = c.MapTaskInfo
	//更改tasks[i]是完全无效的
	
	var tasks *[]TaskInfo

	c.lock.Lock()
	c.stageLock.RLock()
	if c.stage==STAGE_COMPLETE{
		c.stageLock.RUnlock()
		c.lock.Unlock()
		return
	}
	if c.stage == STAGE_MAP {
		tasks = &c.MapTaskInfos
	} else if c.stage == STAGE_REDUCE {
		tasks = &c.ReduceTaskInfos
	}
	c.stageLock.RUnlock()
	
	
	for idx, task := range (*tasks) {
		if task.Status != TASK_STATUS_COMPETED && task.Status != TASK_STATUS_IDLE{
			if  task.StartTime.Add(TIME_LIMIT * time.Second).Before(now) {
				log.Printf("type %v task %v is overtime", task.TaskType, task.TaskID)
				(*tasks)[idx].Status = TASK_STATUS_IDLE			
			}
		}

		if task.Status == TASK_STATUS_COMPLETING {
			if task.NotifyTime.Add(3 * time.Second).Before(now) {
				(*tasks)[idx].Status = TASK_STATUS_PROCESSING
				(*tasks)[idx].Completor = ""
				//task.Executor executor不用管
			}
		}
	}
	c.lock.Unlock()
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
	// Your code here.

	select {
	case <-c.doneCh:
		log.Println("receive doneCh!!!!")
		return true
	default:
		return false
	}

	//return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	//TODO:捋一下每个函数的逻辑，开始调试

	// Your code here.
	//TODO:改成文件名允许使用Prefix的格式
	var mapTasks []TaskInfo
	for idx, fileName := range files {
		mapTasks = append(mapTasks, TaskInfo{
			TaskID:    int32(idx),
			TaskType:  TASK_TYPE_MAP,
			InputPath: fileName,
			Completor: "",
		})
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTaskInfos = append(c.ReduceTaskInfos, TaskInfo{
			TaskID:   int32(i),
			TaskType: TASK_TYPE_REDUCE,
		})
	}

	c.lock = sync.Mutex{}
	c.stage = 0
	c.MapTaskInfos = mapTasks
	c.Workers = make(map[string]*WorkerInfo)
	c.doneCh = make(chan bool)
	c.stageLock = sync.RWMutex{}

	//初始化完成
	c.stage++
	go backGroundCheck(&c,c.doneCh)
	c.server()
	return &c
}
