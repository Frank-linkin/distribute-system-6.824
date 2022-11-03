# overall designation

the output of user's map function is a series of key-value pairs. After finishing the user's map
function, worker should allocate those key-value pairs to R slot, sort the key-value
pair for each slot, and generate a file for each slot for the usage of Reduce stage.

the Guidance advice the naming intermediate file in mr-x-y style, where x is the Map task number
and Y is reduce task number.



## coordinator's job

receive M and R as parameter, where M is number of Map tasks, N is number of Reduce tasks.

coordinator里面有m个Map task和R个Reduce task的状态，task状态包括：

1.status:任务状态是processing，idle或者finished

2.FinishedWorkerID:完成该task的workerid

coordinator don't care who is executing the task, it only concerns about Who completed it.

3.startTime:



已注册的P个Worker的状态，Worker状态包括：

1.lastHeartBeatTime: 上次心跳时间

2.workerID



一个Map,通过WorkerID来检索worker

M个maptask的bitmap，表示是否已经完成

R一个ReduceTask的bitMap,表示是否已经完成。



#### 动作

//1.RegisterWorker():接收worker注册的调用

取消worker注册函数，Heartbeat本身就是一种注册。

2.PollTask():向worker返回一个任务

3.workerHeartBeat():更新worker的心跳

4.一个backgroundCheck，检验是不是已经有worker10s没有通信，如果有，就把当前worker所复制的任务置为idle。

5.CompleteTask():修改某个任务，置为已经完成

6.一个backgroundCheck，检验是不是已经有任务执行了超过10s，如果有，就把当前任务置为idle。




## Worker's job

1.hearbeat

heartbeat的时候带着注册用信息。Worker的ip+worker的id

pollTask():向coordinator请求一个task

如果不忙，就每隔1s请求一次PollTask；如果忙，就不PollTask

2.ExecuteTask:执行这个Task
执行完user defiend Reduce task后，
向Coordinator报告完成了这个Task，CompleteTask
if CompleteTask success {
    将result写入到intermediate file中
}
if CompleteTask fail {
    放弃已有result
}





Worker会主动去Poll一个任务。

Question：

1.了解一下rpc：

​	1.如何将一个函数远程传输过去

​	done 并不是将函数传递过去，而是通过按名调用。

2.了解一下从文件从写入和读取keyvalue

3.go的plugin机制 done



rpc调用

worker主要是做client,coordinator是server

服务主要有：

- hearbeat()

- pollTask()
- CompleteTask()



肯定要抽象成两层

service层，纯纯服务| including ClusterService, WorkerService, CoordinatorService

client层，为上层提供服务

Question,


每个Map函数输出一个Intermediate file naming rule: intermediate-X-Y
X:mapTask的taskID
Y:ReduceTask再Hash中的槽