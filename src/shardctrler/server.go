package shardctrler

import (
	"bytes"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/sasha-s/go-deadlock"
)

const Debug = true

func DPrintf(topic raft.LogTopic, format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	} else {
		raft.MyDebug(topic, format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	//gids          []int //最新配置中所有的gids的ID，按照添加事件排序
	commitApplier commitApplier
	//gidToShards   map[int][]int

	configs []Config // indexed by config num √

	lastestInfoLock deadlock.RWMutex
	maxNums         map[string]int    //√
	lastestResp     map[string]Config //√
}

type Op struct {
	// Your data here.

	OpType int
	OpArgs interface{}
	Config Config

	ClientID  string
	RequestID string
}

const ERR_NOT_LEADER = "Im not leader"
const ERR_COMMIT_FAIL = "Commit Fail"
const ERR_COMMIT_TIMEOUT = "commit timeout"

const COMMIT_TIMEOUT = 4

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	lastRequestNum, _ := sc.getLastestInfo(args.ClientID)
	requestNum := requestIDToRequestNum(args.RequestID)
	if requestNum <= lastRequestNum {
		//DPrintf(raft.DServer, "ShardCtrl P%d requestID=%v has executed,lastRequestID=%v", sc.me, args.RequestID, lastRequestNum)
		return
	}

	cmd := Op{
		OpType:    OP_TYPE_JOIN,
		OpArgs:    *args,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	logIndex, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ERR_NOT_LEADER
		return
	}
	//DPrintf(raft.DServer, "ShardCtrl P%d requestID=%v join logIndex=%v", sc.me, args.RequestID, logIndex)

	var op Op
	waitChan := sc.commitApplier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case op = <-waitChan:
	case <-timeOut.C:
		//TODO：我觉得waitChan应该想办法回收掉，不然会占用空间把
		reply.Err = ERR_COMMIT_TIMEOUT
		DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v Timeout", sc.me, args.RequestID)
		return
	}

	opArgs := op.OpArgs.(JoinArgs)

	if opArgs.ClientID != args.ClientID || opArgs.RequestID != args.RequestID {
		reply.Err = ERR_COMMIT_FAIL
		DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v CMTfail", sc.me, args.RequestID)
		return
	}
	var servers []int
	for k, _ := range opArgs.Servers {
		servers = append(servers, k)
	}
	DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v join gid[%v] success", sc.me, args.RequestID, servers)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	lastRequestNum, _ := sc.getLastestInfo(args.ClientID)
	requestNum := requestIDToRequestNum(args.RequestID)
	if requestNum <= lastRequestNum {
		//DPrintf(raft.DServer, "ShardCtrl P%d requestID=%v has executed,lastRequestID=%v", sc.me, args.RequestID, lastRequestNum)
		return
	}

	cmd := Op{
		OpType:    OP_TYPE_LEAVE,
		OpArgs:    *args,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	logIndex, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ERR_NOT_LEADER
		return
	}
	//DPrintf(raft.DServer, "ShardCtrl P%d requestID=%v leave logIndex=%v", sc.me, args.RequestID, logIndex)

	var op Op
	waitChan := sc.commitApplier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case op = <-waitChan:
	case <-timeOut.C:
		//TODO：我觉得waitChan应该想办法回收掉，不然会占用空间把
		reply.Err = ERR_COMMIT_TIMEOUT
		DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v Timeout", sc.me, args.RequestID)
		return
	}

	opArgs := op.OpArgs.(LeaveArgs)

	if opArgs.ClientID != args.ClientID || opArgs.RequestID != args.RequestID {
		reply.Err = ERR_COMMIT_FAIL
		DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v CMTfail", sc.me, args.RequestID)
		return
	}

	DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v gids=[%v]leave success", sc.me, args.RequestID, opArgs.GIDs)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	lastRequestNum, _ := sc.getLastestInfo(args.ClientID)
	requestNum := requestIDToRequestNum(args.RequestID)
	if requestNum <= lastRequestNum {
		DPrintf(raft.DServer, "ShardCtrl P%d requestID=%v has executed,lastRequestID=%v", sc.me, args.RequestID, lastRequestNum)
		return
	}

	cmd := Op{
		OpType:    OP_TYPE_MOVE,
		OpArgs:    *args,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	logIndex, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ERR_NOT_LEADER
		return
	}
	//DPrintf(raft.DServer, "ShardCtrl P%d requestID=%v move(%v)->Group{%v} logIndex=%v", sc.me, args.RequestID, args.Shard, args.GID, logIndex)

	var op Op
	waitChan := sc.commitApplier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case op = <-waitChan:
	case <-timeOut.C:
		//TODO：我觉得waitChan应该想办法回收掉，不然会占用空间把
		reply.Err = ERR_COMMIT_TIMEOUT
		DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v Timeout", sc.me, args.RequestID)
		return
	}

	opArgs := op.OpArgs.(MoveArgs)

	if opArgs.ClientID != args.ClientID || opArgs.RequestID != args.RequestID {
		reply.Err = ERR_COMMIT_FAIL
		DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v CMTfail", sc.me, args.RequestID)
		return
	}
	DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v move(%v)->Group{%v} success", sc.me, args.RequestID, opArgs.Shard, opArgs.GID)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	lastRequestNum, lastestResp := sc.getLastestInfo(args.ClientID)
	requestNum := requestIDToRequestNum(args.RequestID)
	if requestNum <= lastRequestNum {
		reply.Config = lastestResp
		DPrintf(raft.DServer, "ShardCtrl P%d requestID=%v has executed,lastRequestID=%v", sc.me, args.RequestID, lastRequestNum)
		return
	}

	cmd := Op{
		OpType:    OP_TYPE_QUERY,
		OpArgs:    *args,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}
	logIndex, _, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ERR_NOT_LEADER
		return
	}
	//DPrintf(raft.DServer, "ShardCtrl P%d requestID=%v Query logIndex=%v", sc.me, args.RequestID, logIndex)

	var op Op
	waitChan := sc.commitApplier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case op = <-waitChan:
	case <-timeOut.C:
		//TODO：我觉得waitChan应该想办法回收掉，不然会占用空间把
		reply.Err = ERR_COMMIT_TIMEOUT
		//DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v Timeout", sc.me, args.RequestID)
		return
	}

	opArgs := op.OpArgs.(QueryArgs)

	if opArgs.ClientID != args.ClientID || opArgs.RequestID != args.RequestID {
		reply.Err = ERR_COMMIT_FAIL
		//DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v CMTfail", sc.me, args.RequestID)
		return
	}
	reply.Config = op.Config
	//DPrintf(raft.DServer, "ShardCtrl P%d RequestID=%v Query success", sc.me, args.RequestID)
}

func (sc *ShardCtrler) makeSnapshot(logIndex int) {
	sc.mu.Lock()
	sc.lastestInfoLock.Lock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(sc.me)
	e.Encode(sc.configs)
	e.Encode(sc.maxNums)
	e.Encode(sc.lastestResp)

	data := w.Bytes()

	//rf.persister.SaveRaftState(data)
	sc.rf.Snapshot(logIndex, data)

	sc.mu.Unlock()
	sc.lastestInfoLock.Unlock()

	//DPrintf(raft.DServer, "ShardCtrl S%d logIndex=%v,snapshotSize=%v", sc.me, logIndex, len(data))

}

func (sc *ShardCtrler) applySnapshot(logIndex int, data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		panic("nil snapshot")
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var me int
	var configs []Config
	var maxNums map[string]int
	var lastestResp map[string]Config
	if d.Decode(&me) != nil ||
		d.Decode(&configs) != nil ||
		d.Decode(&maxNums) != nil ||
		d.Decode(&lastestResp) != nil {
		DPrintf(raft.DServer, "ShardCtrl S%d decode error", sc.me)
	} else {
		sc.mu.Lock()
		sc.lastestInfoLock.Lock()

		sc.me = me
		sc.configs = configs
		sc.maxNums = maxNums
		sc.lastestResp = lastestResp
		//rf.lastApplied = lastApplied
		sc.mu.Unlock()
		sc.lastestInfoLock.Unlock()
		//DPrintf(raft.DServer, "S%d logIndex=%v,snapshot applied", sc.me, logIndex)
		//DPrintf(raft.DServer, "S%d me=%v,len(maxNums)=%v", sc.me, me, len(maxNums))

	}

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.commitApplier.stop()

	sc.rf.Kill()

	// Your code here, if desired.
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Config{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)

	sc.commitApplier = NewCommitApplier(sc.applyCh, sc, -1)
	sc.maxNums = make(map[string]int)
	sc.lastestResp = make(map[string]Config)
	sc.lastestInfoLock = deadlock.RWMutex{}
	sc.commitApplier.start()
	sc.rf = raft.Make(servers, me, persister, sc.applyCh, "ShardCtrl",0)

	// Your code here.

	return sc
}

type commitApplier struct {
	waiters map[int]chan Op
	lock    deadlock.RWMutex

	applyCh chan raft.ApplyMsg

	scserver *ShardCtrler

	appliedOpCount int
	maxraftstate   int

	exitCh chan interface{} //[TODO]只关注信号，不关注信号内容的chan咋写呢？
}

// [TODO] chan本身就是个指针，按说不加*号完全没有毛病
func NewCommitApplier(applyCh chan raft.ApplyMsg, scserver *ShardCtrler, maxraftstate int) commitApplier {
	return commitApplier{
		applyCh:      applyCh,
		exitCh:       make(chan interface{}),
		waiters:      make(map[int]chan Op),
		scserver:     scserver,
		maxraftstate: maxraftstate,
	}
}

func (ca *commitApplier) start() {
	go func() {
		for {
			select {
			case <-ca.exitCh:
				return
			default:
			}

			select {
			case applyMsg := <-ca.applyCh:
				if applyMsg.CommandValid {

					notifyChan := ca.getWaiter(applyMsg.CommandIndex)
					//DPrintf(raft.DServer, "ShardCtrl S%d StateMachine receive logIndex=%v", ca.scserver.me, applyMsg.CommandIndex)

					op, ok := applyMsg.Command.(Op)
					if ok {
						maxRqNum, _ := ca.scserver.getLastestInfo(op.ClientID)
						if maxRqNum < requestIDToRequestNum(op.RequestID) || op.OpType == OP_TYPE_QUERY {
							op.Config = applyOp(ca.scserver, op, applyMsg.CommandIndex)
							ca.appliedOpCount += 20

							if ca.maxraftstate != -1 && ca.appliedOpCount >= ca.maxraftstate {
								//TODO:make snapshot
								//ca.appliedOpCount=0
								ca.scserver.makeSnapshot(applyMsg.CommandIndex)
								//DPrintf(raft.DServer, "S%d logIndex=%v,make snapshot", ca.scserver.me, applyMsg.CommandIndex)
								ca.appliedOpCount = 0
							}
						}

						if notifyChan != nil {
							notifyChan <- op
						}
					}

				}

				if applyMsg.SnapshotValid {
					ca.scserver.applySnapshot(applyMsg.SnapshotIndex, applyMsg.Snapshot)
				}
			case <-ca.exitCh:
				return
			}
		}
	}()
}

func (ca *commitApplier) stop() {
	close(ca.exitCh)
}

// 看样不太支持结构体方法在自身被调用
func (ca *commitApplier) getWaiter(logIndex int) chan Op {
	ca.lock.RLock()
	defer ca.lock.RUnlock()

	return ca.waiters[logIndex]
}

func (ca *commitApplier) addWaiter(logIndex int) chan Op {
	ca.lock.Lock()
	defer ca.lock.Unlock()

	notifyChan := make(chan Op, 1)
	ca.waiters[logIndex] = notifyChan
	return notifyChan
}

// func (ca *commitApplier) getAppliedLogIndex() int {
// 	return int(atomic.LoadInt32(&ca.lastLogIndex))
// }

// func (ca *commitApplier) setAppliedLogIndex(logIndex int) {
// 	atomic.StoreInt32(&ca.lastLogIndex, int32(logIndex))
// }

func requestIDToRequestNum(requestID string) int {
	resp, _ := strconv.Atoi(requestID[12:])
	return resp
}

const OP_TYPE_JOIN = 1
const OP_TYPE_LEAVE = 2
const OP_TYPE_MOVE = 3
const OP_TYPE_QUERY = 4

func applyOp(sc *ShardCtrler, op Op, logIndex int) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	var resp Config
	switch op.OpType {
	case OP_TYPE_JOIN:
		args, ok := op.OpArgs.(JoinArgs)
		if !ok {
			panic("wrong join Args ")
		}

		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := deepCopyConfig(lastConfig)
		for k, v := range args.Servers {
			newConfig.Groups[k] = v
		}
		newConfig = getNewConfig(newConfig, sc.me)
		sc.configs = append(sc.configs, newConfig)
		//DPrintf(raft.DInfo, "P%v requestID=%v logIndex=%v Join success", sc.me, op.RequestID, logIndex)

	case OP_TYPE_LEAVE:
		args, ok := op.OpArgs.(LeaveArgs)
		if !ok {
			panic("wrong LeaveArgs ")
		}

		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := deepCopyConfig(lastConfig)
		for _, gid := range args.GIDs {
			delete(newConfig.Groups, gid)
			for i := 0; i < NShards; i++ {
				if newConfig.Shards[i] == gid {
					newConfig.Shards[i] = 0
				}
			}
		}

		newConfig = getNewConfig(newConfig, sc.me)
		sc.configs = append(sc.configs, newConfig)
		//DPrintf(raft.DInfo, "P%v requestID=%v logIndex=%v Leave success", sc.me, op.RequestID, logIndex)

	case OP_TYPE_MOVE:
		args, ok := op.OpArgs.(MoveArgs)
		if !ok {
			panic("wrong MoveArgs ")
		}

		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig := deepCopyConfig(lastConfig)

		newConfig.Shards[args.Shard] = args.GID
		newConfig.Num++

		sc.configs = append(sc.configs, newConfig)
		//DPrintf(raft.DInfo, "P%v requestID=%v logIndex=%v move success", sc.me, op.RequestID, logIndex)
	case OP_TYPE_QUERY:
		args, ok := op.OpArgs.(QueryArgs)
		if !ok {
			panic("wrong QueryArgs ")
		}

		if args.Num == -1 {
			resp = deepCopyConfig(sc.configs[len(sc.configs)-1])
		} else {
			resp = deepCopyConfig(sc.configs[args.Num])
		}
		//DPrintf(raft.DInfo, "P%v requestID=%v logIndex=%v query num=%v len(sc.configs)=%v success", sc.me, op.RequestID, logIndex, args.Num, len(sc.configs))
	}
	sc.setLastestInfo(op.ClientID, requestIDToRequestNum(op.RequestID), resp)

	return resp
}

func getExpectShardsNum(unAllocateShardsNum int, unAllocateGidNum int) int {
	if unAllocateShardsNum%unAllocateGidNum == 0 {
		return unAllocateShardsNum / unAllocateGidNum
	} else {
		return unAllocateShardsNum/unAllocateGidNum + 1
	}
}

func getNewConfig(config Config, me int) Config {
	//这里的config应该是一个深拷贝
	config.Num++

	gidTimesMap := make(map[int]int, len(config.Groups))
	for _, gid := range config.Shards {
		times, ok := gidTimesMap[gid]
		if ok {
			times++
			gidTimesMap[gid] = times
		} else {
			gidTimesMap[gid] = 1
		}
	}

	gids := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}

	quickSort(gids, gidTimesMap, 0, len(gids)-1)

	shardsLeft := NShards
	replicaGroupLeft := len(gids)
	if replicaGroupLeft == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
	} else {
		for _, gid := range gids {
			expectNum := getExpectShardsNum(shardsLeft, replicaGroupLeft)
			have := gidTimesMap[gid]
			//DPrintf(raft.DInfo, "P%v gid=%v,shardLeft=%v replicaGroupLeft=%v", me, gid, shardsLeft, replicaGroupLeft)
			//DPrintf(raft.DInfo, "P%v gid=%v,have=%v expectNum=%v", me, gid, have, expectNum)

			if expectNum > have {
				transferNum := expectNum - have
				for i := 0; i < NShards; i++ {
					if config.Shards[i] == 0 {
						config.Shards[i] = gid
						//DPrintf(raft.DInfo, "P%v shard(%v)->Gid(%v)", me, i, gid)
						transferNum--
						if transferNum == 0 {
							break
						}
					}
				}
			} else if expectNum < have {
				transferNum := have - expectNum
				for i := 0; i < NShards; i++ {
					if config.Shards[i] == gid {
						config.Shards[i] = 0
						//DPrintf(raft.DInfo, "P%v shard(%v)->0", me, i)
						transferNum--
						if transferNum == 0 {
							break
						}
					}
				}
			}
			replicaGroupLeft--
			shardsLeft -= expectNum
		}
	}

	return config
}

func (sc *ShardCtrler) setLastestInfo(clientID string, requestNum int, config Config) {
	sc.lastestInfoLock.Lock()
	defer sc.lastestInfoLock.Unlock()
	sc.maxNums[clientID] = requestNum
	sc.lastestResp[clientID] = config
}

func (sc *ShardCtrler) getLastestInfo(clientID string) (int, Config) {
	sc.lastestInfoLock.RLock()
	defer sc.lastestInfoLock.RUnlock()
	return sc.maxNums[clientID], sc.lastestResp[clientID]
}

func deepCopyConfig(config Config) Config {
	newShards := config.Shards

	newGroups := make(map[int][]string)
	for k, v := range config.Groups {
		newGroups[k] = v
	}

	return Config{
		Num:    config.Num,
		Shards: newShards,
		Groups: newGroups,
	}
}

func quickSort(gids []int, gidTimesMap map[int]int, start int, end int) {
	if start >= end {
		return
	}

	sentinel := start

	left, right := start, end
	for left < right {
		for left < right && !judge(gids, gidTimesMap, right, sentinel) {
			right--
		}

		for left < right && judge(gids, gidTimesMap, left, sentinel) {
			left++
		}

		gids[left], gids[right] = gids[right], gids[left]
		//fmt.Printf("[%v,%v] left=%v ,right=%v\n", start, end, left, right)
	}

	gids[left], gids[sentinel] = gids[sentinel], gids[left]
	quickSort(gids, gidTimesMap, start, left-1)
	quickSort(gids, gidTimesMap, left+1, end)
}

// p大于q输出true，p小于q输出false
func judge(gids []int, gidTimesMap map[int]int, p int, q int) bool {
	if gidTimesMap[gids[p]] == gidTimesMap[gids[q]] {
		return gids[p] > gids[q]
	}
	return gidTimesMap[gids[p]] > gidTimesMap[gids[q]]
}

func (c *Config) String() {
	DPrintf(raft.DWarn, "num=%v", c.Num)

	var keys []int
	for k, _ := range c.Groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, gid := range keys {
		var shards []int
		for shardNo := 0; shardNo < NShards; shardNo++ {
			if c.Shards[shardNo] == gid {
				shards = append(shards, shardNo)
			}
		}
		DPrintf(raft.DWarn, "gid(%v).servers=%v shards=%v", gid, c.Groups[gid], shards)
	}
}
