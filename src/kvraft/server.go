package kvraft

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType int //0,1,2分别代表Get,Put,Append
	Key    string
	Value  string

	ClientID  string
	RequestID string
}

type KVServer struct {
	mu      sync.Mutex
	me      int //√
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big //√

	// Your definitions here.
	disk map[string]DiskValue //√

	commitApplier commitApplier

	lastestInfoLock deadlock.RWMutex
	maxNums         map[string]int    //√
	lastestResp     map[string]string //√
}

type DiskValue struct {
	//lock  deadlock.RWMutex
	Value     string
	VersionID int
}

// 既然说用一个大锁，就用一个大锁
// func(dv *DiskValue)setValue(value string,versionID int) {
// 	dv.lock.Lock()
// 	defer dv.lock.Unlock()

// 	dv.value = value
// 	dv.versionID = versionID
// }

// func(dv *DiskValue)getValue()string {
// 	dv.lock.RLock()
// 	defer dv.lock.RUnlock()
// 	return dv.value
// }

// func(dv *DiskValue)getVersionID()int {
// 	dv.lock.RLock()
// 	defer dv.lock.RUnlock()
// 	return dv.versionID
// }

const OP_TYPE_GET = 0
const OP_TYPE_PUT = 1
const OP_TYPE_APPEND = 2

const COMMIT_TIMEOUT = 8

const ERR_COMMIT_TIMEOUT = "ERR:commit timeout"
const ERR_NOT_LEADER = "ERR:I'm not leader"
const ERR_COMMIT_FAIL = "ERR:commit fail, please retry"

//const ERR_REQUEST_HAS_ALREADY_EXECUTED="ERR:request has already executed"

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	lastRequestNum, lastResponse := kv.getLastestInfo(args.ClientID)
	requestNum := requestIDToRequestNum(args.RequestID)
	if requestNum <= lastRequestNum {
		DPrintf(raft.DServer, "P%vs%v requestID=%v has executed,lastRequestID=%v", kv.me, kv.rf.WhoAmI(), args.RequestID, lastRequestNum)
		reply.Value = lastResponse
		return
	}

	cmd := Op{
		OpType:    OP_TYPE_GET,
		Key:       args.Key,
		RequestID: args.RequestID,
		ClientID:  args.ClientID,
	}
	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ERR_NOT_LEADER
		return
	}
	DPrintf(raft.DServer, "P%vs%v requestID=%v Get k[%v] logIdx=%v", kv.me, kv.rf.WhoAmI(), args.RequestID, args.Key, logIndex)

	var op Op
	waitChan := kv.commitApplier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case op = <-waitChan:
	case <-timeOut.C:
		//TODO：我觉得waitChan应该想办法回收掉，不然会占用空间把
		reply.Err = ERR_COMMIT_TIMEOUT
		DPrintf(raft.DServer, "P%vs%v RequestID=%v Timeout", kv.me, kv.rf.WhoAmI(), args.RequestID)
		return
	}

	if op.ClientID != args.ClientID || op.RequestID != args.RequestID {
		reply.Err = ERR_COMMIT_FAIL
		DPrintf(raft.DServer, "P%vs%v RequestID=%v CMTfail", kv.me, kv.rf.WhoAmI(), args.RequestID)
		return
	}
	reply.Value = op.Value
	DPrintf(raft.DServer, "P%vs%v RequestID=%v reply.Value=%v", kv.me, kv.rf.WhoAmI(), args.RequestID, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	lastRequestNum, _ := kv.getLastestInfo(args.ClientID)
	if requestIDToRequestNum(args.RequestID) <= lastRequestNum {
		DPrintf(raft.DServer, "P%vs%v requestID=%v has executed,lastRequestNum=%v", kv.me, kv.rf.WhoAmI(), args.RequestID, lastRequestNum)
		return
	}

	var opType int
	switch args.Op {
	case "Put":
		opType = 1
	case "Append":
		opType = 2
	}
	cmd := Op{
		OpType:    opType,
		Key:       args.Key,
		Value:     args.Value,
		RequestID: args.RequestID,
		ClientID:  args.ClientID,
	}

	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ERR_NOT_LEADER
		return
	}
	DPrintf(raft.DServer, "P%vs%v requestID=%v %v k[%v](%v)  logIdx=%v", kv.me, kv.rf.WhoAmI(), args.RequestID, args.Op, args.Key, args.Value, logIndex)

	var op Op
	waitChan := kv.commitApplier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case op = <-waitChan:
	case <-timeOut.C:
		//TODO：我觉得waitChan应该想办法回收掉，不然会占用空间把
		reply.Err = ERR_COMMIT_TIMEOUT
		DPrintf(raft.DServer, "P%vs%v requestID=%v %v k[%v]v[%v] logIdx=%v Timeout", kv.me, kv.rf.WhoAmI(), args.RequestID, args.Op, args.Key, args.Value, logIndex)
		return
	}

	if op.ClientID != args.ClientID || op.RequestID != args.RequestID {
		reply.Err = ERR_COMMIT_FAIL
		DPrintf(raft.DServer, "P%vs%v requestID=%v %v k[%v]v[%v] logIdx=%v CMT fail", kv.me, kv.rf.WhoAmI(), args.RequestID, args.Op, args.Key, args.Value, logIndex)
		return
	}
}

func applyOp(kv *KVServer, op Op, logIndex int) string {

	resp := ""
	valueWrapper, ok := kv.disk[op.Key]
	if ok && valueWrapper.VersionID >= logIndex {
		return ""
	}

	switch op.OpType {
	case OP_TYPE_GET:
		if ok {
			resp = valueWrapper.Value
			DPrintf(raft.DInfo, "P%vs%v requestID=%v logIndex=%v get[%v]->(%v)", kv.me, kv.rf.WhoAmI(), op.RequestID, logIndex, op.Key, resp)
		} else {
			resp = ""
			DPrintf(raft.DInfo, "P%vs%v requestID=%v logIndex=%v get[%v] no this key", kv.me, kv.rf.WhoAmI(), op.RequestID, logIndex, op.Key)
		}

	case OP_TYPE_PUT:
		kv.disk[op.Key] = DiskValue{
			Value:     op.Value,
			VersionID: logIndex,
		}
		DPrintf(raft.DInfo, "P%vs%v requestID=%v logIndex=%v put[%v]->(%v)", kv.me, kv.rf.WhoAmI(), op.RequestID, logIndex, op.Key, op.Value)
		resp = op.Value
	case OP_TYPE_APPEND:
		oldValue := ""
		if ok {
			oldValue = valueWrapper.Value
		} else {
			valueWrapper = DiskValue{}
		}
		valueWrapper.Value = oldValue + op.Value
		valueWrapper.VersionID = logIndex
		kv.disk[op.Key] = valueWrapper
		resp = valueWrapper.Value
		DPrintf(raft.DInfo, "P%vs%v requestID=%v logIndex=%v append[%v](%v)->(%v) ", kv.me, kv.rf.WhoAmI(), op.RequestID, logIndex, op.Key, op.Value, valueWrapper.Value)
	}
	kv.setLastestInfo(op.ClientID, requestIDToRequestNum(op.RequestID), resp)

	return resp
}

func (kv *KVServer) setLastestInfo(clientID string, requestNum int, response string) {
	kv.lastestInfoLock.Lock()
	defer kv.lastestInfoLock.Unlock()
	kv.maxNums[clientID] = requestNum
	kv.lastestResp[clientID] = response
}

func (kv *KVServer) getLastestInfo(clientID string) (int, string) {
	kv.lastestInfoLock.RLock()
	defer kv.lastestInfoLock.RUnlock()
	return kv.maxNums[clientID], kv.lastestResp[clientID]
}

func (kv *KVServer) makeSnapshot(logIndex int) {
	kv.mu.Lock()
	kv.lastestInfoLock.Lock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.me)
	e.Encode(kv.disk)
	e.Encode(kv.maxNums)
	e.Encode(kv.lastestResp)

	data := w.Bytes()

	//rf.persister.SaveRaftState(data)
	kv.rf.Snapshot(logIndex, data)

	kv.mu.Unlock()
	kv.lastestInfoLock.Unlock()

	DPrintf(raft.DServer, "P%vs%v logIndex=%v,snapshotSize=%v", kv.me, kv.rf.WhoAmI(), logIndex, len(data))

}

func (kv *KVServer) applySnapshot(logIndex int, data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		panic("nil snapshot")
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var me int
	var disk map[string]DiskValue
	var maxNums map[string]int
	var lastestResp map[string]string
	if d.Decode(&me) != nil ||
		d.Decode(&disk) != nil ||
		d.Decode(&maxNums) != nil ||
		d.Decode(&lastestResp) != nil {
		DPrintf(raft.DServer, "P%v decode error", kv.me)
	} else {
		kv.mu.Lock()
		kv.lastestInfoLock.Lock()

		kv.me = me
		kv.disk = disk
		kv.maxNums = maxNums
		kv.lastestResp = lastestResp
		//rf.lastApplied = lastApplied
		kv.mu.Unlock()
		kv.lastestInfoLock.Unlock()
		DPrintf(raft.DServer, "P%v logIndex=%v,snapshot applied", kv.me, logIndex)
		DPrintf(raft.DServer, "P%v me=%v,len(maxNums)=%v len(disk)=%v", kv.me, me, len(maxNums), len(disk))

	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.commitApplier.stop()
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(DiskValue{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)

	kv.commitApplier = NewCommitApplier(kv.applyCh, kv, kv.maxraftstate)
	kv.maxNums = make(map[string]int)
	kv.lastestResp = make(map[string]string)
	kv.disk = make(map[string]DiskValue)
	kv.lastestInfoLock = deadlock.RWMutex{}
	kv.commitApplier.start()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	log.Printf("An Kvserver made,len(server)=%v,me=%v rf.me=%v\n", len(servers), me, kv.rf.WhoAmI())
	return kv
}

type commitApplier struct {
	waiters map[int]chan Op
	lock    deadlock.RWMutex

	applyCh chan raft.ApplyMsg

	kvserver *KVServer

	appliedOpCount int
	maxraftstate   int

	appliedSnapshotLogIndex int

	exitCh chan interface{} //[TODO]只关注信号，不关注信号内容的chan咋写呢？
}

// [TODO] chan本身就是个指针，按说不加*号完全没有毛病
func NewCommitApplier(applyCh chan raft.ApplyMsg, kvserver *KVServer, maxraftstate int) commitApplier {
	return commitApplier{
		applyCh:      applyCh,
		exitCh:       make(chan interface{}),
		waiters:      make(map[int]chan Op),
		kvserver:     kvserver,
		maxraftstate: maxraftstate,
	}
}

func (ca *commitApplier) start() {
	go func() {
		for {
			select {
			case <-ca.exitCh:
				DPrintf(raft.DServer, "P%vs%v commitApplier 关闭", ca.kvserver.me, ca.kvserver.rf.WhoAmI())
				return
			default:
			}

			select {
			case applyMsg := <-ca.applyCh:
				if applyMsg.CommandValid {
					DPrintf(raft.DServer, "P%vs%v new Command logIndex=%v", ca.kvserver.me, ca.kvserver.rf.WhoAmI(), applyMsg.CommandIndex)
					notifyChan := ca.getWaiter(applyMsg.CommandIndex)
					op, _ := applyMsg.Command.(Op)

					if dealWithOp(ca.kvserver, applyMsg, &op) {
						ca.addAppliedOpCount()

						if ca.maxraftstate != -1 && ca.getAppliedOpCount() >= ca.maxraftstate {
							//TODO:make snapshot
							//ca.appliedOpCount=0
							ca.kvserver.makeSnapshot(applyMsg.CommandIndex)
							DPrintf(raft.DServer, "P%vs%v logIndex=%v,make snapshot", ca.kvserver.me, ca.kvserver.rf.WhoAmI(), applyMsg.CommandIndex)
							ca.appliedOpCount = 0
						}
					}

					if notifyChan != nil {
						notifyChan <- op
					}
				}

				if applyMsg.SnapshotValid {
					if applyMsg.SnapshotIndex > ca.appliedSnapshotLogIndex {
						ca.appliedSnapshotLogIndex = applyMsg.SnapshotIndex
						ca.kvserver.applySnapshot(applyMsg.SnapshotIndex, applyMsg.Snapshot)
					}
				}
			case <-ca.exitCh:
				DPrintf(raft.DServer, "P%vs%v commitApplier 关闭", ca.kvserver.me, ca.kvserver.rf.WhoAmI())
				return
			}
		}
	}()
}

func dealWithOp(kv *KVServer, applyMsg raft.ApplyMsg, op *Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	maxRqNum, _ := kv.getLastestInfo(op.ClientID)
	if maxRqNum < requestIDToRequestNum(op.RequestID) || op.OpType == OP_TYPE_GET {
		DPrintf(raft.DServer, "P%vs%v maxRequestNum=%v,currentRequestNum=%v", kv.me, kv.rf.WhoAmI(), maxRqNum, requestIDToRequestNum(op.RequestID))
		op.Value = applyOp(kv, *op, applyMsg.CommandIndex)
		return true
	}
	return false
}

func (ca *commitApplier) getAppliedOpCount() int {
	return ca.appliedOpCount
}
func (ca *commitApplier) addAppliedOpCount() {
	ca.appliedOpCount += 20
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
