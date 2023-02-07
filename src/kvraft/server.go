package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/sasha-s/go-deadlock"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
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

	ClientID  int
	RequestID int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	disk map[string]string

	cmtNotifier commitApplier

	maxIDs      []int
	lastestResp []string
}

type diskValue struct {
	lock  deadlock.RWMutex
	value string
}

const OP_TYPE_GET = 0
const OP_TYPE_PUT = 1
const OP_TYPE_APPEND = 2
const COMMIT_TIMEOUT = 5

const ERR_COMMIT_TIMEOUT = "ERR:commit timeout"
const ERR_NOT_LEADER = "ERR:I'm not leader"
const ERR_COMMIT_FAIL = "ERR:commit fail, please retry"
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if args.RequestID == kv.maxIDs[args.ClientID] {
		reply.Value = kv.lastestResp[args.ClientID]
		return
	}

	cmd := Op{
		OpType: OP_TYPE_GET,
		Key:    args.Key,
	}
	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = "Error:not leader"
		return
	}

	var op Op
	waitChan := kv.cmtNotifier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case op = <-waitChan:
	case <-timeOut.C:
		//TODO：我觉得waitChan应该想办法回收掉，不然会占用空间把
		reply.Err = ERR_COMMIT_TIMEOUT
		return
	}

	if op.ClientID != args.ClientID || op.RequestID != args.RequestID {
		reply.Err = ERR_COMMIT_FAIL
		return  
	}


	kv.mu.Lock()
	defer kv.mu.Unlock()
	value := kv.disk[args.Key]
	
	kv.lastestResp[args.ClientID] = reply.Value
	kv.maxIDs[args.ClientID] = args.ClientID

	reply.Value = value

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if args.RequestID == kv.maxIDs[args.ClientID] {
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
		OpType: opType,
		Key:    args.Key,
		Value:  args.Value,
	}

	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = "Error:not leader"
		return
	}

	var op Op
	waitChan := kv.cmtNotifier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case op = <-waitChan:
	case <-timeOut.C:
		//TODO：我觉得waitChan应该想办法回收掉，不然会占用空间把
		reply.Err = ERR_COMMIT_TIMEOUT
		return
	}

	if op.ClientID != args.ClientID || op.RequestID != args.RequestID {
		reply.Err = ERR_COMMIT_FAIL
		return  
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	value := kv.disk[args.Key]

	kv.maxIDs[args.ClientID] = args.ClientID
	switch args.Op {
	case "Put":
		value = args.Value
	case "Append":
		value = value + args.Value
	}
	kv.disk[args.Key] = value
}

func (kv *KVServer) setMaxIDs(clientID int, requestID int) {
	kv.maxIDs[clientID] = requestID
}

func (kv *KVServer) setLatestResponse(clientID int, response string) {
	kv.lastestResp[clientID] = response
}

func (kv *KVServer) applyOp(op Op) string {
	switch op.OpType {
	case 0:
		return kv.disk[op.Key]
	case 1:
		kv.disk[op.Key] = op.Value
	case 2:
		kv.disk[op.Key] = kv.disk[op.Key] + op.Value
	}
	return ""
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.cmtNotifier = NewCommitApplier(kv.applyCh)
	kv.maxIDs = make([]int, len(servers))
	kv.lastestResp = make([]string, len(servers))
	return kv
}

type commitApplier struct {
	waiters map[int]chan Op
	lock    deadlock.RWMutex

	applyCh chan raft.ApplyMsg

	kvserver *KVServer

	exitCh chan interface{} //[TODO]只关注信号，不关注信号内容的chan咋写呢？
}

// [TODO] chan本身就是个指针，按说不加*号完全没有毛病
func NewCommitApplier(applyCh chan raft.ApplyMsg) commitApplier {
	return commitApplier{
		applyCh: applyCh,
		exitCh:  make(chan interface{}),
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
					op, _ := applyMsg.Command.(Op)

					if notifyChan != nil {
						notifyChan <- op
					}
				}

				if applyMsg.SnapshotValid {
					panic("shouldn't have snapshot")
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
