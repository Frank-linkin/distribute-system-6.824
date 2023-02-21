package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"github.com/sasha-s/go-deadlock"
)

const (
	OP_TYPE_GET    = "Get"
	OP_TYPE_PUT    = "Put"
	OP_TYPE_APPEND = "Append"

	OP_TYPE_SHARD_DELETE  = "ShardDelete"
	OP_TYPE_SHARD_STOP    = "ShardStop"
	OP_TYPE_SHARD_ADD     = "ShardAdd"
	OP_TYPE_SHARD_CONFIRM = "ShardConfirm"

	OP_TYPE_CONFIG_CHANGE = "ConfigChange"
)

const COMMIT_TIMEOUT = 4

const Debug = true

const CONFIG_UPDATE_INTERVAL = 1500 * time.Millisecond

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
	OperationValid bool
	OpType         string
	Key            string
	Value          string
	ClientID       string
	RequestID      string

	ShardID      int
	Shard        Shard
	ShardOpValid bool
	ConfigNum    int

	ConfigValid bool
	Config      shardctrler.Config

	Error string
}

type ShardKV struct {
	mu           deadlock.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shardMap         map[int]*Shard //√
	shardState       map[int]int32  //√
	maxShardLogIndex int32          //√

	sm *shardctrler.Clerk

	commitApplier commitApplier

	config     *shardctrler.Config //√
	lastConfig *shardctrler.Config //√

	killCh chan int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	reponse, err := kv.checkExecutedAndGroup(args.ClientID, args.RequestID, args.Key)
	if err != nil {
		DPrintf(raft.DServer, "G%vs%v requestID=%v check receive4", kv.gid, kv.rf.WhoAmI(), args.RequestID)
		kv.mu.Unlock()
		if err.Error() == ErrExecuted {
			reply.Err = OK
			reply.Value = reponse
			return
		}
		reply.Value = reponse
		reply.Err = Err(err.Error())
		return
	}
	kv.mu.Unlock()
	cmd := Op{
		OperationValid: true,
		OpType:         OP_TYPE_GET,
		Key:            args.Key,
		RequestID:      args.RequestID,
		ClientID:       args.ClientID,
	}
	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf(raft.DServer, "G%vP%vs%v requestID=%v Get k[%v] logIdx=%v", kv.gid, kv.me, kv.rf.WhoAmI(), args.RequestID, args.Key, logIndex)

	op, err := kv.waitForCommit(logIndex, func(op Op) bool {
		if op.OpType != OP_TYPE_GET || op.ClientID != args.ClientID || op.RequestID != args.RequestID {
			reply.Err = ErrCommitFail
			return true
		}
		return false
	})
	if err != nil {
		DPrintf(raft.DServer, "G%vP%vs%v RequestID=%v err=%v", kv.gid, kv.me, kv.rf.WhoAmI(), args.RequestID, err)
		reply.Err = Err(err.Error())
		return
	}

	if op.Error != "" {
		reply.Err = Err(op.Error)
		return
	}

	reply.Err = OK
	reply.Value = op.Value
	DPrintf(raft.DServer, "G%vP%vs%v RequestID=%v reply.Value=%v", kv.gid, kv.me, kv.rf.WhoAmI(), args.RequestID, reply.Value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	_, err := kv.checkExecutedAndGroup(args.ClientID, args.RequestID, args.Key)
	if err != nil {
		kv.mu.Unlock()
		reply.Err = Err(err.Error())
		return
	}

	cmd := Op{
		OperationValid: true,
		OpType:         args.Op,
		Key:            args.Key,
		Value:          args.Value,
		RequestID:      args.RequestID,
		ClientID:       args.ClientID,
	}

	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	DPrintf(raft.DServer, "G%vP%vs%v requestID=%v %v k[%v](%v)  logIdx=%v", kv.gid, kv.me, kv.rf.WhoAmI(), args.RequestID, args.Op, args.Key, args.Value, logIndex)

	op, err := kv.waitForCommit(logIndex, func(op Op) bool {
		if op.OpType != args.Op || op.ClientID != args.ClientID || op.RequestID != args.RequestID {
			reply.Err = ErrCommitFail
			return true
		}
		return false
	})
	if err != nil {
		DPrintf(raft.DServer, "G%vP%vs%v RequestID=%v err=%v", kv.gid, kv.me, kv.rf.WhoAmI(), args.RequestID, err)
		reply.Err = Err(err.Error())
		return
	}

	if op.Error != "" {
		reply.Err = Err(op.Error)
		return
	}
	reply.Err = OK
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	// Your code here.
	kv.mu.Lock()
	configNum := kv.config.Num
	if args.ConfigNum != kv.config.Num {
		reply.Err = ErrConfigNumMismatch
		kv.mu.Unlock()
		return
	}

	cmd := Op{
		OpType:       OP_TYPE_SHARD_STOP,
		ShardID:      args.ShardID,
		ShardOpValid: true,
		ConfigNum:    configNum,
	}
	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	waitChan := kv.commitApplier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case <-waitChan:
	case <-timeOut.C:
		//TODO：我觉得waitChan应该想办法回收掉，不然会占用空间把
		reply.Err = ErrCommitTimeout
		return
	}

	kv.mu.Lock()
	state, ok := kv.shardState[args.ShardID]

	if !ok {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	if state != SHARD_STATE_OFF {
		reply.Err = ErrCommitFail
		kv.mu.Unlock()
		return
	}

	reply.Shard = *kv.shardMap[args.ShardID]

	kv.mu.Unlock()
	reply.Err = OK
}

func (kv *ShardKV) ConfirmReceivedShard(args *ConfirmReceivedShardArgs, reply *ConfirmReceivedShardReply) {
	// Your code here.
	kv.mu.Lock()
	configNum := kv.config.Num
	if args.ConfigNum < kv.config.Num {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrConfigNumMismatch
		kv.mu.Unlock()
		return
	}

	_, ok := kv.shardState[args.ShardID]
	if !ok {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	cmd := Op{
		OpType:       OP_TYPE_SHARD_DELETE,
		ShardID:      args.ShardID,
		ShardOpValid: true,
		ConfigNum:    configNum,
	}
	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf(raft.DServer, "G%v confirming shard%v,logIndex=%v", kv.gid, args.ShardID, logIndex)

	_, err := kv.waitForCommit(logIndex, func(op Op) bool {
		if op.OpType != OP_TYPE_SHARD_DELETE {
			return true
		}
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if _, ok := kv.shardMap[args.ShardID]; ok {
			return true
		}
		return false
	})
	if err != nil {
		reply.Err = Err(err.Error())
	}
	reply.Err = OK
}

func (kv *ShardKV) addShardToStorage(shard Shard, ShardID int) error {
	// Your code here.
	kv.mu.Lock()
	configNum := kv.config.Num
	if state, ok := kv.shardState[ShardID]; ok && state >= 1 {
		kv.mu.Unlock()
		return nil
	}

	cmd := Op{
		OpType:       OP_TYPE_SHARD_ADD,
		ShardID:      ShardID,
		Shard:        shard,
		ShardOpValid: true,
		ConfigNum:    configNum,
	}
	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		kv.mu.Unlock()
		return fmt.Errorf(ErrWrongLeader)
	}
	kv.mu.Unlock()

	_, err := kv.waitForCommit(logIndex, func(op Op) bool {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if state, ok := kv.shardState[ShardID]; ok && state >= 2 {
			return false
		}
		return true
	})
	if err != nil {
		return fmt.Errorf(err.Error())
	}
	return nil
}

func (kv *ShardKV) changeShardStateToConfirmed(ShardID int) error {
	// Your code here.
	kv.mu.Lock()
	if state, ok := kv.shardState[ShardID]; ok && state == SHARD_STATE_ON_AND_CONFIRMED {
		kv.mu.Unlock()
		return nil
	}
	configNum := kv.config.Num
	kv.mu.Unlock()

	cmd := Op{
		OpType:       OP_TYPE_SHARD_CONFIRM,
		ShardID:      ShardID,
		ShardOpValid: true,
		ConfigNum:    configNum,
	}
	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return fmt.Errorf(ErrWrongLeader)
	}

	_, err := kv.waitForCommit(logIndex, func(op Op) bool {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if state, ok := kv.shardState[ShardID]; ok && state != 2 {
			return true
		}
		return false
	})
	if err != nil {
		return fmt.Errorf(err.Error())
	}
	return nil
}

func (kv *ShardKV) tryCommitNewConfig(config *shardctrler.Config) error {
	// Your code here.
	cmd := Op{
		OpType:      OP_TYPE_CONFIG_CHANGE,
		ConfigValid: true,
		Config:      *config,
	}

	logIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return fmt.Errorf(ErrWrongLeader)
	}

	_, err := kv.waitForCommit(logIndex, func(op Op) bool {
		if !op.ConfigValid {
			return true
		}

		kv.mu.Lock()
		defer kv.mu.Unlock()
		if op.Config.Num != config.Num {
			return true
		}
		return false
	})
	if err != nil {
		return fmt.Errorf(err.Error())
	}
	DPrintf(raft.DServer, "G%v applynew config.num=%v success", kv.gid, config.Num)
	return nil
}

func (kv *ShardKV) waitForCommit(logIndex int, commitFail func(op Op) bool) (*Op, error) {
	var op Op
	waitChan := kv.commitApplier.addWaiter(logIndex)
	timeOut := time.NewTimer(COMMIT_TIMEOUT * time.Second)
	select {
	case op = <-waitChan:
	case <-timeOut.C:
		return nil, fmt.Errorf(ErrCommitTimeout)
	}

	if commitFail(op) {
		return nil, fmt.Errorf(ErrCommitFail)
	}
	return &op, nil
}

func (kv *ShardKV) checkExecutedAndGroup(clientID string, requestID string, key string) (string, error) {
	//检查是否是这个Shard的Owner，如果不是，就需要查询config

	shardID := key2shard(key)
	gid := kv.config.Shards[shardID]
	if gid != kv.gid || kv.shardState[shardID] == SHARD_STATE_OFF {
		DPrintf(raft.DServer, "G%vs%v requestID=%v shard%vstate=%v", kv.gid, kv.rf.WhoAmI(), requestID, shardID, kv.shardState[shardID])
		return "", fmt.Errorf(ErrWrongGroup)
	}

	shard := kv.shardMap[shardID]
	lastRequestNum, lastResponse := shard.getLastestInfo(clientID)
	if requestIDToRequestNum(requestID) <= lastRequestNum {
		DPrintf(raft.DServer, "G%vs%v requestID=%v has executed,lastRequestID=%v", kv.gid, kv.rf.WhoAmI(), requestID, lastRequestNum)
		return lastResponse, fmt.Errorf(ErrExecuted)
	}

	return "", nil
}

const (
	SHARD_STATE_OFF = iota
	SHARD_STATE_ON_AND_NOT_CONFIRM
	SHARD_STATE_ON_AND_CONFIRMED
)

func (kv *ShardKV) daemon(do func(), sleepMs int) {
	for {
		select {
		case <-kv.killCh:
			return
		default:
			do()
		}
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
}
func (kv *ShardKV) DoConfirmShards() {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}

	kv.mu.Lock()
	lastConfig := *kv.lastConfig
	unconfirmedShardIDs := kv.getUnconfirmedShardsLocked()
	myGID := kv.gid
	kv.mu.Unlock()

	DPrintf(raft.DServer, "G%vs%v unconfirmedShards=%v configNum=%vnow=(%v)", kv.gid, kv.rf.WhoAmI(), unconfirmedShardIDs, lastConfig.Num+1, time.Now().String())

	if len(*unconfirmedShardIDs) != 0 {
		kv.confirmShards(myGID, lastConfig, unconfirmedShardIDs)
	}
}

func (kv *ShardKV) confirmShards(myGID int, lastConfig shardctrler.Config, unconfirmedShardIDs *[]int) {
	var wg sync.WaitGroup
	for _, ShardID := range *unconfirmedShardIDs {
		targetGID := lastConfig.Shards[ShardID]
		wg.Add(1)
		go func(shardID int) {
			args := ConfirmReceivedShardArgs{
				ConfigNum: lastConfig.Num + 1,
				ShardID:   shardID,
				GID:       myGID,
			}
			reply := ConfirmReceivedShardReply{}
			DPrintf(raft.DServer, "G%vs%v shard%v confirming", kv.gid, kv.rf.WhoAmI(), shardID)

			SendConfirmReceivedShardRequestToGroup(kv.make_end, targetGID, lastConfig, "ShardKV.ConfirmReceivedShard", &args, &reply)
			DPrintf(raft.DServer, "G%vs%v shard%v confirming,reply.Err=%v", kv.gid, kv.rf.WhoAmI(), shardID, reply.Err)

			if reply.Err == OK {
				DPrintf(raft.DServer, "G%vs%v shard%v confirmed,try to change state", kv.gid, kv.rf.WhoAmI(), shardID)
				//将这个Shard装入command并comit
				for {
					err := kv.changeShardStateToConfirmed(shardID)
					if err == nil || err.Error() == ErrWrongLeader {
						break
					}
				}

			}
			wg.Done()
		}(ShardID)
	}
	wg.Wait()
}

func (kv *ShardKV) getUnconfirmedShardsLocked() *[]int {
	var unconfirmedShards []int
	for ShardID, state := range kv.shardState {
		if state == SHARD_STATE_ON_AND_NOT_CONFIRM {
			unconfirmedShards = append(unconfirmedShards, ShardID)
		}
	}
	return &unconfirmedShards
}

// configListener 启动时需要kv.config已配置
func (kv *ShardKV) DoPullNextConfig() {
	//检查当前config是否还有剩余Shard没有Pull，没有Pull的话赶紧Pull

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}

	kv.mu.Lock()
	absentShardIDs := kv.checkAbsentShard()
	redundantShardIDs := kv.checkRedundantShard()
	unconfirmedShardIDs := kv.checkUnconfirmedShard()
	lastConfig := *kv.lastConfig
	myGID := kv.gid
	currentConfig := *kv.config
	kv.mu.Unlock()

	DPrintf(raft.DServer, "G%vs%v redundantShards=%v,absentShardIDs=%v,unconfirmedShardIDs=%v", kv.gid, kv.rf.WhoAmI(), redundantShardIDs, absentShardIDs, unconfirmedShardIDs)

	if len(*redundantShardIDs) == 0 && len(*absentShardIDs) == 0 && len(*unconfirmedShardIDs) == 0 {
		DPrintf(raft.DServer, "G%vs%v shards all are valid,pull new config", kv.gid, kv.rf.WhoAmI())
		newConfig := kv.pullNextConfig(currentConfig.Num)
		if newConfig == nil {
			//DPrintf(raft.DServer, "G%vs%v config.Num=%v is update", kv.gid, kv.rf.WhoAmI(), currentConfig.Num)
			return
		}

		kv.tryCommitNewConfig(newConfig)
	} else if len(*absentShardIDs) != 0 {
		DPrintf(raft.DServer, "G%vs%v absentShard=%v", kv.gid, kv.rf.WhoAmI(), *absentShardIDs)
		kv.dealWithAbsentShards(myGID, lastConfig, absentShardIDs)
	}
}

func (kv *ShardKV) dealWithAbsentShards(myGID int, lastConfig shardctrler.Config, absentShardIDs *[]int) {
	var wg sync.WaitGroup
	for _, ShardID := range *absentShardIDs {
		targetGID := lastConfig.Shards[ShardID]
		wg.Add(1)
		go func(shardID int) {

			args := PullShardArgs{
				ConfigNum: lastConfig.Num + 1,
				ShardID:   shardID,
				GID:       myGID,
			}
			reply := PullShardReply{}
			SendPullShardRequestToGroup(kv.make_end, targetGID, lastConfig, "ShardKV.PullShard", &args, &reply)

			if reply.Err == OK {
				//将这个Shard装入command并comit
				DPrintf(raft.DServer, "G%vs%v getShard%v,try to commit", kv.gid, kv.rf.WhoAmI(), shardID)
				tryToAddShardToStorage(kv, reply.Shard, shardID)
			}

			wg.Done()
		}(ShardID)
	}
	wg.Wait()
}

func tryToAddShardToStorage(kv *ShardKV, shard Shard, ShardID int) {
	for {
		err := kv.addShardToStorage(shard, ShardID)
		if err == nil || err.Error() == ErrWrongLeader {
			break
		}
	}
}

func (kv *ShardKV) pullNextConfig(currentConfigNum int) *shardctrler.Config {
	config := kv.sm.Query(-1)
	switch {
	case config.Num > currentConfigNum+1:
		config = kv.sm.Query(currentConfigNum + 1)
		return &config
	case config.Num == currentConfigNum+1:
		return &config
	default:
		return nil
	}
}

func (kv *ShardKV) checkAbsentShard() *[]int {
	var absentShard []int
	for ShardID, gid := range kv.config.Shards {
		if gid == kv.gid && kv.shardState[ShardID] == SHARD_STATE_OFF {
			absentShard = append(absentShard, ShardID)
		}
	}
	return &absentShard
}

func (kv *ShardKV) checkRedundantShard() *[]int {
	var redundantShard []int
	for ShardID, _ := range kv.shardMap {
		if kv.config.Shards[ShardID] != kv.gid {
			redundantShard = append(redundantShard, ShardID)
		}
	}
	return &redundantShard
}

func (kv *ShardKV) checkUnconfirmedShard() *[]int {
	var unconfirmedShards []int
	for shardID, state := range kv.shardState {
		if state == SHARD_STATE_ON_AND_NOT_CONFIRM {
			unconfirmedShards = append(unconfirmedShards, shardID)
		}
	}
	return &unconfirmedShards
}

func SendPullShardRequestToGroup(make_end func(string) *labrpc.ClientEnd, gid int, config shardctrler.Config, functionName string, args *PullShardArgs, reply *PullShardReply) {
	//TODO:这里可以做成记住Leader形式，可能会做成一个struct
	if servers, ok := config.Groups[gid]; ok {
		for si := 0; si < len(servers); si++ {
			srv := make_end(servers[si])
			ok := srv.Call(functionName, args, reply)
			if ok && (reply.Err == OK || reply.Err == ErrExecuted) {
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				break
			}
			// ... not ok, or ErrWrongLeader
		}
	}
}

func SendConfirmReceivedShardRequestToGroup(make_end func(string) *labrpc.ClientEnd, gid int, config shardctrler.Config, functionName string, args *ConfirmReceivedShardArgs, reply *ConfirmReceivedShardReply) {
	//TODO:这里可以做成记住Leader形式，可能会做成一个struct
	if servers, ok := config.Groups[gid]; ok {
		for si := 0; si < len(servers); si++ {
			srv := make_end(servers[si])
			ok := srv.Call(functionName, args, reply)
			if ok && (reply.Err == OK || reply.Err == ErrExecuted) {
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				break
			}
			// ... not ok, or ErrWrongLeader
		}
	}
}

func (kv *ShardKV) getShardMaxLogIndex() int {
	return int(atomic.LoadInt32(&kv.maxShardLogIndex))
}

func (kv *ShardKV) setShardMaxLogIndex(logIndex int) {
	atomic.StoreInt32(&kv.maxShardLogIndex, int32(logIndex))
}

type DiskValue struct {
	//lock  deadlock.RWMutex
	Value     string
	VersionID int
}

func applyKVOp(kv *ShardKV, op Op, logIndex int) (string, error) {
	resp := ""

	shardID := key2shard(op.Key)
	shard := kv.shardMap[shardID]
	valueWrapper, ok := shard.Disk[op.Key]

	//VersionID如果使用LogIndex，那么不适合在分片的情况下使用，因为不同机器LogIndex不同
	//如果使用自增ID，就需要先知道它的expected，但是即使收集了expected，但是因为还有没commit的操作，失败概率会无限放大
	// if ok && valueWrapper.VersionID >= logIndex {
	// 	return "", nil
	// }

	if !ok {
		valueWrapper = DiskValue{}
	}

	switch op.OpType {
	case OP_TYPE_GET:
		if ok {
			resp = valueWrapper.Value
			DPrintf(raft.DInfo, "G%vP%vs%v requestID=%v logIndex=%v get[%v]->(%v)", kv.gid, kv.me, kv.rf.WhoAmI(), op.RequestID, logIndex, op.Key, resp)
		} else {
			resp = ""
			DPrintf(raft.DInfo, "G%vP%vs%v requestID=%v logIndex=%v get[%v] no this key", kv.gid, kv.me, kv.rf.WhoAmI(), op.RequestID, logIndex, op.Key)
		}
		shard.setLastestInfo(op.ClientID, requestIDToRequestNum(op.RequestID), resp)
	case OP_TYPE_PUT:
		diskValue, ok := shard.Disk[op.Key]
		if ok {
			diskValue.Value = op.Value
			diskValue.VersionID = diskValue.VersionID + 1
			shard.Disk[op.Key] = diskValue
		} else {
			shard.Disk[op.Key] = DiskValue{
				Value:     op.Value,
				VersionID: 0,
			}
		}

		DPrintf(raft.DInfo, "G%vP%vs%v requestID=%v logIndex=%v put[%v]->(%v)", kv.gid, kv.me, kv.rf.WhoAmI(), op.RequestID, logIndex, op.Key, op.Value)
		resp = op.Value
		shard.setLastestInfo(op.ClientID, requestIDToRequestNum(op.RequestID), "E")

	case OP_TYPE_APPEND:
		oldValue := ""
		if ok {
			oldValue = valueWrapper.Value
		} else {
			valueWrapper = DiskValue{}
		}
		valueWrapper.Value = oldValue + op.Value
		valueWrapper.VersionID = valueWrapper.VersionID + 1
		shard.Disk[op.Key] = valueWrapper
		resp = valueWrapper.Value
		DPrintf(raft.DInfo, "G%vP%vs%v requestID=%v logIndex=%v append[%v](%v)->(%v) ", kv.gid, kv.me, kv.rf.WhoAmI(), op.RequestID, logIndex, op.Key, op.Value, valueWrapper.Value)
		shard.setLastestInfo(op.ClientID, requestIDToRequestNum(op.RequestID), "E")
	
	}
	

	return resp, nil
}

func applyShardOp(kv *ShardKV, op *Op, logIndex int) {
	switch op.OpType {
	case OP_TYPE_SHARD_ADD:
		DPrintf(raft.DServer, "G%vs%v add Shard%v,logIndex=%v", kv.gid, kv.rf.WhoAmI(), op.ShardID, logIndex)
		state := kv.shardState[op.ShardID]
		if state == SHARD_STATE_OFF {
			kv.shardMap[op.ShardID] = &op.Shard
			kv.shardState[op.ShardID] = SHARD_STATE_ON_AND_NOT_CONFIRM
		}
	case OP_TYPE_SHARD_STOP:
		DPrintf(raft.DServer, "G%vs%v stop Shard%v,transfering,logIndex=%v", kv.gid, kv.rf.WhoAmI(), op.ShardID, logIndex)
		if _, ok := kv.shardState[op.ShardID]; ok {
			kv.shardState[op.ShardID] = SHARD_STATE_OFF
		}
	case OP_TYPE_SHARD_DELETE:
		if _, ok := kv.shardState[op.ShardID]; ok {
			DPrintf(raft.DServer, "G%vs%v delete Shard%v,logIndex=%v", kv.gid, kv.rf.WhoAmI(), op.ShardID, logIndex)
			delete(kv.shardMap, op.ShardID)
			delete(kv.shardState, op.ShardID)
		}
	case OP_TYPE_SHARD_CONFIRM:
		DPrintf(raft.DServer, "G%vs%v confirmed Shard%v,logIndex=%v", kv.gid, kv.rf.WhoAmI(), op.ShardID, logIndex)
		kv.shardState[op.ShardID] = SHARD_STATE_ON_AND_CONFIRMED
	}
}

func applyConfigOp(kv *ShardKV, op *Op, logIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num+1 == op.Config.Num {
		kv.lastConfig = kv.config
		kv.config = &op.Config

		//不需要Pull，直接初始化即可
		if kv.config.Num == 1 {
			for shardID, gid := range kv.config.Shards {
				if gid == kv.gid {

					kv.shardMap[shardID] = &Shard{
						Disk:        make(map[string]DiskValue),
						MaxNums:     make(map[string]int),
						LastestResp: make(map[string]string),
					}
					kv.shardState[shardID] = SHARD_STATE_ON_AND_CONFIRMED
					DPrintf(raft.DServer, "G%vs%v Create Shard%v", kv.gid, kv.rf.WhoAmI(), shardID)

				}
			}
		}

		onDuty := ""
		for shardID, gid := range kv.config.Shards {
			if gid == kv.gid {
				onDuty = onDuty + strconv.Itoa(shardID) + ","
			}
		}
		DPrintf(raft.DServer, "G%vs%v config.Num=%v,onduty=%v,logIndex=%v", kv.gid, kv.rf.WhoAmI(), kv.config.Num, onDuty, logIndex)

	}
}

func (s *Shard) setLastestInfo(clientID string, requestNum int, response string) {
	s.MaxNums[clientID] = requestNum
	s.LastestResp[clientID] = response
}

func (s *Shard) getLastestInfo(clientID string) (int, string) {
	return s.MaxNums[clientID], s.LastestResp[clientID]
}

func (kv *ShardKV) makeSnapshot(logIndex int) {
	kv.mu.Lock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.me)
	e.Encode(kv.shardMap)
	e.Encode(kv.shardState)
	e.Encode(kv.maxShardLogIndex)

	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	data := w.Bytes()

	//rf.persister.SaveRaftState(data)
	kv.rf.Snapshot(logIndex, data)

	kv.mu.Unlock()

	DPrintf(raft.DServer, "P%vs%v logIndex=%v,snapshotSize=%v", kv.me, kv.rf.WhoAmI(), logIndex, len(data))

	kv.rf.ForcePersist()
}

func (kv *ShardKV) applySnapshot(logIndex int, data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		panic("nil snapshot")
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var me int
	var shardMap map[int]*Shard
	var shardState map[int]int32
	var maxShardLogIndex int32

	var config *shardctrler.Config
	var lastConfig *shardctrler.Config

	if d.Decode(&me) != nil ||
		d.Decode(&shardMap) != nil ||
		d.Decode(&shardState) != nil ||
		d.Decode(&maxShardLogIndex) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil {
		DPrintf(raft.DServer, "P%v decode error", kv.me)
	} else {
		kv.mu.Lock()

		kv.me = me
		kv.shardMap = shardMap
		kv.shardState = shardState
		kv.maxShardLogIndex = maxShardLogIndex

		kv.config = config
		kv.lastConfig = lastConfig
		//rf.lastApplied = lastApplied
		kv.mu.Unlock()
		DPrintf(raft.DServer, "P%v logIndex=%v,snapshot applied", kv.me, logIndex)
	}

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.killCh)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Shard{})
	labgob.Register(DiskValue{})
	//labgob.Register()

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.shardMap = make(map[int]*Shard)
	kv.shardState = make(map[int]int32)

	kv.commitApplier = NewCommitApplier(kv.applyCh, kv, kv.maxraftstate)
	kv.commitApplier.start()
	kv.killCh = make(chan int)
	kv.config = &shardctrler.Config{
		Num: 0,
	}
	kv.lastConfig = &shardctrler.Config{
		Num: 0,
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh, "KVStore", kv.gid)

	//TODO:启动一个对Config的监听线程，每两秒监听一次Config
	go kv.daemon(kv.DoConfirmShards, 1000)
	go kv.daemon(kv.DoPullNextConfig, 100)
	DPrintf(raft.DServer, "G%vP%v StartServer", kv.gid, kv.me)
	return kv
}

func requestIDToRequestNum(requestID string) int {
	resp, _ := strconv.Atoi(requestID[12:])
	return resp
}

type commitApplier struct {
	waiters map[int]chan Op
	lock    deadlock.RWMutex

	applyCh chan raft.ApplyMsg

	kvserver *ShardKV

	appliedOpCount int
	maxraftstate   int

	appliedSnapshotLogIndex int

	exitCh chan interface{} //[TODO]只关注信号，不关注信号内容的chan咋写呢？
}

// [TODO] chan本身就是个指针，按说不加*号完全没有毛病
func NewCommitApplier(applyCh chan raft.ApplyMsg, kvserver *ShardKV, maxraftstate int) commitApplier {
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
					//DPrintf(raft.DServer, "G%vP%vs%v new Command logIndex=%v", ca.kvserver.gid, ca.kvserver.me, ca.kvserver.rf.WhoAmI(), applyMsg.CommandIndex)
					notifyChan := ca.getWaiter(applyMsg.CommandIndex)
					op, ok := applyMsg.Command.(Op)
					if ok {
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
					} else {
						DPrintf(raft.DServer, "P%vs%v logIndex=%v receive emptyString", ca.kvserver.me, ca.kvserver.rf.WhoAmI(), applyMsg.CommandIndex)
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

func dealWithOp(kv *ShardKV, applyMsg raft.ApplyMsg, op *Op) bool {
	switch {
	case op.ShardOpValid:
		kv.mu.Lock()
		if op.ConfigNum == kv.config.Num {
			applyShardOp(kv, op, applyMsg.CommandIndex)
		}
		kv.mu.Unlock()
		return true
	case op.ConfigValid:
		applyConfigOp(kv, op, applyMsg.CommandIndex)
		return true
	default:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		shardID := key2shard(op.Key)
		state := kv.shardState[shardID]
		if state == SHARD_STATE_OFF {
			op.Error = ErrWrongGroup
			return true
		}

		shard := kv.shardMap[shardID]
		maxRqNum, _ := shard.getLastestInfo(op.ClientID)
		if maxRqNum < requestIDToRequestNum(op.RequestID) || op.OpType == OP_TYPE_GET {
			//DPrintf(raft.DServer, "P%vs%v maxRequestNum=%v,currentRequestNum=%v", kv.me, kv.rf.WhoAmI(), maxRqNum, requestIDToRequestNum(op.RequestID))
			value, err := applyKVOp(kv, *op, applyMsg.CommandIndex)
			if err != nil {
				op.Error = err.Error()
			}
			op.Value = value
			return true
		}
		return false
	}
}

func (ca *commitApplier) getAppliedOpCount() int {
	return ca.appliedOpCount
}
func (ca *commitApplier) addAppliedOpCount() {
	ca.appliedOpCount += 40
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
