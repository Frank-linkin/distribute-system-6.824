package kvraft

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"github.com/google/uuid"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int32

	requestID int32

	clientID string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0

	ck.requestID = 0

	ck.clientID = uuid.NewString()
	return ck
}

func (ck *Clerk) getLeader() int {
	return int(atomic.LoadInt32(&ck.leader))
}

func (ck *Clerk) setLeader(leader int) {
	atomic.StoreInt32(&ck.leader, int32(leader))
}

func (ck *Clerk) getRequestNum() int32 {
	return atomic.AddInt32(&ck.requestID, 1)
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	var target int
	var value string

	// ck.monotonicLock.Lock()
	// defer ck.monotonicLock.Unlock()

	requestID := buildRequestID(ck.clientID, int(ck.getRequestNum()))
SERVER_LOOP:
	initialLeader := ck.getLeader()
	continueLoop := false
	for offset := 0; offset < len(ck.servers); offset++ {
		reply := GetReply{}
		target = (initialLeader + offset) % len(ck.servers)
		//DPrintf(raft.DClient, "C(%v)->(P%v) requestID=%v len(servers)=%v", ck.clientID, target, requestID, len(ck.servers))
		args := GetArgs{
			Key:       key,
			RequestID: requestID,
			ClientID:  ck.clientID,
		}
		DPrintf(raft.DClient, "C(%v)->(P%v) requestID=%v prepare to Get[%v]", ck.clientID, target, requestID, args.Key)

		success := ck.sendGet(target, &args, &reply)
		count := 0
		for !success && count < 3 {
			time.Sleep(time.Second)
			DPrintf(raft.DClient, "C(%v)->(P%v) requestID=%v Get[%v]=(%v) retry", ck.clientID, target, requestID, args.Key, reply.Value)
			success = ck.sendGet(target, &args, &reply)
			count++
		}
		if success && reply.Err == "" {
			continueLoop = false
			DPrintf(raft.DClient, "C(%v)->(P%v) requestID=%v Get[%v]=(%v) success", ck.clientID, target, requestID, args.Key, reply.Value)
			ck.setLeader(target)
			value = reply.Value
			break
		} else if reply.Err == ERR_COMMIT_TIMEOUT {
			offset--

		} else {
			continueLoop = true
		}
	}

	if continueLoop {
		time.Sleep(20 * time.Millisecond)
		goto SERVER_LOOP
	}
	DPrintf(raft.DClient, "C(%v)->(P%v) requestID=%v Get[%v]=(%v) quit", ck.clientID, target, requestID, key, value)
	return value
}

func buildRequestID(clientID string, requestNum int) string {
	return clientID[:8] + "_No." + strconv.Itoa(int(requestNum))
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	var target int

	// ck.monotonicLock.Lock()
	// defer ck.monotonicLock.Unlock()

	requestID := buildRequestID(ck.clientID, int(ck.getRequestNum()))
SERVER_LOOP:
	initialLeader := ck.getLeader()
	continueLoop := false
	for offset := 0; offset < len(ck.servers); offset++ {
		reply := PutAppendReply{}
		target = (initialLeader + offset) % len(ck.servers)
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			RequestID: requestID,
			ClientID:  ck.clientID,
		}
		DPrintf(raft.DClient, "C(%v)->(P%v) requestID=%v prepare to %v k[%v]v(%v)", ck.clientID, target, args.RequestID, op, args.Key, args.Value)
		success := ck.sendPutAppend(target, &args, &reply)
		count := 1
		for !success && count < 3 {
			time.Sleep(1 * time.Second)
			DPrintf(raft.DClient, "C(%v)->(P%v) requestID=%v %v k[%v]v(%v) Retry", ck.clientID, target, args.RequestID, op, args.Key, args.Value)
			success = ck.sendPutAppend(target, &args, &reply)
			count++
		}

		if success && reply.Err == "" {
			continueLoop = false
			DPrintf(raft.DClient, "C(%v)->(P%v) requestID=%v %v k[%v]v(%v) success", ck.clientID, target, args.RequestID, op, args.Key, args.Value)
			ck.setLeader(target)
			break
		} else if reply.Err == ERR_COMMIT_TIMEOUT {
			offset--

		} else {
			continueLoop = true
		}
	}

	if continueLoop {
		time.Sleep(20 * time.Millisecond)
		goto SERVER_LOOP
	}
	DPrintf(raft.DClient, "C(%v)->(P%v) requestID=%v %v k[%v]v(%v) quit", ck.clientID, target, requestID, op, key, value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	return ck.servers[server].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	return ck.servers[server].Call("KVServer.PutAppend", args, reply)
}
