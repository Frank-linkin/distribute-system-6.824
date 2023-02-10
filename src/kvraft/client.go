package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int32

	requestID int32
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
	return ck
}

func (ck *Clerk) getLeader() int {
	return int(atomic.LoadInt32(&ck.leader))
}

func (ck *Clerk) setLeader(leader int) {
	atomic.StoreInt32(&ck.leader, int32(leader))
}

var requestID int32=0
func (ck *Clerk) getRequestID() int32 {
	return atomic.AddInt32(&requestID, 1)
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
	initialLeader := ck.getLeader()
	requestID := ck.getRequestID()
SERVER_LOOP:
	for offset := 0; offset < len(ck.servers); offset++ {
		reply := GetReply{}
		target = (initialLeader + offset) % len(ck.servers)
		args := GetArgs{
			Key:       key,
			RequestID: int(requestID),
			ClientID:  target,
		}
		DPrintf(raft.DClient, "C%v requestID=%v Get[%v]=(%v)", target, requestID, args.Key, reply.Value)

		success := false
		for !success {
			success = ck.sendGet(target, &args, &reply)
		}

		if reply.Err == "" {
			DPrintf(raft.DClient, "C%v requestID=%v Get[%v]=(%v) success", target, requestID, args.Key, reply.Value)
			value = reply.Value
			ck.setLeader(target)
			return value
		} else {
			if reply.Err == ERR_NOT_LEADER && offset == len(ck.servers)-1 {
				DPrintf(raft.DClient, "C%d requestID=%v, no leader, wait for 1 second\n", target, requestID)
				time.Sleep(1 * time.Second)
				goto SERVER_LOOP
			}
		}
	}

	return value
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
	initialLeader := ck.getLeader()
	requestID := ck.getRequestID()
SERVER_LOOP:
	for offset := 0; offset < len(ck.servers); offset++ {
		reply := PutAppendReply{}
		target = (initialLeader + offset) % len(ck.servers)
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			RequestID: int(requestID),
			ClientID:  target,
		}
		DPrintf(raft.DClient, "C%v requestID=%v %v k[%v]v(%v)  C%v", target, args.RequestID, op, args.Key, args.Value, target)
		success := ck.sendPutAppend(target, &args, &reply)
		for !success {
			time.Sleep(1 * time.Second)
			success = ck.sendPutAppend(target, &args, &reply)
		}

		if reply.Err == "" {
			DPrintf(raft.DClient, "C%v requestID=%v %v k[%v]v(%v) success", target, args.RequestID, op, args.Key, args.Value)
			ck.setLeader(target)
			break
		} else {
			if reply.Err == ERR_NOT_LEADER && offset == len(ck.servers)-1 {
				DPrintf(raft.DClient, "C%v requestID=%v no leader, wait for 1 second\n", args.RequestID, requestID)
				time.Sleep(1 * time.Second)
				goto SERVER_LOOP
			}
			//DPrintf(raft.DClient, "C%d %v k[%v] v[%v],Err=%v", target, op, args.Key, args.Value, reply.Err)
		}
	}

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
