package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int32

	requestID []int
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

	ck.requestID = make([]int, len(servers))
	return ck
}

func (ck *Clerk) getLeader() int {
	return int(atomic.LoadInt32(&ck.leader))
}

func (ck *Clerk) setLeader(leader int) {
	atomic.StoreInt32(&ck.leader, int32(leader))
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
	var reply *GetReply
	initialLeader := ck.getLeader()
	for offset := 0; offset < len(ck.servers); offset++ {
		target = (initialLeader + offset) % len(ck.servers)
		ck.requestID[target]++
		args := GetArgs{
			Key:       key,
			RequestID: ck.requestID[target],
			ClientID:  target,
		}
		
		ck.servers[target].Call("Get", args, reply)
		if reply.Err == "" {
			ck.setLeader(target)
			break
		}
	}

	return reply.Value
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
	var reply *PutAppendReply
	initialLeader := ck.getLeader()
	for offset := 0; offset < len(ck.servers); offset++ {
		target = (initialLeader + offset) % len(ck.servers)
		ck.requestID[target]++
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			RequestID: ck.requestID[target],
			ClientID:  target,
		}
		ck.servers[target].Call("PutAppend", args, reply)
		if reply.Err == "" {
			ck.setLeader(target)
			break
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
