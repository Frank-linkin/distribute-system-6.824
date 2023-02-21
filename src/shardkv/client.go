package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"strconv"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"github.com/google/uuid"
	"github.com/sasha-s/go-deadlock"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	leaderLock deadlock.RWMutex
	leaderMap  map[int]int

	requestID int32
	clientID  string

	me int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd, me int) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leaderLock = deadlock.RWMutex{}
	ck.leaderMap = make(map[int]int)
	ck.clientID = uuid.New().String()
	ck.requestID = 0

	ck.me = me
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	requestID := buildRequestID(ck.clientID, int(ck.getRequestNum()))
	args.ClientID = ck.clientID
	args.RequestID = requestID
	shard := key2shard(key)
	for {
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			leader := ck.getLeader(gid)
			for offset := 0; offset < len(servers); offset++ {
				target := (offset + leader) % len(servers)
				srv := ck.make_end(servers[target])
				var reply GetReply

				//DPrintf(raft.DServer, "C%v requestID=%v Get targetShard=%v now=(%v)", ck.me, requestID, shard, time.Now().String())

				ok := srv.Call("ShardKV.Get", &args, &reply)

				DPrintf(raft.DServer, "C%v requestID=%v Get targetShard=%v now=(%v) reply.Err=%v", ck.me, requestID, shard, time.Now().String(), reply.Err)

				if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrExecuted) {
					if offset != 0 {
						ck.SetLeader(gid, target)
					}
					DPrintf(raft.DServer, "C%v requestID=%v Get targetShard=%v success", ck.me, requestID, shard)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		//DPrintf(raft.DServer, "C%v requestID=%v fetchNewConfig now=(%v)", ck.me, requestID, time.Now().String())
		ck.config = ck.sm.Query(-1)
		//DPrintf(raft.DServer, "C%v requestID=%v fetchNewConfig finished now=(%v)", ck.me, requestID, time.Now().String())
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	requestID := buildRequestID(ck.clientID, int(ck.getRequestNum()))
	args.ClientID = ck.clientID
	args.RequestID = requestID
	shard := key2shard(key)
	DPrintf(raft.DServer, "C%v requestID=%v %v targetShard=%v", ck.me, requestID, op, shard)
	for {
		gid := ck.config.Shards[shard]
		DPrintf(raft.DServer, "C%v requestID=%v %v targetShard=%v", ck.me, requestID, op, shard)

		//DPrintf(raft.DClient, "requestID=%v PutAppend k[%v](%v) targetGID=%v", args.RequestID, args.Key, args.Value, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			leader := ck.getLeader(gid)
			for offset := 0; offset < len(servers); offset++ {
				target := (offset + leader) % len(servers)
				srv := ck.make_end(servers[target])
				var reply PutAppendReply

				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrExecuted) {
					if offset != 0 {
						ck.SetLeader(gid, target)
					}
					DPrintf(raft.DServer, "C%v requestID=%v %v targetShard=%v success", ck.me, requestID, op, shard)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					log.Printf("Wrong group")
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) SetLeader(gid int, leader int) {
	ck.leaderLock.Lock()
	defer ck.leaderLock.Unlock()

	ck.leaderMap[gid] = leader
}

func (ck *Clerk) getLeader(gid int) int {
	ck.leaderLock.RLock()
	defer ck.leaderLock.RUnlock()

	return ck.leaderMap[gid]
}

func (ck *Clerk) getRequestNum() int32 {
	return atomic.AddInt32(&ck.requestID, 1)
}

func buildRequestID(clientID string, requestNum int) string {
	return clientID[:8] + "_No." + strconv.Itoa(int(requestNum))
}
