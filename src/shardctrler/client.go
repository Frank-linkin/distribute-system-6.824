package shardctrler

//
// Shardctrler clerk.
//

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
	// Your data here.
	clientID  string
	requestID int32
	leader    int32
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
	// Your code here.

	ck.clientID = uuid.NewString()
	ck.requestID = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.RequestID = buildRequestID(ck.clientID, int(ck.getRequestNum()))
	args.ClientID = ck.clientID
	for {
		initialLeader := ck.getLeader()
		// try each known server.
		for offset := 0; offset < len(ck.servers); offset++ {
			var reply QueryReply
			target := (initialLeader + offset) % len(ck.servers)

			DPrintf(raft.DClient, "C(%v) requestID=%v Querying", ck.clientID, args.RequestID)

			ok := ck.servers[target].Call("ShardCtrler.Query", args, &reply)

			DPrintf(raft.DClient, "C(%v) requestID=%v QueryReply=%v", ck.clientID, args.RequestID, reply)
			if ok && reply.WrongLeader == false && reply.Err == "" {
				ck.setLeader(target)
				return reply.Config
			}

			if reply.Err == ERR_COMMIT_TIMEOUT|| reply.Err == ERR_COMMIT_FAIL{
				offset--
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.RequestID = buildRequestID(ck.clientID, int(ck.getRequestNum()))
	args.ClientID = ck.clientID
	for {
		initialLeader := ck.getLeader()
		// try each known server.
		for offset := 0; offset < len(ck.servers); offset++ {
			var reply JoinReply
			target := (initialLeader + offset) % len(ck.servers)
			ok := ck.servers[target].Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == "" {
				ck.setLeader(target)
				return
			}
			if reply.Err == ERR_COMMIT_TIMEOUT|| reply.Err == ERR_COMMIT_FAIL{
				offset--
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	args.RequestID = buildRequestID(ck.clientID, int(ck.getRequestNum()))
	args.ClientID = ck.clientID
	for {
		initialLeader := ck.getLeader()
		// try each known server.
		for offset := 0; offset < len(ck.servers); offset++ {
			var reply LeaveReply
			target := (initialLeader + offset) % len(ck.servers)

			ok := ck.servers[target].Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == "" {
				ck.setLeader(target)
				return
			}
			if reply.Err == ERR_COMMIT_TIMEOUT|| reply.Err == ERR_COMMIT_FAIL{
				offset--
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	args.RequestID = buildRequestID(ck.clientID, int(ck.getRequestNum()))
	args.ClientID = ck.clientID
	DPrintf(raft.DClient, "C(%v) requestID=%v move(%v)->Group{%v}", ck.clientID, args.RequestID, shard, gid)
	for {
		initialLeader := ck.getLeader()

		// try each known server.
		for offset := 0; offset < len(ck.servers); offset++ {
			var reply MoveReply
			target := (initialLeader + offset) % len(ck.servers)

			ok := ck.servers[target].Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == "" {
				ck.setLeader(target)
				return
			}
			if reply.Err == ERR_COMMIT_TIMEOUT|| reply.Err == ERR_COMMIT_FAIL{
				offset--
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
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

func buildRequestID(clientID string, requestNum int) string {
	return clientID[:8] + "_No." + strconv.Itoa(int(requestNum))
}
