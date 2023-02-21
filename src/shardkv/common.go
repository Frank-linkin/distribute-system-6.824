package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrExecuted = "ErrExecuted"

	ErrCommitTimeout = "ERR:commit timeout"
    ErrCommitFail = "ERR:commit fail, please retry"
    ErrConfigNumMismatch = "ERR:ConfigNum mismatch"

)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	RequestID string
	ClientID string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestID string
	ClientID string
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	ConfigNum int
	GID int
	ShardID int
}

type PullShardReply struct {
	Err   Err
	Shard Shard
}

type ConfirmReceivedShardArgs struct {
	ConfigNum int
	GID int
	ShardID int
}

type ConfirmReceivedShardReply struct {
	Err   Err
}

type Shard struct {
	//mu deadlock.Mutex
	Disk map[string]DiskValue

	MaxNums     map[string]int    //√
	LastestResp map[string]string //√
}