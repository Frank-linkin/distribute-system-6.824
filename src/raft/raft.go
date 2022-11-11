package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const HEARTBEAT_INTERVAL = 40
const ROLE_FOLLOWER = 1
const ROLE_CANDIDATE = 2
const ROLE_PRIMARY = 3

// ElectionTimeout (base - base + interval*ratio)
const ELECTION_TIMEOUT_BASE = 400 // millisecond
const ELECTION_TIMEOUT_INTERVAl = 40
const ELECTION_TIMEOUT_RATIO = 10

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type MyData struct {
}

type logEntry struct {
	Idx  int
	Term int
	Data *MyData
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int //-1 表示没有投票
	lastLog     logEntry

	//[Question] commitIndex和lastApplied分别是啥意思？
	commitIdx   int
	lastApplied int

	//for leaders
	nextidx  []int
	matchidx []int

	//for role control
	resetCh        chan bool
	electionTicker *time.Ticker
	role           int //1:follower,
	roleLock       sync.RWMutex

	//log 不应该有这个的
	diskLog []logEntry

	//
	exitCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	rf.mu.Unlock()

	rf.roleLock.RLock()
	if rf.role == ROLE_PRIMARY {
		isleader = true
	}
	rf.roleLock.RUnlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
	LastLogIdx  int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//TODO: ElectionTime out的interval太近了，时候分不开
	MyDebug(dVote, "S%d receive vote request from S%d", rf.me, args.CandidateID)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	MyDebug(dVote, "S%d have graped lock", rf.me)
	if args.Term < rf.currentTerm {
		MyDebug(dVote, "S%d reject vote for %d, stale term", rf.me, args.CandidateID)
		goto REJECT_VOTE
	}

	//如果candidate term>currentTerm，那么一定没有投过票；只有term=currentTerm的时候，
	//才考虑拒绝投票
	if rf.currentTerm == args.Term && rf.voteFor != -1 && rf.voteFor != args.CandidateID {
		MyDebug(dVote, "S%d reject vote for %d, voted", rf.me, args.CandidateID)
		goto REJECT_VOTE
	}
	//关于as update-to-date as receiver这个条件的判断
	if rf.lastLog.Idx <= args.LastLogIdx {
		rf.currentTerm = args.Term
		rf.voteFor = args.CandidateID
		//如果当前是primary，收到VoteRequest也要stopDown
		reply.Term = args.Term
		reply.VoteGranted = true
		MyDebug(dVote, "S%d vote and reset", rf.me)
		rf.resetCh <- true
		MyDebug(dVote, "S%d vote for %d", rf.me, args.CandidateID)
		return
	}

REJECT_VOTE:
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIdx   int
	PrevLogTerm  int
	LogEntries   []logEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//只有currentTerm<=args.Term的时候，才会发送reset信号
	//currentTerm==args.Term，证明是follower
	//currentTerm<args.Term，不用说肯定是落后了
	MyDebug(dInfo, "S%d beat from s%d", rf.me, args.LeaderID)
	MyDebug(dInfo, "S%d currTerm=%d,args.Term=%d", rf.me, rf.currentTerm, args.Term)

	rf.resetCh <- true
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	//TODELETE:暂时这么写
	//证明此server上有落后的logEntry
	// if rf.diskLog[args.PrevLogIdx].Term!=args.PrevLogTerm {
	// 	reply.success = false
	// 	reply.Term=rf.currentTerm
	// 	return
	// }
	//implement3,4,5
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.exitCh <- true
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// 可以看看别的项目里candidate,follower,Primary的转化是怎么写的
func (rf *Raft) ticker() {
	MyDebug(dInfo, "S%d start being a follower", rf.me)
	//TODO:这里在真正去写可以改成状态机
	//https://github.com/salamer/naive_raft/blob/master/node.go
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// as a follower
		if rf.role != ROLE_PRIMARY {
			//server间，electionTimeOut的interval最好大于heartbeat的时间
			rand.Seed(int64(rf.me) + time.Now().Unix())
			timeOut := (ELECTION_TIMEOUT_BASE + (rand.Int()%ELECTION_TIMEOUT_RATIO)*ELECTION_TIMEOUT_INTERVAl)
			//这里必须用一个NewTicker，因为如果因为网络原因没有连接到其他server，原来的ticker里面会积累很多message
			//ticker就失去了作用

			rf.electionTicker = time.NewTicker(time.Duration(timeOut) * time.Millisecond)
		}

		if rf.role == ROLE_FOLLOWER {
			MyDebug(dInfo, "S%d start being a follower", rf.me)
		FOLLOWER_LOOP:
			for {
				select {
				case <-rf.exitCh:
					goto EXIT
				default:
				}

				select {
				case <-rf.resetCh:
					timeOut := (ELECTION_TIMEOUT_BASE + (rand.Int()%ELECTION_TIMEOUT_RATIO)*ELECTION_TIMEOUT_INTERVAl)
					rf.electionTicker.Reset(time.Duration(timeOut) * time.Millisecond)
					MyDebug(dInfo, "S%d reset, follower -> follower", rf.me)
				case <-rf.electionTicker.C:
					MyDebug(dVote, "S%d electionTimeout,follower -> candidate", rf.me)
					rf.roleLock.Lock()
					rf.role = ROLE_CANDIDATE
					rf.roleLock.Unlock()
					break FOLLOWER_LOOP
				case <-rf.exitCh:
					goto EXIT
				}
			}
		} else if rf.role == ROLE_CANDIDATE {
			select {
			case <-rf.exitCh:
				goto EXIT
			default:
			}

			successChan := make(chan bool)
			go startElection(rf, successChan)
			select {
			case <-successChan:
				MyDebug(dVote, "S%d win the election, candidate -> Leader", rf.me)
				rf.roleLock.Lock()
				rf.role = ROLE_PRIMARY
				rf.roleLock.Unlock()
			case <-rf.electionTicker.C:
				MyDebug(dInfo, "S%d candidate electionTimeout,restart election", rf.me)
			case <-rf.resetCh:
				MyDebug(dInfo, "S%d candidate->follower", rf.me)
				rf.roleLock.Lock()
				rf.role = ROLE_FOLLOWER
				rf.roleLock.Unlock()
			case <-rf.exitCh:
				goto EXIT
			}
		} else if rf.role == ROLE_PRIMARY {
			//为了保证heartBeatToOthers的时候，防止因为接受了appendEntry改变了term，然后才运行到heartBeart，这样heartBeat出去的就是新接受的term，导致发出appendEntry的
			//Leader退位。而Leader上位之后，term应该是不变的，因为立刻提取出来
			accessionTime := time.Now()
			rf.mu.Lock()
			accesstionTerm := rf.currentTerm
			me := rf.me
			rf.mu.Unlock()
			sendAppendEntriesToFollower(rf, me, accesstionTerm)
			heartbeatTicker := time.NewTicker(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
		PRIMARY_LOOP:
			for {
				select {
				case <-rf.exitCh:
					goto EXIT
				case <-rf.resetCh:
					MyDebug(dInfo, "S%d Leader -> follower", rf.me)
					rf.roleLock.Lock()
					rf.role = ROLE_FOLLOWER
					rf.roleLock.Unlock()
					break PRIMARY_LOOP
				default:
				}
				MyDebug(dInfo, "S%d enter main select", rf.me)
				select {
				case <-heartbeatTicker.C:
					MyDebug(dInfo, "S%d beat to others,term=%d,accessTime=%v,", rf.me, accesstionTerm, accessionTime.Nanosecond())
					sendAppendEntriesToFollower(rf, me, accesstionTerm)
				case <-rf.exitCh:
					goto EXIT
				}
			}
		}
	}
EXIT:
	MyDebug(dWarn, "S%d exit", rf.me)
	return
}

func startElection(rf *Raft, successChan chan bool) {

	var termBefore int
	rf.mu.Lock()
	termBefore = rf.currentTerm
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.mu.Unlock()
	MyDebug(dVote, "S%d termBefore=%d,electionTerm=%d", rf.me, termBefore, rf.currentTerm)

	//1.向所有peers requestVote
	//2. if voteCount is majority
	//    	成为leader
	//   if voteTimeOut
	//      发起下一轮选举
	//   if received apppendEntries. 且leader.term>=currentterm
	//      停止选举，成为follower
	count := 1
	countLock := sync.Mutex{}
	finished := 0
	cond := sync.NewCond(&countLock)
	request := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		LastLogIdx:  rf.lastLog.Idx,
		LastLogTerm: rf.lastLog.Term,
	}
	//maxReceivedTerm := rf.currentTerm
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				response := RequestVoteReply{}
				success := rf.sendRequestVote(server, &request, &response)
				countLock.Lock()

				if success && response.VoteGranted {
					count++
				}
				finished++
				// if response.term > maxReceivedTerm {
				// 	MyDebug(dError, "S%d receive higher term during election", rf.me)
				// 	maxReceivedTerm = response.term
				// }
				cond.Broadcast()
				countLock.Unlock()
			}(i)
		}
	}

	halfNumber := len(rf.peers) / 2
	countLock.Lock()
	for count <= halfNumber && finished != 10 {
		cond.Wait()
	}
	if count > halfNumber {
		successChan <- true
	}
	countLock.Unlock()
}

func sendAppendEntriesToFollower(rf *Raft, me int, currentTerm int) {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     me,
				PrevLogIdx:   0,
				PrevLogTerm:  0,
				LeaderCommit: rf.commitIdx,
			}
			response := &AppendEntriesReply{}
			for {
				if success := rf.sendAppendEntries(server, args, response); success {
					//
					break
				}
			}
		}(i)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.lastLog = logEntry{
		Term: 0,
		Idx:  0,
		Data: nil,
	}
	rf.commitIdx = 0
	rf.lastApplied = 0
	rf.nextidx = make([]int, len(peers))
	rf.matchidx = make([]int, len(peers))
	rf.resetCh = make(chan bool)
	rf.electionTicker = time.NewTicker(time.Duration(10) * time.Hour)
	rf.role = ROLE_FOLLOWER
	rf.roleLock = sync.RWMutex{}
	rf.exitCh = make(chan bool)
	//TODELETE:
	rf.diskLog = append(rf.diskLog, logEntry{})

	//配置log
	file := "log"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
