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
	"bytes"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"

	"github.com/sasha-s/go-deadlock"
)

const HEARTBEAT_INTERVAL = 40
const ROLE_FOLLOWER = 1
const ROLE_CANDIDATE = 2
const ROLE_PRIMARY = 3

// ElectionTimeout (base - base + interval*ratio)
const ELECTION_TIMEOUT_BASE = 400 // millisecond
const ELECTION_TIMEOUT_INTERVAl = 60
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

type logEntry struct {
	Idx  int
	Term int
	//raft底层协议，存的data不必识别，所以要么是byte[]，要么是interface
	Data interface{}
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

	commitIdx   int
	lastApplied int

	//yo record follower's log state
	nextidx  []int
	matchidx []int

	//for role control
	followerCh     chan bool
	electionTicker *time.Ticker
	role           int
	roleLock       deadlock.RWMutex

	//diskLog
	diskLog      []logEntry
	diskLogIndex int
	//TODO:看看别的exit怎么检测的
	exitCh chan bool

	catchUpWorkers []catchUpWorker

	commitUpdater commitUpdater
	applyCh       chan ApplyMsg
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

	//这个函数必须在lock里面调用
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	currentTerm := rf.currentTerm
	voteFor := rf.voteFor
	diskLog := rf.diskLog
	diskLogIndex := rf.diskLogIndex
	commitIndex := rf.commitIdx

	e.Encode(currentTerm)
	e.Encode(voteFor)
	e.Encode(diskLog)
	e.Encode(diskLogIndex)
	e.Encode(commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor, diskLogIndex, commitIdx int
	var diskLog []logEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&diskLog) != nil ||
		d.Decode(&diskLogIndex) != nil ||
		d.Decode(&commitIdx) != nil {
		MyDebug(dError, "decode err")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.diskLog = diskLog
		rf.diskLogIndex = diskLogIndex
		rf.commitIdx = commitIdx
		MyDebug(dInfo, "S%v restore cTerm=%v,lIdx=%v,cmt=%v", rf.me, currentTerm, diskLogIndex, commitIdx)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch {
	case args.Term < rf.currentTerm:
		MyDebug(dVote, "S%d reject vote for %d, stale term", rf.me, args.CandidateID)
		goto REJECT_VOTE
	case args.Term == rf.currentTerm:
		if rf.voteFor != -1 && rf.voteFor != args.CandidateID {
			MyDebug(dVote, "S%d reject vote for %d, voted", rf.me, args.CandidateID)
			goto REJECT_VOTE
		}
	case args.Term > rf.currentTerm:
		rf.currentTerm = args.Term
		rf.persist()
	}

	//update-to-date as receiver这个条件的判断
	if rf.diskLog[rf.diskLogIndex].Term < args.LastLogTerm ||
		(rf.diskLog[rf.diskLogIndex].Term == args.LastLogTerm && rf.diskLogIndex <= args.LastLogIdx) {
		//走到这个term相同，则一定没投过票，或就是投给了这个Leader，要么LeaderTerm>currentTerm
		MyDebug(dVote, "S%d LastLogTerm=%v,lastLogIdex=%v", rf.me, args.LastLogTerm, args.LastLogIdx)
		//TODO：这个挪到上面去
		rf.voteFor = args.CandidateID
		rf.persist()

		//如果当前是primary，收到VoteRequest也要stopDown

		MyDebug(dVote, "S%d vote and reset", rf.me)
		rf.sendSetToFollowerSignal()
		MyDebug(dVote, "S%d vote for %d", rf.me, args.CandidateID)

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
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

	Xterm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//prevLogIdex mismatch
	MyDebug(dInfo, "S%d receive AE from %v,PLIdx=%v PLTerm=%v", rf.me, args.LeaderID, args.PrevLogIdx, args.PrevLogTerm)

	rf.currentTerm = args.Term
	rf.voteFor = args.LeaderID
	rf.persist()

	//只有currentTerm<=args.Term的时候，才会发送reset信号
	//currentTerm==args.Term，证明是follower
	//currentTerm<args.Term，不用说肯定是落后了
	if args.LogEntries == nil {
		rf.sendSetToFollowerSignal()
		if args.PrevLogTerm != args.Term {
			reply.Term = rf.currentTerm
			reply.Success = true
			MyDebug(dInfo, "S%d receive heartbeat, but no sync", rf.me)
			MyDebug(dInfo, "S%d from S%v,term=%v,PLTerm=%v,PLIdx=%v", rf.me, args.LeaderID, args.Term, args.PrevLogIdx, args.PrevLogIdx)
			return
		}
	}

	if args.PrevLogIdx > rf.diskLogIndex || rf.diskLog[args.PrevLogIdx].Term != args.PrevLogTerm {
		reply.XIndex, reply.Xterm, reply.XLen = rf.getExpectedTermInfoLocked(args.PrevLogIdx)
		reply.Success = false
		MyDebug(dVote, "S%d expect XIndex=%d len=%v xterm=%v", rf.me, reply.XIndex, reply.XLen, reply.Xterm)
		return
	}

	if upToDateAndNoNeedToAppend(rf, args) {
		rf.TryToAdjustCommitIndexLocked(args.LeaderCommit)
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	rf.applyLogEntriesToDiskLocked(args.LogEntries)
	rf.TryToAdjustCommitIndexLocked(args.LeaderCommit)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) getExpectedTermInfoLocked(prevLogIdex int) (int, int, int) {
	var lastIndex int
	if prevLogIdex > rf.diskLogIndex {
		lastIndex = rf.diskLogIndex
	} else {
		lastIndex = prevLogIdex
	}

	var xIndex int
	for xIndex = lastIndex; xIndex >= 1 && rf.diskLog[xIndex].Term == rf.diskLog[lastIndex].Term; xIndex-- {
	}
	if xIndex == 0 && rf.diskLogIndex == 0 {
		return xIndex + 1, 0, 1
	}
	xIndex++
	return xIndex, rf.diskLog[xIndex].Term, (lastIndex - xIndex + 1)
}

func (rf *Raft) TryToAdjustCommitIndexLocked(leaderCommit int) {
	//TODO 这里可以优化成updateCommit()函数
	if leaderCommit > rf.commitIdx {
		commitIndexBefore := rf.commitIdx
		for logIndex := rf.commitIdx + 1; logIndex <= leaderCommit &&
			logIndex <= rf.diskLogIndex; logIndex++ {

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: logIndex,
				Command:      rf.diskLog[logIndex].Data,
			}
			rf.commitIdx = logIndex

		}
		rf.persist()
		MyDebug(dCommit, "S%d commitIdx:%v->%v", rf.me, commitIndexBefore, rf.commitIdx)
	} else {
		MyDebug(dCommit, "S%d commitIdx:%v", rf.me, rf.commitIdx)
	}
}

func (rf *Raft) applyLogEntriesToDiskLocked(logEntries []logEntry) {
	if logEntries == nil {
		return
	}
	tailLogIndex := logEntries[len(logEntries)-1].Idx
	if tailLogIndex <= rf.diskLogIndex && rf.diskLog[tailLogIndex].Term == logEntries[0].Term {
		return
	}

	logIndex := logEntries[0].Idx
	transferIndex := 0
	for ; logIndex < len(rf.diskLog) && transferIndex < len(logEntries); transferIndex, logIndex = transferIndex+1, logIndex+1 {
		rf.diskLog[logIndex] = logEntries[transferIndex]
	}
	if transferIndex < len(logEntries) {
		rf.diskLog = append(rf.diskLog, logEntries[transferIndex:]...)
		logIndex = len(rf.diskLog)
	}

	rf.diskLogIndex = logIndex - 1
	rf.diskLog = rf.diskLog[:logIndex]
	rf.persist()
	MyDebug(dCommit, "S%d disLogIndex->%v value=%v", rf.me, rf.diskLogIndex, rf.diskLog[logIndex-1].Data)
}

func upToDateAndNoNeedToAppend(rf *Raft, info *AppendEntriesArgs) bool {
	return info.PrevLogIdx == rf.diskLogIndex && info.LogEntries == nil
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
	MyDebug(dCommit, "S%d getCurrentTerm", rf.me)
	term = rf.getCurrentTerm()
	MyDebug(dCommit, "S%d start getRole", rf.me)
	currentRole := rf.getRole()
	if currentRole != ROLE_PRIMARY {
		isLeader = false
		return index, term, isLeader
	}
	if currentRole == ROLE_PRIMARY {
		isLeader = true
	}

	logEntry := rf.persistToDisk(command)
	MyDebug(dCommit, "S%d start %v logIndex=%v", rf.me, command, logEntry.Idx)
	index = logEntry.Idx

	rf.tryToCommit(logEntry)
	// commitCh:=rf.tryToCommit(logEntry)
	// <-commitCh
	//OPTIMIZE:这里应该不用定时，因为机器挂掉TCP自动返回错误
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
	rf.exitCh <- true
	rf.commitUpdater.stop()
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	MyDebug(dInfo, "S%d service killed", rf.me)
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
		if rf.getRole() == ROLE_FOLLOWER {
			//server间，electionTimeOut的interval最好大于heartbeat的时间
			rand.Seed(int64(rf.me) + time.Now().Unix())
			timeOut := (ELECTION_TIMEOUT_BASE + (rand.Int()%ELECTION_TIMEOUT_RATIO)*ELECTION_TIMEOUT_INTERVAl)
			//这里必须用一个NewTicker，因为如果因为网络原因没有连接到其他server，原来的ticker里面会积累很多message
			//ticker就失去了作用
			electionTicker := time.NewTicker(time.Duration(timeOut) * time.Millisecond)
			//rf.electionTicker = time.NewTicker(time.Duration(timeOut) * time.Millisecond)

		FOLLOWER_LOOP:
			for {
				select {
				case <-rf.exitCh:
					goto EXIT
				default:
				}

				select {
				case <-rf.followerCh:
					timeOut := (ELECTION_TIMEOUT_BASE + (rand.Int()%ELECTION_TIMEOUT_RATIO)*ELECTION_TIMEOUT_INTERVAl)
					electionTicker.Reset(time.Duration(timeOut) * time.Millisecond)
					//MyDebug(dInfo, "S%d reset, follower -> follower", rf.me)
				case <-electionTicker.C:
					MyDebug(dVote, "S%d electionTimeout,follower -> candidate", rf.me)
					rf.setRole(ROLE_CANDIDATE)
					break FOLLOWER_LOOP
				case <-rf.exitCh:
					goto EXIT
				}

			}
		} else if rf.getRole() == ROLE_CANDIDATE {
			timeOut := (ELECTION_TIMEOUT_BASE + (rand.Int()%ELECTION_TIMEOUT_RATIO)*ELECTION_TIMEOUT_INTERVAl)
			electionTicker := time.NewTicker(time.Duration(timeOut) * time.Millisecond)
			resultChan := make(chan int)
			go startElection(rf, resultChan)

			select {
			case result := <-resultChan:
				if result < 0 {
					MyDebug(dVote, "S%d win the election, candidate -> Leader", rf.me)
					rf.setRole(ROLE_PRIMARY)
					break
				}
				rf.setRole(ROLE_FOLLOWER)
			case <-electionTicker.C:
				MyDebug(dInfo, "S%d candidate electionTimeout,restart election", rf.me)
			case <-rf.followerCh:
				rf.setRole(ROLE_FOLLOWER)
			case <-rf.exitCh:
				goto EXIT
			}
		} else if rf.getRole() == ROLE_PRIMARY {
			//为了保证heartBeatToOthers的时候，防止因为接受了appendEntry改变了term，然后才运行到heartBeart，这样heartBeat出去的就是新接受的term，导致发出appendEntry的
			//Leader退位。而Leader上位之后，term应该是不变的，因为立刻提取出来
			accessionTime := time.Now()
			rf.mu.Lock()
			rf.commitUpdater.setCommitCriterial(rf.diskLogIndex)
			accesstionTerm := rf.currentTerm
			for i := 0; i < len(rf.nextidx); i++ {
				if i == rf.me {
					rf.nextidx[i] = rf.diskLogIndex + 1
					rf.matchidx[i] = rf.diskLogIndex
					continue
				}
				rf.nextidx[i] = rf.diskLogIndex + 1
				rf.matchidx[i] = 0
			}
			rf.mu.Unlock()
			//[Question] 我记得Morres说may not send heartbeat when Leader come to power

			//把已有的followerCh消耗干净
			select {
			case <-rf.followerCh:
			default:
			}

			rf.heartbeatToFollowers()
			heartbeatTicker := time.NewTicker(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
		PRIMARY_LOOP:
			for {
				select {
				case <-rf.exitCh:
					goto EXIT
				default:
				}

				select {
				case <-rf.exitCh:
					goto EXIT
				case <-rf.followerCh:
					MyDebug(dInfo, "S%d Leader -> follower", rf.me)
					rf.setRole(ROLE_FOLLOWER)
					break PRIMARY_LOOP
				case <-heartbeatTicker.C:
					MyDebug(dInfo, "S%d beat to others,term=%d,accessTime=%v,", rf.me, accesstionTerm, accessionTime.Nanosecond())
					rf.heartbeatToFollowers()
				}
			}
		}
	}
EXIT:
	MyDebug(dWarn, "S%d exit ticker", rf.me)
}

func startElection(rf *Raft, resultChan chan int) {
	var termBefore int
	var currentTerm int
	rf.mu.Lock()
	termBefore = rf.currentTerm
	rf.currentTerm++
	rf.voteFor = rf.me
	me := rf.me
	currentTerm = rf.currentTerm
	lastLogIndex := rf.diskLogIndex
	LastLogTerm := rf.diskLog[lastLogIndex].Term
	rf.mu.Unlock()
	MyDebug(dVote, "S%d termBefore=%d,electionTerm=%d", rf.me, termBefore, currentTerm)

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
		Term:        currentTerm,
		CandidateID: me,
		LastLogIdx:  lastLogIndex,
		LastLogTerm: LastLogTerm,
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
				} else if response.Term > currentTerm {
					MyDebug(dVote, "S%d more up-to-date term,candidate->follower", rf.me)
					rf.mu.Lock()
					rf.currentTerm = response.Term
					rf.persist()
					rf.mu.Unlock()
					resultChan <- response.Term
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
		resultChan <- -1
	}
	countLock.Unlock()
}

func (rf *Raft) heartbeatToFollowers() {
	rf.sendAppendEntriesToFollowers(nil)
}

func (rf *Raft) SendAppendEntriesToFollower(server int, logEntries []logEntry) {
	//MyDebug(dCommit, "S%v new AppendEntries to s%v", rf.me, server)
	args := getAppendEntriesArgs(rf, server, logEntries)
	for {
		if rf.killed() || rf.getRole() != ROLE_PRIMARY {
			//MyDebug(dCommit, "S%v retry AE s%v,PLIdx=%v,JUMP out", rf.me, server, args.PrevLogIdx)
			return
		}

		//MyDebug(dCommit, "S%v try AE s%v,PLIdx=%v,pTerm=%v", rf.me, server, args.PrevLogIdx, args.PrevLogTerm)
		response := AppendEntriesReply{}
		if success := rf.sendAppendEntries(server, args, &response); success {
			term := rf.getCurrentTerm()
			if term == args.Term {
				dealWithAppendEntriesResponse(rf, &response, server, args, term)
			}
			break
		}
	}
}

func dealWithAppendEntriesResponse(rf *Raft, response *AppendEntriesReply, server int, args *AppendEntriesArgs, currentTerm int) {
	if !response.Success {
		if response.Term > args.Term {
			MyDebug(dVote, "S%v to follower,s%v term=%v", rf.me, server, response.Term)
			rf.sendSetToFollowerSignal()
			return
		}

		nextIndex := getNextIndexLocked(rf, response)
		if nextIndex <= rf.nextidx[server] {
			rf.nextidx[server] = nextIndex
			if nextIndex < rf.nextidx[server] {
				MyDebug(dCommit, "S%v s%v.nextIndex->%v", rf.me, server, nextIndex)
			}
			rf.catchUpWorkers[server].sendCatchUpSignal(nextIndex)
		}
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		var matchIndex, nextIndex int
		if args.LogEntries != nil {
			last := len(args.LogEntries) - 1
			nextIndex = args.LogEntries[last].Idx + 1
			matchIndex = args.LogEntries[last].Idx
		} else {
			matchIndex = args.PrevLogIdx
			nextIndex = args.PrevLogIdx + 1
		}

		if matchIndex > rf.matchidx[server] {
			MyDebug(dCommit, "S%d s%v.matchidx->%v", rf.me, server, matchIndex)
			rf.matchidx[server] = matchIndex
		}
		if rf.nextidx[server] < nextIndex {
			rf.nextidx[server] = nextIndex
			rf.matchidx[server] = nextIndex - 1
			MyDebug(dCommit, "S%d s%v.nextIndex->%v", rf.me, server, nextIndex)

			rf.commitUpdater.addUpdatedFollower(server)
		}

	}
}

func getNextIndexLocked(rf *Raft, XInfo *AppendEntriesReply) int {
	xTerm := XInfo.Xterm
	xIndex := XInfo.XIndex
	xLen := XInfo.XLen
	xTailIndex := xIndex + xLen - 1
	//MyDebug(dCommit, "S%d AE res:xIdx=%v,xlen=%v,xTerm=%v", rf.me, XInfo.XIndex, XInfo.XLen, XInfo.Xterm)
	switch {
	case xTailIndex <= rf.diskLogIndex && rf.diskLog[xTailIndex].Term == xTerm:
		return xTailIndex + 1
	case rf.diskLog[xIndex].Term == xTerm:
		nextOne := xIndex
		for ; nextOne <= rf.diskLogIndex && rf.diskLog[nextOne].Term == xTerm; nextOne++ {
		}
		return nextOne
	default:
		return xIndex
	}
}

func getAppendEntriesArgs(rf *Raft, server int, logEntries []logEntry) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	leaderID := rf.me
	var prevLogIdx, prevLogTerm int

	if logEntries == nil {
		prevLogIdx = rf.diskLogIndex
		//prevLogIdx = rf.nextidx[server] - 1
	} else {
		prevLogIdx = logEntries[0].Idx - 1
	}
	//MyDebug(dCommit, "S%v prevLogIdex=%v", rf.me, prevLogIdx)
	prevLogTerm = rf.diskLog[prevLogIdx].Term
	commitIdx := rf.commitIdx

	return &AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIdx:   prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: commitIdx,
		LogEntries:   logEntries,
	}
}

type pollerGroup struct {
	sync.Mutex
	pollers []*commitPoller
}

func (wg *pollerGroup) Add(w *commitPoller) {
	wg.Lock()
	defer wg.Unlock()

	wg.pollers = append(wg.pollers, w)
}

func (wg *pollerGroup) Iter(routine func(*commitPoller)) {
	wg.Lock()
	wg.Unlock()

	for _, worker := range wg.pollers {
		routine(worker)
	}
}

func (wg *pollerGroup) gc() {
	wg.Lock()
	defer wg.Unlock()

	if len(wg.pollers) < 100 {
		return
	}

	var newPoller []*commitPoller
	for i := 0; i < len(wg.pollers); i++ {
		if wg.pollers[i].liveness {
			newPoller = append(newPoller, wg.pollers[i])
		}
	}
	wg.pollers = newPoller
}

type commitPoller struct {
	doneCh   chan int
	criteria int
	liveness bool
}

func NewCommitPoller(criterial int, doneCh chan int) *commitPoller {
	return &commitPoller{
		doneCh:   doneCh,
		criteria: criterial,
		liveness: true,
	}
}

func (c *commitPoller) isLive() bool {
	return c.liveness
}

func (rf *Raft) tryToCommit(data logEntry) chan int {
	doneCh := make(chan int, 1)
	rf.commitUpdater.addPoller(data.Idx, doneCh)

	var logEntries []logEntry
	logEntries = append(logEntries, data)
	rf.sendAppendEntriesToFollowers(logEntries)
	return doneCh

}

func (rf *Raft) persistToDisk(command interface{}) logEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.diskLogIndex + 1
	// data, ok := command.(int)
	// if !ok {
	// 	MyDebug(dTrace, "S%d command convert fail %v", rf.me)
	// }

	logEntry := logEntry{
		Term: rf.currentTerm,
		Idx:  index,
		Data: command,
	}
	if index == len(rf.diskLog) {
		rf.diskLog = append(rf.diskLog, logEntry)
	} else {
		rf.diskLog[index] = logEntry
	}

	rf.diskLogIndex = index
	rf.nextidx[rf.me] = index + 1
	rf.matchidx[rf.me] = index
	rf.persist()
	return logEntry
}

func (rf *Raft) sendAppendEntriesToFollowers(data []logEntry) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.SendAppendEntriesToFollower(i, data)
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
	if rf.persister.RaftStateSize() == 0 {
		rf.currentTerm = 0
		rf.voteFor = -1
		rf.diskLog = append(rf.diskLog, logEntry{})
		rf.diskLogIndex = 0
		rf.commitIdx = 0
		rf.lastApplied = 0
	}

	rf.nextidx = make([]int, len(peers))
	rf.matchidx = make([]int, len(peers))
	rf.followerCh = make(chan bool)
	rf.electionTicker = time.NewTicker(time.Duration(10) * time.Hour)
	rf.role = ROLE_FOLLOWER
	rf.roleLock = deadlock.RWMutex{}
	rf.exitCh = make(chan bool)
	for i := 0; i < len(rf.peers); i++ {
		worker := NewCatchUpWorker(rf, i)
		worker.start()
		rf.catchUpWorkers = append(rf.catchUpWorkers, *worker)
	}
	quorom := len(rf.peers)/2 + 1
	rf.commitUpdater = *newCommitUpdater(rf, quorom)
	rf.commitUpdater.start()
	rf.applyCh = applyCh

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
	deadlock.Opts.DeadlockTimeout = 3*time.Second
	
	go rf.ticker()

	return rf
}

// catchup
type catchUpWorker struct {
	ch         chan int
	rf         *Raft
	followerID int
}

func NewCatchUpWorker(rf *Raft, server int) *catchUpWorker {
	return &catchUpWorker{
		ch:         make(chan int, 100),
		rf:         rf,
		followerID: server,
	}
}
func (w *catchUpWorker) start() {
	go func() {
		server := w.followerID
		for {
			if w.rf.killed() {
				MyDebug(dTest, "S%d catup worker stop", w.rf.me)
				return
			}

			minIndex := getMinIndex(w.ch, w.rf.getMatchIndex(server))
			nextIndex := w.rf.getNextIndex(server)
			if minIndex < nextIndex {
				nextIndex = minIndex
			}
			if nextIndex <= w.rf.getDiskLogIndex() {
				data := generateNextCatchUpLogEntries(w.rf, nextIndex)
				MyDebug(dTrace, "S%d backup[%v] logIndex[%v,%v]", w.rf.me, w.followerID, data[0].Idx, data[len(data)-1].Idx)
				w.rf.SendAppendEntriesToFollower(w.followerID, data)
				MyDebug(dTrace, "S%d backup[%v] logIndex[%v,%v] finish", w.rf.me, w.followerID, data[0].Idx, data[len(data)-1].Idx)
			}
		}
	}()
}

func getMinIndex(ch chan int, matchIndex int) int {
	nextIndexs := make([]int, 0, 100)
	nextIndexs = append(nextIndexs, <-ch)
	lenCh := len(ch)
	for i := 0; i < lenCh; i++ {
		nextIndexs = append(nextIndexs, <-ch)
	}
	sort.Ints(nextIndexs)
	for _, idx := range nextIndexs {
		if idx > matchIndex {
			return idx
		}
	}
	return matchIndex + 1
}

func (w *catchUpWorker) sendCatchUpSignal(nextIndex int) {
	if w.rf.getRole() == ROLE_PRIMARY {
		select {
		case w.ch <- nextIndex:
		default:
			MyDebug(dVote, "S%d fail to sent catch-up-signal to s%v", w.rf.me, w.followerID)
		}
	} else {
		// MyDebug(dVote, "S%d fail sent catch-up-signal, no longer Leader", w.rf.me)
	}
}
func generateNextCatchUpLogEntries(rf *Raft, startIndex int) []logEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex := startIndex
	for ; lastIndex < len(rf.diskLog) && rf.diskLog[lastIndex].Term == rf.diskLog[startIndex].Term; lastIndex++ {
	}
	return rf.diskLog[startIndex:lastIndex]
}

type commitUpdater struct {
	deadlock.RWMutex
	updatedFollower map[int]bool
	state           int32
	quorom          int
	exitCh          chan bool
	rf              *Raft
	criteria        int
	pollers         pollerGroup
}

const COMMIT_CHECK_INTERVAL = 30
const QUORUM = 0

func newCommitUpdater(rf *Raft, quorum int) *commitUpdater {
	return &commitUpdater{
		quorom:          quorum,
		rf:              rf,
		updatedFollower: make(map[int]bool),
		exitCh:          make(chan bool),
		state:           0,
		pollers:         pollerGroup{},
	}
}

// TDDO:怎样快速优雅地关闭一个服务？
func (c *commitUpdater) start() {
	go func() {
		atomic.AddInt32(&c.state, 1)
		ticker := time.NewTicker(time.Duration(COMMIT_CHECK_INTERVAL) * time.Millisecond)
		for {
			if c.state > 1 {
				c.exitCh <- true
				MyDebug(dTrace, "S%d commitupdater killed", c.rf.me)
				return
			}
			<-ticker.C
			c.checkCommitUpdate()
		}
	}()
}

func (c *commitUpdater) stop() {
	atomic.AddInt32(&c.state, 1)
	//<-c.exitCh
	MyDebug(dTrace, "S%d commitupdater killed", c.rf.me)
}

func (c *commitUpdater) checkCommitUpdate() {
	c.Lock()
	defer c.Unlock()
	if len(c.updatedFollower)+1 >= c.quorom {
		commitIndex := c.rf.updateCommitIndex(c.quorom, c.criteria)
		if commitIndex > 0 {
			c.updatedFollower = make(map[int]bool)
			c.pollers.Iter(func(w *commitPoller) {
				if w.isLive() && w.criteria >= commitIndex {
					w.doneCh <- commitIndex
					w.liveness = false
				}
			})
			c.pollers.gc()
		}
	}
}

func (c *commitUpdater) addUpdatedFollower(follower int) {
	c.Lock()
	defer c.Unlock()
	c.updatedFollower[follower] = true
}

func (c *commitUpdater) setCommitCriterial(commitCriterial int) {
	c.Lock()
	defer c.Unlock()
	MyDebug(dCommit, "S%d commitCriterial=%v", c.rf.me, commitCriterial)
	c.criteria = commitCriterial
}

func (c *commitUpdater) getCommitCriteria() int {
	c.Lock()
	defer c.Unlock()
	return c.criteria
}

func (c *commitUpdater) getQuorom() int {
	c.Lock()
	defer c.Unlock()
	return c.criteria
}

func (c *commitUpdater) addPoller(logIndex int, doneCh chan int) {
	c.pollers.Add(NewCommitPoller(logIndex, doneCh))
}

func (rf *Raft) updateCommitIndex(quorom int, commitCriterial int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var matchidxs []int
	matchidxs = append(matchidxs, rf.matchidx...)

	sort.Slice(matchidxs, func(i, j int) bool {
		return matchidxs[j] < matchidxs[i]
	})

	commitIndex := matchidxs[quorom-1]
	if commitIndex > rf.commitIdx && commitIndex > commitCriterial {
		for logIndex := rf.commitIdx + 1; logIndex <= commitIndex; logIndex++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: logIndex,
				Command:      rf.diskLog[logIndex].Data,
			}
		}
		MyDebug(dCommit, "S%d commitIndex:%v->%v", rf.me, rf.commitIdx, commitIndex)
		rf.commitIdx = commitIndex
		rf.persist()
		return commitIndex
	} else if commitIndex == rf.commitIdx {
		return -1
	} else {
		//打印错误日志
		return -1
	}
}

func (rf *Raft) getRole() int {
	rf.roleLock.RLock()
	defer rf.roleLock.RUnlock()

	return rf.role
}

func (rf *Raft) setRole(role int) {
	rf.roleLock.Lock()
	defer rf.roleLock.Unlock()

	rf.role = role
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getNextIndex(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextidx[server]
}

func (rf *Raft) getMatchIndex(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchidx[server]
}

func (rf *Raft) getDiskLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.diskLogIndex
}

func (rf *Raft) sendSetToFollowerSignal() {
	select {
	case rf.followerCh <- true:
	default:
	}
}

func (rf *Raft) showRaftInfo() {
	rf.showDiskLog()
}
func (rf *Raft) showDiskLog() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	MyDebug(dTrace, "S%v diskLog:%v", rf.me, rf.diskLog)
}
