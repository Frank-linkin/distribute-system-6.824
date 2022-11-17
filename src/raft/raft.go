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
	"sort"
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

	//[Question] commitIndex和lastApplied分别是啥意思？
	commitIdx   int
	lastApplied int

	//yo record follower's log state
	nextidx  []int
	matchidx []int

	//for role control
	followerCh     chan bool
	electionTicker *time.Ticker
	role           int
	roleLock       sync.RWMutex

	//diskLog
	diskLog      []logEntry
	diskLogIndex int
	//TODO:看看别的exit怎么检测的
	exitCh chan bool

	catchUpWorkers []catchUpWorker
	commitPollers  pollerGroup
	commitUpdater  commitUpdater
	applyCh        chan ApplyMsg
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	//update-to-date as receiver这个条件的判断
	if rf.diskLog[rf.diskLogIndex].Term < args.LastLogTerm ||
		(rf.diskLog[rf.diskLogIndex].Term == args.LastLogTerm && rf.diskLogIndex <= args.LastLogIdx) {
		//走到这个term相同，则一定没投过票，或就是投给了这个Leader，要么LeaderTerm>currentTerm
		rf.voteFor = args.CandidateID
		rf.currentTerm = args.Term
		//如果当前是primary，收到VoteRequest也要stopDown

		MyDebug(dVote, "S%d vote and reset", rf.me)
		rf.followerCh <- true
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

	rf.currentTerm = args.Term
	rf.voteFor = args.LeaderID

	//只有currentTerm<=args.Term的时候，才会发送reset信号
	//currentTerm==args.Term，证明是follower
	//currentTerm<args.Term，不用说肯定是落后了
	if args.LogEntries == nil {
		rf.followerCh <- true
		if args.PrevLogTerm != args.Term {
			reply.Term = rf.currentTerm
			reply.Success = true
			MyDebug(dInfo, "S%d receive heartbeat, but no sync", rf.me)
			return
		}
	}

	//prevLogIdex mismatch
	MyDebug(dVote, "S%d appendEntrie,PrevLogIdx=%v,PrevlogTerm=%v", rf.me, args.PrevLogIdx)
	MyDebug(dVote, "S%d appendEntrie,PrevlogTerm=%v", rf.me, args.PrevLogTerm)
	
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
		MyDebug(dCommit, "S%d commitIdx:%v->%v", rf.me, commitIndexBefore, rf.commitIdx)
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
	rf.roleLock.RLock()
	rf.mu.Lock()
	term = rf.currentTerm
	rf.mu.Unlock()
	currentRole := rf.role
	rf.roleLock.RUnlock()
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
	MyDebug(dTest, "S%d service killed", rf.me)
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
		if rf.role == ROLE_FOLLOWER {
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
				case <-rf.followerCh:
					timeOut := (ELECTION_TIMEOUT_BASE + (rand.Int()%ELECTION_TIMEOUT_RATIO)*ELECTION_TIMEOUT_INTERVAl)
					electionTicker.Reset(time.Duration(timeOut) * time.Millisecond)
					MyDebug(dInfo, "S%d reset, follower -> follower", rf.me)
				case <-electionTicker.C:
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
			timeOut := (ELECTION_TIMEOUT_BASE + (rand.Int()%ELECTION_TIMEOUT_RATIO)*ELECTION_TIMEOUT_INTERVAl)
			electionTicker := time.NewTicker(time.Duration(timeOut) * time.Millisecond)
			resultChan := make(chan int)
			go startElection(rf, resultChan)
			// select {
			// case <-rf.exitCh:
			// 	goto EXIT
			// default:
			// }

			select {
			case result := <-resultChan:
				if result < 0 {
					MyDebug(dVote, "S%d win the election, candidate -> Leader", rf.me)
					rf.roleLock.Lock()
					rf.role = ROLE_PRIMARY
					rf.roleLock.Unlock()
					break
				}
				rf.roleLock.Lock()
				rf.role = ROLE_FOLLOWER
				rf.roleLock.Unlock()
			case <-electionTicker.C:
				MyDebug(dInfo, "S%d candidate electionTimeout,restart election", rf.me)
			case <-rf.followerCh:
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
			for i := 0; i < len(rf.nextidx); i++ {
				if i == rf.me {
					continue
				}
				rf.nextidx[i] = rf.commitIdx + 1
			}
			rf.mu.Unlock()
			//[Question] 我记得Morres说may not send heartbeat when Leader come to power
			rf.heartbeatToFollowers()
			heartbeatTicker := time.NewTicker(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
		PRIMARY_LOOP:
			for {
				select {
				case <-rf.exitCh:
					goto EXIT
				case <-rf.followerCh:
					MyDebug(dInfo, "S%d Leader -> follower", rf.me)
					rf.roleLock.Lock()
					rf.role = ROLE_FOLLOWER
					rf.roleLock.Unlock()
					break PRIMARY_LOOP
				case <-heartbeatTicker.C:
					MyDebug(dInfo, "S%d beat to others,term=%d,accessTime=%v,", rf.me, accesstionTerm, accessionTime.Nanosecond())
					rf.heartbeatToFollowers()
				}
			}
		}
	}
EXIT:
	MyDebug(dWarn, "S%d exit", rf.me)
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
					rf.currentTerm = response.Term
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
	args := getAppendEntriesArgs(rf, server, logEntries)
	for {
		if rf.killed() {
			return
		}
		response := &AppendEntriesReply{}
		if success := rf.sendAppendEntries(server, args, response); success {
			if !response.Success {
				if response.Term > args.Term {
					rf.followerCh <- true
					return
				}

				nextIndex := getNextIndexLocked(rf, response)
				if nextIndex <= rf.nextidx[server] {
					rf.nextidx[server] = nextIndex
					MyDebug(dCommit, "S%v s%v.nextIndex->%v", rf.me, server, nextIndex)
					rf.catchUpWorkers[server].sendCatchUpSignal(nextIndex)
				}
			} else if logEntries != nil {
				last := len(logEntries) - 1
				nextIndex := logEntries[last].Idx + 1
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.nextidx[server] < nextIndex {
					rf.nextidx[server] = nextIndex
					MyDebug(dCommit, "S%d s%v.nextIndex->%v", rf.me, server, nextIndex)

					rf.commitUpdater.addUpdatedFollower(server)
				}
			}
			break
		}
	}
}

func getNextIndexLocked(rf *Raft, XInfo *AppendEntriesReply) int {
	xTerm := XInfo.Xterm
	xIndex := XInfo.XIndex
	xLen := XInfo.XLen
	xTailIndex := xIndex + xLen - 1
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
	rf.commitPollers.Add(NewCommitPoller(data.Idx, doneCh))

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
	rf.currentTerm = 0
	rf.voteFor = -1

	rf.commitIdx = 0
	rf.lastApplied = 0
	rf.nextidx = make([]int, len(peers))
	rf.matchidx = make([]int, len(peers))
	rf.followerCh = make(chan bool)
	rf.electionTicker = time.NewTicker(time.Duration(10) * time.Hour)
	rf.role = ROLE_FOLLOWER
	rf.roleLock = sync.RWMutex{}
	rf.diskLog = append(rf.diskLog, logEntry{})
	rf.exitCh = make(chan bool)
	rf.commitPollers = pollerGroup{}
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
		ch:         make(chan int, 10),
		rf:         rf,
		followerID: server,
	}
}
func (w *catchUpWorker) start() {
	go func() {
		for {
			if w.rf.killed() {
				MyDebug(dTest, "S%d catup worker stop", w.rf.me)
				return
			}
			nextIndexs := make([]int, 0, 10)

			nextIndex := <-w.ch

			nextIndexs = append(nextIndexs, nextIndex)
			lenCh := len(w.ch)
			for i := 0; i < lenCh; i++ {
				nextIndexs = append(nextIndexs, <-w.ch)
			}
			sort.Ints(nextIndexs)
			nextIndex = nextIndexs[0]
			MyDebug(dTrace, "S%d s%v nextIndex=%v", w.rf.me, w.followerID, nextIndex)

			data := generateNextCatchUpLogEntries(w.rf, nextIndex)
			MyDebug(dTrace, "S%d backup[%v] logIndex[%v,%v]", w.rf.me, w.followerID, data[0].Idx, data[len(data)-1].Idx)
			w.rf.SendAppendEntriesToFollower(w.followerID, data)
		}
	}()
}

func (w *catchUpWorker) sendCatchUpSignal(nextIndex int) {
	select {
	case w.ch <- nextIndex:
	default:
		MyDebug(dVote, "S%d fail to sent catch-up-signal to s%v", w.rf.me, w.followerID)
	}
}
func generateNextCatchUpLogEntries(rf *Raft, nextIndex int) []logEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex := nextIndex
	for ; lastIndex < len(rf.diskLog) && rf.diskLog[lastIndex].Term == rf.diskLog[nextIndex].Term; lastIndex++ {
	}
	return rf.diskLog[nextIndex:lastIndex]
}

type commitUpdater struct {
	sync.RWMutex
	updatedFollower map[int]bool
	state           int32
	quorom          int
	rf              *Raft
}

const COMMIT_CHECK_INTERVAL = 30
const QUORUM = 0

func newCommitUpdater(rf *Raft, quorum int) *commitUpdater {
	return &commitUpdater{
		quorom:          quorum,
		rf:              rf,
		updatedFollower: make(map[int]bool),
		state:           0,
	}
}

// TDDO:怎样快速优雅地关闭一个服务？
func (c *commitUpdater) start() {
	go func() {
		atomic.AddInt32(&c.state, 1)
		ticker := time.NewTicker(time.Duration(COMMIT_CHECK_INTERVAL) * time.Millisecond)
		for {
			if c.state > 1 {
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
}

func (c *commitUpdater) checkCommitUpdate() {
	c.Lock()
	defer c.Unlock()
	if len(c.updatedFollower)+1 >= c.quorom {
		commitIndex := c.rf.updateCommitIndex(c.quorom)
		if commitIndex > 0 {
			c.updatedFollower = make(map[int]bool)
			c.rf.commitPollers.Iter(func(w *commitPoller) {
				if w.isLive() && w.criteria >= commitIndex {
					w.doneCh <- commitIndex
					w.liveness = false
				}
			})
			c.rf.commitPollers.gc()
		}
	}
}

func (c *commitUpdater) addUpdatedFollower(follower int) {
	c.Lock()
	defer c.Unlock()
	c.updatedFollower[follower] = true
}

func (rf *Raft) updateCommitIndex(quorom int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var nextIndexes []int
	nextIndexes = append(nextIndexes, rf.nextidx...)

	sort.Slice(nextIndexes, func(i, j int) bool {
		return nextIndexes[j] < nextIndexes[i]
	})

	commitIndex := nextIndexes[quorom-1] - 1
	if commitIndex > rf.commitIdx {
		for logIndex := rf.commitIdx; logIndex <= commitIndex; logIndex++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: logIndex,
				Command:      rf.diskLog[logIndex].Data,
			}
		}
		MyDebug(dCommit, "S%d commitIndex:%v->%v", rf.me, rf.commitIdx, commitIndex)
		rf.commitIdx = commitIndex
		return commitIndex
	} else if commitIndex == rf.commitIdx {
		return -1
	} else {
		//打印错误日志
		return -1
	}
}
