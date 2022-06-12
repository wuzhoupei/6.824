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
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
	"strconv"
	"sort"
)

const (
	FollowerState  = 0
	CandidateState = 1
	LeaderState    = 2

	// electionWaitTime = 500
	// electionLen      = 250
	// electionTime     = 300
	// electionEach     = 10
	// appendTime       = 150
	// rpcCheckEach     = 75
	
	electionWaitTime = 200
	electionLen      = 150
	electionTime     = 150
	electionEach     = 10
	appendTime       = 15
	rpcCheckEach     = 10
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type LogNodes struct {
	Term int
	Log interface{}
}

type aCounter struct {
	cnt int
	mu sync.Mutex 
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int
	logs        []LogNodes

	commandIndex int
	lastApplied  int

	nextIndex  []int
	matchIndex []int
	
	myS            int 
	lastAppendTime time.Time
	applyCh        chan ApplyMsg

	snapshot  []byte
	baseIndex int
	baseTerm  int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) beFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.myS = FollowerState
	rf.votedFor = -1
	rf.persist()
	DPrintf("%v be follower.", rf.me)
}

func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.myS = CandidateState
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("%v be candidate.", rf.me)
}

func (rf *Raft) beLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.myS = LeaderState
	pLen := len(rf.peers)
	lLen := len(rf.logs) + rf.baseIndex
	for i := 0; i < pLen; i ++ {
		rf.nextIndex[i] = lLen 
		rf.matchIndex[i] = rf.baseIndex
	}
	rf.persist()
	DPrintf("%v be leader.", rf.me)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.myS == LeaderState {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.baseIndex)
	e.Encode(rf.baseTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	// DPrintf("\n%v in term : %v votefor : %v \n logs : %v\n",rf.me, rf.currentTerm, rf.votedFor, rf.logs)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, votedfor, bIndex, bTerm int
	var logs []LogNodes
	if d.Decode(&term) != nil ||
		d.Decode(&votedfor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&bIndex) != nil || 
		d.Decode(&bTerm) != nil {
		// log.
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedfor
		rf.logs = logs
		rf.lastApplied = bIndex
		rf.commandIndex = bIndex
		rf.baseIndex = bIndex
		rf.baseTerm = bTerm
		rf.mu.Unlock()
	}
}

func (rf *Raft) saveStateAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.baseIndex)
	e.Encode(rf.baseTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	if rf.commandIndex >= lastIncludedIndex {
		rf.mu.Unlock()
		return false
	}

	if len(rf.logs) + rf.baseIndex <= lastIncludedIndex || 
		rf.baseIndex > lastIncludedIndex || 
		rf.logs[lastIncludedIndex-rf.baseIndex].Term != lastIncludedTerm {
		rf.logs = append(make([]LogNodes,0), LogNodes{Term : lastIncludedTerm})
		// DPrintf("%v logs : %v",rf.me,rf.logs)
	} else {
		rf.logs = append(make([]LogNodes,0), rf.logs[lastIncludedIndex - rf.baseIndex: ]...)
		// DPrintf("%v logs : %v",rf.me,rf.logs)
	}

	// if rf.lastApplied < index {
	// 	rf.lastApplied = index
	// }
	// if rf.commandIndex < index {

	// }
	rf.commandIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	// DPrintf("%v get a snapshot at %v",rf.me,lastIncludedIndex)
	rf.snapshot = snapshot
	rf.baseIndex = lastIncludedIndex
	rf.baseTerm = lastIncludedTerm
	rf.saveStateAndSnapshot()
	rf.mu.Unlock()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	rf.baseTerm = rf.logs[index - rf.baseIndex].Term
	rf.logs = append(make([]LogNodes,0), rf.logs[index - rf.baseIndex: ]...)
	rf.snapshot = snapshot
	rf.baseIndex = index
	rf.saveStateAndSnapshot()
	rf.mu.Unlock()
	rf.GiveSnapshot(-1)
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetDataSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

func (rf *Raft) GiveSnapshot(x int) {
	pLen := len(rf.peers)
	for i := 0; i < pLen; i ++ {
		if i == rf.me {
			continue 
		}

		if x != -1 {
			i = x;
		}

		go func (e0,e1,e2,e3,e4 int, e5 []byte) {
			args := SnapshotArgs{e1,e2,e3,e4,e5}
			ok := false
			d, _ := time.ParseDuration(strconv.Itoa(appendTime) + "ms")
			endTime := time.Now().Add(d)

			for ok == false && time.Now().Before(endTime) {
				reply := SnapshotReply{}
				ok = rf.sendInstallSnapshot(e0, &args, &reply)

				if ok == true {
					if reply.Term > args.Term {
						rf.beFollower()
					} else {
						rf.mu.Lock()
						rf.nextIndex[e0] = e3 + 1
						rf.matchIndex[e0] = e3
						rf.mu.Unlock()
					}
				}
			}
		} (i, rf.currentTerm, rf.me, rf.baseIndex, rf.baseTerm, rf.snapshot)
		
		if x != -1 {
			break 
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// 
// AppendEntries Args
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int 
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogNodes
	LeaderCommit int
}

//
// Snapshot Args
//
type SnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// AppendEntries Reply
//
type AppendEntriesReply struct {
	Term    int
	Success bool

	// LastTerm  int
	LastIndex int
}

//
// Snapshot Reply
//
type SnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return 
	} 

	if args.Term == rf.currentTerm {
		if rf.myS != FollowerState {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.mu.Unlock()
			return 
		}

		lLen := len(rf.logs) + rf.baseIndex
		if (lLen-1 > args.LastLogIndex && (rf.logs[lLen-1-rf.baseIndex].Term == args.LastLogTerm)) || 
			(rf.logs[lLen-1-rf.baseIndex].Term > args.LastLogTerm) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.mu.Unlock()
			return 
		}

		if rf.votedFor != -1 {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.mu.Unlock()
			return 
		}

		// DPrintf("%v vote to  %v\n", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		rf.myS = FollowerState
		rf.lastAppendTime = time.Now()
		rf.persist()
		rf.mu.Unlock()
		reply.Term = args.Term
		reply.VoteGranted = true
		return 
	}

	if args.Term > rf.currentTerm {
		rf.myS = FollowerState
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
		lLen := len(rf.logs) + rf.baseIndex

		// DPrintf("lLen : %v, args.len : %v, rf.term : %v, args.term : %v", lLen-1, args.LastLogIndex, rf.logs[lLen-1-rf.baseIndex].Term, args.LastLogTerm)
		
		if (lLen-1 > args.LastLogIndex && (rf.logs[lLen-1-rf.baseIndex].Term == args.LastLogTerm)) || 
			(rf.logs[lLen-1-rf.baseIndex].Term > args.LastLogTerm) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.mu.Unlock()
			return 
		}

		// DPrintf("%v vote to  %v !\n", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		// rf.myS = FollowerState
		// rf.votedFor = -1
		// rf.currentTerm = args.Term
		rf.persist()
		rf.lastAppendTime = time.Now()
		rf.mu.Unlock()
		reply.Term = args.Term
		reply.VoteGranted = true
		return 
	}
}

//
// AppendEnteries RPC handler.
//
func (rf *Raft) AppendEnteries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// DPrintf("%v %v %v %v %v %v\n",args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	// DPrintf("%v\n", rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm 
		reply.Success = false
		rf.mu.Unlock()
		return 
	}

	rf.lastAppendTime = time.Now()
	if rf.currentTerm > args.Term {
		rf.currentTerm = args.Term
		rf.myS = FollowerState
		rf.votedFor = -1
		rf.persist()
	}

	if args.PrevLogIndex > len(rf.logs) - 1 + rf.baseIndex {
		reply.Term = rf.currentTerm 
		reply.Success = false
		reply.LastIndex = len(rf.logs) + rf.baseIndex
		rf.mu.Unlock()
		return 
	}

	if args.PrevLogIndex < rf.baseIndex ||
		(args.PrevLogIndex == rf.baseIndex && rf.baseTerm != args.PrevLogTerm) {
		reply.LastIndex = -1
		reply.Success = false
		rf.mu.Unlock()
		return 
	}

	if rf.logs[args.PrevLogIndex-rf.baseIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm 
		reply.Success = false
		reply.LastIndex = args.PrevLogIndex
		for reply.LastIndex > rf.baseIndex && rf.logs[reply.LastIndex-rf.baseIndex].Term == rf.logs[reply.LastIndex-1-rf.baseIndex].Term  {
			reply.LastIndex -= 1
		}
		rf.mu.Unlock()
		return 
	}

	if len(args.Entries) != 0 {
		rf.logs = append(make([]LogNodes,0), append(rf.logs[0 : args.PrevLogIndex + 1 - rf.baseIndex], args.Entries...)...)
		// DPrintf("%v new logs: %v",rf.me, rf.logs)
		rf.persist()
	}
	if args.LeaderCommit > rf.commandIndex {
		if args.LeaderCommit > len(rf.logs) - 1 + rf.baseIndex {
			rf.commandIndex = len(rf.logs) - 1 + rf.baseIndex
		} else {
			rf.commandIndex = args.LeaderCommit
		}
	} else {
		rf.commandIndex = args.LeaderCommit
	}
	
	reply.Term = rf.currentTerm 
	reply.Success = true
	// DPrintf("%v append by %v\n",rf.me,args.LeaderId)

	rf.mu.Unlock()
	return 
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return 
	}

	rf.lastAppendTime = time.Now()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.myS = FollowerState
		rf.votedFor = -1
		rf.persist()
	}
	rf.mu.Unlock()
	reply.Term = args.Term

	// DPrintf("%v send a snapshot at : %v",rf.me, args.LastIncludedIndex)
	rf.applyCh <- ApplyMsg {
		false,nil,0,
		true,args.Data,args.LastIncludedTerm,args.LastIncludedIndex,
	}
	return 
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEnteries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) appendEnteries() {
	vG := aCounter{}
	vG.cnt = 1
	rf.mu.Lock()
	pLen := len(rf.peers)
	lLen := len(rf.logs) + rf.baseIndex
	rf.mu.Unlock()
	
	for i := 0; i < pLen; i ++ {
		if i == rf.me {
			rf.mu.Lock()
			rf.nextIndex[i] = lLen
			rf.matchIndex[i] = lLen - 1
			rf.mu.Unlock()
			continue 
		}

		rf.mu.Lock()
		if rf.myS != LeaderState {
			rf.mu.Unlock()
			break 
		}

		if rf.nextIndex[i] > lLen {
			rf.nextIndex[i] = lLen
			if rf.matchIndex[i] >= lLen {
				rf.matchIndex[i] = lLen - 1
			}
			rf.mu.Unlock()
			continue 
		}
		rf.mu.Unlock()

		rf.mu.Lock()
		if rf.nextIndex[i] <= rf.baseIndex {
			rf.GiveSnapshot(i)
			rf.mu.Unlock()
			continue 
		}

		// DPrintf("+ %v to %v :  %v, %v", rf.me, i, rf.nextIndex[i], rf.baseIndex)
		logE := rf.logs[rf.nextIndex[i]-rf.baseIndex : lLen-rf.baseIndex]
		rf.mu.Unlock()
		go func (e0 int, e1 int, e2 int, e3 int, e4 int, e5 []LogNodes, e6 int) {
			args := AppendEntriesArgs{e1,e2,e3,e4,e5,e6}
			ok := false
			d, _ := time.ParseDuration(strconv.Itoa(appendTime) + "ms")
			endTime := time.Now().Add(d)

			for ok == false && time.Now().Before(endTime) {
				reply := AppendEntriesReply{}
				ok = rf.sendAppendEntries(e0, &args, &reply)
				// k -- 

				if ok == true {
					if reply.Success == true {
						// DPrintf("%v %v",rf.nextIndex[e0],rf.baseIndex)
						vG.mu.Lock()
						vG.cnt += 1
						vG.mu.Unlock()

						rf.mu.Lock()
						rf.nextIndex[e0] = lLen
						rf.matchIndex[e0] = lLen - 1
						// DPrintf("* %v to %v :  %v, %v", rf.me, e0, rf.nextIndex[e0], rf.baseIndex)
						
						rf.mu.Unlock()
					} else {
						if args.Term < reply.Term {
							DPrintf("go back")
							rf.beFollower()
							// rf.mu.Lock()
							// rf.currentTerm = reply.Term
							// rf.mu.Unlock()
							break 
						}
						rf.mu.Lock()
						if reply.LastIndex == -1 {
							reply.LastIndex = rf.baseIndex
						}
						// DPrintf("%v is wrong rpc ; %v - > %v\n",e0, rf.nextIndex[e0], reply.LastIndex)
						rf.nextIndex[e0] = reply.LastIndex
						if rf.myS != LeaderState || rf.nextIndex[e0] <= rf.baseIndex {
							rf.mu.Unlock()
							break 
						}
						// DPrintf("- %v to %v :  %v, %v", rf.me, e0, rf.nextIndex[e0], rf.baseIndex)
						logE = rf.logs[rf.nextIndex[e0]-rf.baseIndex : lLen-rf.baseIndex]
						args = AppendEntriesArgs{e1, e2, rf.nextIndex[e0]-1, rf.logs[rf.nextIndex[e0]-1-rf.baseIndex].Term, logE, e6}
						// e1,e2,e3,e4,e5,e6 = rf.currentTerm, rf.me, rf.nextIndex[e0]-1, rf.logs[rf.nextIndex[e0]-1].Term, logE, rf.commandIndex
						rf.mu.Unlock()
						ok = false
					}
				}
			}
		} (i, rf.currentTerm, rf.me, rf.nextIndex[i]-1, rf.logs[rf.nextIndex[i]-1-rf.baseIndex].Term, logE, rf.commandIndex)
	}
}

func (rf *Raft) UpdataIndex() {
	// time.Sleep(time.Duration(appendTime/10*8) * time.Millisecond)
	// d, _ := time.ParseDuration(strconv.Itoa(appendTime/10) + "ms")
	// endTime := time.Now().Add(d)
	
	pLen := len(rf.peers)
	arr := make([]int, pLen)
	for i := 0; i < pLen; i ++ {
		arr[i] = rf.matchIndex[i]
		// DPrintf("%d ",arr[i])
	}
	// DPrintf("\n")

	sort.Ints(arr)
	N := arr[pLen / 2]
	// DPrintf("N : %v; %d ",N, pLen)
	rf.mu.Lock()
	if N < rf.baseIndex {
		// rf.GiveSnapshot(-1)
		rf.mu.Unlock()
		return 
	}
	if rf.myS == LeaderState && rf.logs[N-rf.baseIndex].Term == rf.currentTerm {
		rf.commandIndex = N
	}
	rf.mu.Unlock()
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	// DPrintf("ser: %v , %v %v %v", rf.me,term, isLeader, rf.myS)
	if isLeader == true {
		// DPrintf("%v in log :  %v",rf.me, LogNodes{rf.currentTerm, command})
		rf.mu.Lock()
		lLen := len(rf.logs) + rf.baseIndex
		rf.logs = append(rf.logs,LogNodes{rf.currentTerm, command})
		index = lLen
		rf.persist()
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		lastRPCTime := rf.lastAppendTime
		rf.mu.Unlock()
		electionSleepTime := rand.Int() % electionLen + electionWaitTime
		for i := 0; i < electionSleepTime; i += rpcCheckEach {
			sleepTurn := rpcCheckEach
			if i + rpcCheckEach >= electionSleepTime {
				sleepTurn = i + rpcCheckEach - electionSleepTime
			}

			time.Sleep(time.Duration(sleepTurn) * time.Millisecond)
			rf.mu.Lock()
			if rf.lastAppendTime != lastRPCTime {
				rf.mu.Unlock()
				break 
			}
			rf.mu.Unlock()
		}
		
		if rf.lastAppendTime != lastRPCTime {
			continue 
		}

		DPrintf("%v try leader: term : %v; logs: %v -> %v",rf.me, rf.currentTerm, len(rf.logs), rf.logs[len(rf.logs)-1])
		rf.beCandidate()

		vG := aCounter{}
		vG.cnt = 1
		rf.mu.Lock()
		pLen := len(rf.peers)
		lLen := len(rf.logs) + rf.baseIndex
		rf.mu.Unlock()

		for i := 0; i < pLen; i ++ {
			if i == rf.me {
				// vG.mu.Lock()
				// vG.cnt += 1
				// vG.mu.Unlock()
				continue 
			}

			go func(e0 int, e1 int, e2 int, e3 int, e4 int) {
				args := RequestVoteArgs{e1,e2,e3,e4}
				reply := RequestVoteReply{}
				ok := false
				for ok == false {
					ok = rf.sendRequestVote(e0, &args, &reply)
					break // wait or not ?
				}

				if ok == true {
					if reply.VoteGranted == true {
						vG.mu.Lock()
						vG.cnt += 1
						vG.mu.Unlock()
					} else {
						if reply.Term > args.Term {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.myS = FollowerState
							rf.votedFor = -1
							rf.persist()
							rf.mu.Unlock()
						}
					}
				}
			} (i, rf.currentTerm, rf.me, lLen - 1, rf.logs[lLen-1-rf.baseIndex].Term)
		}

		for i := 0; i < electionTime; i += electionEach {
			time.Sleep(time.Duration(electionEach) * time.Millisecond)
			vG.mu.Lock()
			if vG.cnt > pLen / 2 {
				rf.beLeader()
				DPrintf("%d is leader | %v -> %v\n", rf.me, vG.cnt, pLen/2)
				vG.mu.Unlock()
				break 
			}
			vG.mu.Unlock()

			rf.mu.Lock()
			if rf.myS == FollowerState {
				rf.mu.Unlock()
				break 
			}
			rf.mu.Unlock()
		}

		// rf.mu.Lock()
		// if rf.killed() == false && rf.myS == LeaderState {
		// 	rf.mu.Unlock()
		// } else {
		// 	rf.mu.Unlock()
		// 	continue 
		// }

		for rf.killed() == false {
			rf.mu.Lock()
			if rf.myS != LeaderState {
				rf.mu.Unlock()
				break 
			}
			rf.mu.Unlock()

			rf.appendEnteries()

			time.Sleep(time.Duration(appendTime) * time.Millisecond)
			go rf.UpdataIndex()
		}

		// DPrintf("%v cycle\n",rf.me)
	}
}

func (rf *Raft) BackInterFace() {
	for {
		ok := false
		prepareLog := make([]ApplyMsg,0)
		rf.mu.Lock()
		if rf.lastApplied < rf.commandIndex {
			ok = true
			// DPrintf("%v leader : %v; %v ~ %v; len : %v",rf.me, rf.myS, rf.lastApplied, rf.commandIndex, len(rf.logs)-1)
			for i := rf.lastApplied + 1; i <= rf.commandIndex; i ++ {
				prepareLog = append(prepareLog, ApplyMsg{true, rf.logs[i - rf.baseIndex].Log, i, false, nil, 0, 0})
			}
		} 
		rf.mu.Unlock()

		if ok == false {
			time.Sleep(time.Duration(appendTime) * time.Millisecond)
		} else {
			for _,AM := range prepareLog {
				// DPrintf("%v : %v, %v\n",rf.me, AM.CommandIndex, AM.Command)
				rf.applyCh <- AM
				rf.lastApplied += 1
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogNodes, 0)
	rf.logs = append(rf.logs, LogNodes{0,nil})

	rf.commandIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.myS = 0
	rf.applyCh = applyCh

	rf.snapshot = make([]byte, 0)
	rf.baseIndex = 0
	rf.baseTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// if len(rf.logs) == 0 {
	// 	rf.persist()
	// }

	// start ticker goroutine to start elections
	go rf.ticker()
	
	go rf.BackInterFace()


	return rf
}
