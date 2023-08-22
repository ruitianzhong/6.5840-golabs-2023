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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type Role int

const (
	FOLLOWER Role = iota
	LEADER
	CANDIDATE
)

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	role      Role
	received  bool // whether received heartbeat during last timeout period
	voted     []bool
	applyCh   chan ApplyMsg
	voteCount int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	lastApplied int
	commitIndex int

	// volatile state on all leader
	matchIndex []int
	nextIndex  []int

	// snapshot state,persistent on all servers
	lastIncludedIndex int
	lastIncludedTerm  int
}

type AppendEntriesArgs struct {
	Term, LeaderID, PrevLogIndex, PrevLogTerm, LeaderCommit int // capital letter
	Entry                                                   []LogEntry
	ContainEntry                                            bool
}

type AppendEntriesReply struct {
	Term                int
	Success             bool
	XTerm, XIndex, XLen int
}

type LogEntry struct {
	Term, Index int
	Command     interface{}
}

type InstallSnapshotArgs struct {
	Term, LeaderID, LastIncludedIndex, LastIncludedTerm int
	Data                                                []byte
}

type InstallSnapshotReply struct {
	Term int // for leader to update itself
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	raftstate := rf.raftState2Bytes()

	rf.persister.Save(raftstate, nil)

}

func (rf *Raft) raftState2Bytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	return raftstate
}

func (rf *Raft) persistSnapshot(snapshot []byte) {

	raftstate := rf.raftState2Bytes()

	rf.persister.Save(raftstate, snapshot)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.lastIncludedIndex = -1
		rf.lastIncludedTerm = -1
		rf.log[0].Index = 0
		rf.log[0].Term = 0
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("Can not decode data")
	} else {
		rf.currentTerm = currentTerm
		rf.log = log
		rf.votedFor = votedFor // serious bug?
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()

	defer rf.mu.Unlock()

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term, CandidateID, LastLogIndex, LastLogTerm int // capital letter
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool // capital letter
	Term        int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	} else if rf.currentTerm == args.Term {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.checkIfUpToDate(args) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
		}
	} else {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		if rf.checkIfUpToDate(args) {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		} else {
			rf.votedFor = -1
		}
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
	}
	rf.appendLog(args, reply)
	rf.received = true
	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) InstallSnapshotArgs(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

}

func (rf *Raft) getPrevLogIndexAndTerm() (prevIndex, prevTerm int) {
	if len(rf.log) > 0 {
		prevIndex, prevTerm = rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term
	} else {
		prevIndex, prevTerm = rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	return prevIndex, prevTerm
}

func (rf *Raft) translateIndex(index int) int {

	return index - (rf.lastIncludedIndex + 1)

}

func (rf *Raft) getPrevLogIndex() int {
	index, _ := rf.getPrevLogIndexAndTerm()
	return index
}

func (rf *Raft) getLastLogTerm() int {
	_, term := rf.getPrevLogIndexAndTerm()
	return term
}

func (rf *Raft) unsafeGetLogEntry(index int) LogEntry {
	return rf.log[rf.translateIndex(index)]
}

func (rf *Raft) appendLog(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	prevTerm, prevIndex := args.PrevLogTerm, args.PrevLogIndex
	lastIndex, lastTerm := rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term
	if prevTerm == lastTerm && prevIndex == lastIndex {
		reply.Success = true
		if args.ContainEntry {
			rf.log = append(rf.log, args.Entry...)
		}
		rf.updateFollowerCommitIndex(args)
	} else {
		rf.handleConflict(args, reply)
	}

}

func (rf *Raft) updateFollowerCommitIndex(args *AppendEntriesArgs) {
	if args.LeaderCommit <= rf.commitIndex {
		return
	}
	m := min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	for rf.commitIndex < m {
		rf.commitIndex += 1
		RaftDebug(rCommit, "Server %v Follower commits commmand %v index:%v", rf.me, rf.log[rf.commitIndex].Command, rf.commitIndex)
		rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: rf.commitIndex, Command: rf.log[rf.commitIndex].Command}
	}
}

func (rf *Raft) handleConflict(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	prevTerm, prevIndex := args.PrevLogTerm, args.PrevLogIndex
	lastIndex := rf.log[len(rf.log)-1].Index
	if lastIndex < prevIndex {
		reply.Success = false
		reply.Term = rf.log[lastIndex].Term
		reply.XIndex = lastIndex
		reply.XLen = len(rf.log)
	} else if lastIndex > prevIndex {

		if rf.log[prevIndex].Term == prevTerm {
			i := 0
			ok := true
			for i < len(args.Entry) && i+prevIndex+1 <= lastIndex {
				if args.Entry[i].Term != rf.log[i+prevIndex+1].Term {
					ok = false
					break
				}
				i += 1
			}
			if !ok {
				rf.log = rf.log[:i+prevIndex+1]
				rf.hintBackoff(reply)

			} else if i+prevIndex+1 > lastIndex && i < len(args.Entry) {
				rf.log = append(rf.log, args.Entry[i:]...)
				rf.updateFollowerCommitIndex(args)
				reply.Success = true
			}

		} else {
			rf.log = rf.log[:prevIndex] // easy to be wrong
			rf.hintBackoff(reply)
		}
	} else {
		rf.hintBackoff(reply)
	}
}

func (rf *Raft) hintBackoff(reply *AppendEntriesReply) {
	lastIndex := rf.log[len(rf.log)-1].Index
	reply.XIndex = rf.log[lastIndex].Index
	reply.XTerm = rf.log[lastIndex].Term
	rf.log = rf.log[:lastIndex]
	reply.XLen = len(rf.log)
	reply.Success = false
}
func min(x, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}
func (rf *Raft) checkIfUpToDate(args *RequestVoteArgs) bool {
	lastLogTerm, lastLogIndex := rf.log[len(rf.log)-1].Term, rf.log[len(rf.log)-1].Index
	return args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
}

func (rf *Raft) ResetVoted() {
	for i := range rf.voted {
		rf.voted[i] = false
	}
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == LEADER {
		isLeader = true
		term = rf.currentTerm
		index = rf.log[len(rf.log)-1].Index + 1
		log := LogEntry{Index: index, Term: term, Command: command}
		rf.log = append(rf.log, log)
		rf.persist()
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 200 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.role == LEADER {
			rf.mu.Unlock()
			continue
		}
		if !rf.received {
			// start the election
			// TODO: persist the data
			rf.PrepareElection()
			rf.persist()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.asyncSendRequestVote(i, rf.currentTerm)
				}
			}
		} else {
			rf.received = false
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) PrepareElection() {
	rf.currentTerm += 1
	rf.role = CANDIDATE
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.ResetVoted()
}
func (rf *Raft) asyncSendRequestVote(server int, expectedTerm int) {

	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != CANDIDATE || expectedTerm != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		go func() {
			args := RequestVoteArgs{}
			args.CandidateID = rf.me
			args.Term = expectedTerm
			args.LastLogIndex = rf.log[len(rf.log)-1].Index // initially forget to init index & term
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
			rf.mu.Lock()
			if rf.role != CANDIDATE || expectedTerm != rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock() // send RequestVote without lock
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}

			rf.mu.Lock()

			if rf.role == CANDIDATE && rf.currentTerm == expectedTerm {
				if reply.VoteGranted && !rf.voted[server] {
					rf.voted[server] = true
					rf.voteCount += 1
					if rf.voteCount >= len(rf.peers)/2+1 {
						rf.transfer2Leader()
					}
				}
			}

			rf.mu.Unlock()
		}()
		rf.mu.Unlock()
		time.Sleep(110 * time.Millisecond)
	}

}

func (rf *Raft) transfer2Leader() {
	RaftDebug(rLeader, "Server %v term:%v lastIndex:%v lastTerm:%v",
		rf.me, rf.currentTerm, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term)
	rf.role = LEADER
	rf.initNextIndexAndMatchIndex()
	rf.received = false
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.asyncSendAppendEntries(i, rf.currentTerm)
		}
	}
	go rf.updateMatchIndex(rf.currentTerm)
}

func (rf *Raft) updateMatchIndex(expectedTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != LEADER || rf.currentTerm != expectedTerm {
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
		quorum := len(rf.peers)/2 + 1
		n := len(rf.peers)
		sorted := make([]int, n)
		copy(sorted, rf.matchIndex)
		sort.Ints(sorted)
		end := sorted[quorum-1] //prone to wrong here
		if rf.log[end].Term == rf.currentTerm {
			for rf.commitIndex < end {
				rf.commitIndex += 1
				RaftDebug(rCommit, "Server %v LEADER  commits commmand %v index:%v", rf.me, rf.log[rf.commitIndex].Command, rf.commitIndex)
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex].Command, CommandIndex: rf.commitIndex}
			}
		}

		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) asyncSendAppendEntries(server int, expectedTerm int) {

	for !rf.killed() {
		rf.mu.Lock()

		if rf.role != LEADER || rf.currentTerm != expectedTerm {
			rf.mu.Unlock()
			return
		}

		go func() {
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}
			rf.mu.Lock()

			if rf.role != LEADER || rf.currentTerm != expectedTerm {
				rf.mu.Unlock()
				return
			}
			rf.initAppendEntries(server, &args)
			rf.mu.Unlock() // to send in parallel
			if !rf.sendAppendEntries(server, &args, &reply) {
				return
			}
			rf.mu.Lock()
			if rf.role != LEADER || rf.currentTerm != expectedTerm {
				rf.mu.Unlock()
				return
			}
			RaftDebug(rAppendSend, "Server %v LEADER term:%v prevLogIndex:%v prevLogTerm:%v to %v",
				rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, server)
			if !rf.handleAppendEntriesReply(server, args, reply) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}()
		rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}

}
func max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, args AppendEntriesArgs, reply AppendEntriesReply) bool {
	if reply.Success {
		if args.ContainEntry {
			rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entry), rf.matchIndex[server])
			rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex+len(args.Entry)+1)
			RaftDebug(rAppendAccept, "Server %v Leader received from %v next:%v match:%v",
				rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
		}
		return true
	}
	if reply.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = reply.Term
		rf.persist()
		return false
	}
	// TODO: Update some states of leader
	rf.backoff(server, reply)
	RaftDebug(rAppendReject, "Server %v Leader received from %v next:%v match:%v",
		rf.me, server, rf.nextIndex[server], rf.matchIndex[server])

	return true
}

func (rf *Raft) backoff(server int, reply AppendEntriesReply) {

	conflictTerm := reply.XTerm
	next := rf.nextIndex[server]

	for rf.log[next-1].Term > conflictTerm {
		next -= 1
	}
	if rf.log[next-1].Term != conflictTerm || reply.XIndex+1 < next {
		rf.nextIndex[server] = reply.XIndex + 1
	} else {
		rf.nextIndex[server] = next
	}
}

func (rf *Raft) initAppendEntries(server int, args *AppendEntriesArgs) {
	args.LeaderCommit = rf.commitIndex
	args.LeaderID = rf.me
	args.Term = rf.currentTerm
	args.PrevLogIndex = rf.log[rf.nextIndex[server]-1].Index
	args.PrevLogTerm = rf.log[rf.nextIndex[server]-1].Term
	args.ContainEntry = false
	next := rf.nextIndex[server]
	if next <= rf.log[len(rf.log)-1].Index {
		args.Entry = make([]LogEntry, rf.log[len(rf.log)-1].Index-next+1)
		copy(args.Entry, rf.log[next:])
		args.ContainEntry = true
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
	rf.role = FOLLOWER
	InitLog()
	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.voted = make([]bool, len(peers))
	rf.log = make([]LogEntry, 1)
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.initNextIndexAndMatchIndex()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) initNextIndexAndMatchIndex() {
	x := rf.log[len(rf.log)-1].Index + 1
	for i := range rf.peers {
		rf.nextIndex[i] = x
		rf.matchIndex[i] = 0
	}
}
