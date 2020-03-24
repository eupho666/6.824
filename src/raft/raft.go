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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry ...
// students defined, used for Raft class
type LogEntry struct {
	Command interface{}
	term    int
	index   int
}

// Role type ...
type Role int32

// Role enum ...
const (
	Leader    Role = 0
	Follower  Role = 1
	Candidate Role = 2
	// HEARTBEATTIMEOUT is heartbeat timeout
	HEARTBEATTIMEOUT int = 100
)

// Raft ...
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	// Persistent State

	currentTerm int
	votedFor    int
	log         []LogEntry
	voteCount   int
	// Volatile State for all servers
	commitIndex int
	lastApplied int
	// Volatile State for leaders
	nextIndex  []int
	matchIndex []int

	State  Role
	Leader int

	appendEntriesCh chan bool
	voteGrantedCh   chan bool
	leaderCh        chan bool
	applyMsgCh      chan ApplyMsg

	timer           *time.Timer
	electionTimeout time.Duration
}

// GetState ...
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.State == Leader
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// RequestVoteArgs ...
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int
	candidatedID int
	lastLogIndex int
	lastLogTerm  int
}

// RequestVoteReply ...
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

// RequestVote ...
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.voteGranted = false
	if args.term < rf.currentTerm || args.lastLogIndex < rf.commitIndex {
		reply.term = rf.currentTerm
		return
	}
	if args.term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm = args.term
		rf.votedFor = args.candidatedID
	}
	reply.term = rf.currentTerm
	reply.voteGranted = true
	go func() {
		rf.voteGrantedCh <- true
	}()
}

// 2A

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	term         int
	leaderID     int
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	leaderCommit int
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	term    int
	success bool
}

// AppendEntries ...
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Lock()

	reply.term = rf.currentTerm
	reply.success = false
	if args.term < rf.currentTerm {
		return
	}
	if args.term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm = args.term
	}
	// if len(rf.log) < args.prevLogIndex {
	// 	return
	// }
	// if rf.log[args.prevLogIndex-1].term != args.prevLogTerm || rf.log[args.prevLogIndex-1].index != args.prevLogIndex {
	// 	return
	// }
	// reply.success = true
	// if len(args.entries) == 0 {
	// 	return
	// }
	// for _, entry := range args.entries {
	// 	for ; len(rf.log) < entry.index; rf.log = append(rf.log, LogEntry{}) {
	// 	}
	// 	if rf.log[entry.index-1].index != entry.index || rf.log[entry.index-1].term != entry.term {
	// 		rf.log[entry.index-1] = entry
	// 	}

	// }
	// if args.leaderCommit > rf.commitIndex {
	// 	if args.leaderCommit <= rf.log[len(rf.log)-1].index {
	// 		rf.commitIndex = args.leaderCommit
	// 	} else {
	// 		rf.commitIndex = rf.log[len(rf.log)-1].index
	// 	}
	// }

	go func() {
		rf.appendEntriesCh <- true
	}()

}

// ChangeState ...
func (rf *Raft) ChangeState(state Role) {
	if state == rf.State {
		return
	}
	switch state {
	case Follower:
		rf.State = Follower
		rf.votedFor = -1
		DPrintf("raft%v become follower in term:%v\n", rf.me, rf.currentTerm)
	case Candidate:
		rf.State = Candidate
		DPrintf("raft%v become candidate in term:%v\n", rf.me, rf.currentTerm)
	case Leader:
		rf.State = Leader
		DPrintf("raft%v become leader in term:%v\n", rf.me, rf.currentTerm)
	}
}

// StartElection ...
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.timer.Reset(rf.electionTimeout)
	rf.mu.Unlock()

	for peer := range rf.peers {
		go func(peer int) {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}

			rf.mu.Lock()
			args.term = rf.currentTerm
			rf.mu.Unlock()
			args.candidatedID = rf.me
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				if reply.voteGranted {
					rf.voteCount++
				} else if reply.term > rf.currentTerm {
					rf.currentTerm = reply.term
					rf.ChangeState(Follower)
				}

				rf.mu.Unlock()
			}

		}(peer)

	}
}

// HeartBeats ...
func (rf *Raft) HeartBeats() {
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}

				rf.mu.Lock()
				args.term = rf.currentTerm
				args.leaderID = rf.me
				rf.mu.Unlock()

				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					if reply.term > rf.currentTerm {
						rf.currentTerm = reply.term
						rf.ChangeState(Follower)
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
	}
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	// 2A
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.State = Follower
	rf.Leader = 0

	rf.appendEntriesCh = make(chan bool, 1)
	rf.voteGrantedCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)
	rf.applyMsgCh = applyCh

	electionTimeout := HEARTBEATTIMEOUT*3 + rand.Intn(HEARTBEATTIMEOUT)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	DPrintf("raft%v's election timeout is:%v\n", rf.me, rf.electionTimeout)
	rf.timer = time.NewTimer(rf.electionTimeout)
	go func() {
		for {
			rf.mu.Lock()
			state := rf.State
			rf.mu.Unlock()
			electionTimeout := HEARTBEATTIMEOUT*3 + rand.Intn(HEARTBEATTIMEOUT)
			rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond

			switch state {
			// 对于AppendEntries RPC 和 VoteGranted RPC进行回应后
			// 说明有和其他节点进行交流，要重置计时器
			// 如果计时器终止，说明一段时间没和其他节点交流，需要重新选举
			case Follower:
				select {
				case <-rf.appendEntriesCh:
					rf.timer.Reset(rf.electionTimeout)
				case <-rf.voteGrantedCh:
					rf.ChangeState(Follower)
					rf.timer.Reset(rf.electionTimeout)
				case <-rf.timer.C:
					rf.mu.Lock()
					rf.ChangeState(Candidate)
					rf.mu.Unlock()
					rf.StartElection()
				}
			case Candidate:
				select {
				case <-rf.appendEntriesCh:
					rf.mu.Lock()
					rf.ChangeState(Follower)
					rf.mu.Unlock()
					rf.timer.Reset(rf.electionTimeout)

				case <-rf.timer.C:
					rf.StartElection()
				default:
					rf.mu.Lock()
					if rf.voteCount > len(rf.peers)/2 {
						rf.ChangeState(Leader)
					}
					rf.mu.Unlock()
				}
			case Leader:
				rf.HeartBeats()
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
