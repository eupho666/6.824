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
	Term    int
}
type ChangeToFollower struct {
	term             int
	votedFor         int
	shouldResetTimer bool
}

// Role type ...
type Role int32

// Role enum ...
const (
	Leader    Role = 0
	Follower  Role = 1
	Candidate Role = 2

	HEARTBEATTIMEOUT = 100
	ELECTIONTIMEOUT  = 300
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

	State Role

	receiveQuit           chan bool
	quitCheckRoutine      chan bool
	changeToFollower      chan ChangeToFollower
	changeToFollowerDone  chan bool
	followerAppendEntries chan bool
	applyCh               chan ApplyMsg

	timer *time.Timer
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
	isleader = (rf.State == Leader)
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
	Term         int
	CandidatedID int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply ...
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote ...
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	reply.Term = args.Term
	reply.VoteGranted = false

	if rf.votedFor == -1 || rf.votedFor == args.CandidatedID || args.Term > rf.currentTerm {
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term

		if args.LastLogIndex < lastLogIndex || (args.LastLogIndex == lastLogIndex) && args.LastLogTerm < lastLogTerm {
			reply.VoteGranted = false
			return
		} else {
			DPrintf("[RequestVote RPC] raft %d vote to %d in term %d\n", rf.me, args.CandidatedID, rf.currentTerm)
			reply.VoteGranted = true
		}
		//else if args.Term > rf.currentTerm {
		//	reply.VoteGranted = true
		//
		//} else {
		//	reply.VoteGranted = false
		//}

		if args.Term > rf.currentTerm && reply.VoteGranted == true {
			ch := ChangeToFollower{args.Term, args.CandidatedID, reply.VoteGranted}
			rf.PushChangeToFollower(ch)
		}
	}

}

// 2A

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	Term         int //Leader term
	LeaderID     int
	PrevLogIndex int        // 前一个entry所在的索引位置
	PrevLogTerm  int        // 前一个entry的term
	Entries      []LogEntry //要添加的entry
	LeaderCommit int        // leader中已提交entry的最高索引，对应Raft.commitIndex
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries ...
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex+1 > len(rf.log) || args.PrevLogIndex+1 <= len(rf.log) && args.PrevLogIndex > 0 && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if len(rf.log) != args.PrevLogIndex+1 {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	// 4. Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}

	reply.Term = args.Term
	reply.Success = true

	// todo: [AppendEntryRPC] votefor number may should be -1
	DPrintf("[AppendEntryRPC] raft %d reveive AppendEntryRPC and return success in term %d", rf.me, rf.currentTerm)
	ch := ChangeToFollower{args.Term, -1, true}
	rf.PushChangeToFollower(ch)

}

func (rf *Raft) PushChangeToFollower(ch ChangeToFollower) {
	rf.changeToFollower <- ch
	<-rf.changeToFollowerDone
}
func CheckIfWinHalfVote(voted []bool, server_count int) bool {
	voted_count := 0
	for i := 0; i < server_count; i++ {
		if voted[i] {
			voted_count++
		}
	}
	//win the vote if iut receives votes from a majority of the servers in the ful cluster for the same term.
	return voted_count >= (server_count/2 + 1)
}

// StartElection ...
func (rf *Raft) StartElection(win chan bool) {
	DPrintf("[StartElection] raft %d start send RequestVoteRPC", rf.me)
	n := len(rf.peers)
	voted := make([]bool, n)

	rf.mu.Lock()
	if rf.State != Candidate {
		DPrintf("[StartElection] raft %d is not Candidate", rf.me)
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	voted[rf.me] = true

	request := RequestVoteArgs{}
	request.Term = rf.currentTerm
	request.CandidatedID = rf.me
	request.LastLogIndex = len(rf.log) - 1
	request.LastLogTerm = rf.log[request.LastLogIndex].Term
	rf.mu.Unlock()

	for i := 0; i < n; i++ {
		if i != rf.me {
			go func(peer int, voted []bool) {

				reply := RequestVoteReply{}
				send_ok := rf.sendRequestVote(peer, &request, &reply)
				voted[peer] = send_ok && reply.VoteGranted
				if CheckIfWinHalfVote(voted, n) {
					win <- true
				}

			}(i, voted)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Start] me:%v command:%v rf.role:%v", rf.me, command, rf.State)

	isLeader = (rf.State == Leader)

	if isLeader {
		index = rf.nextIndex[rf.me]
		term = rf.currentTerm

		//If command received from client: append entry to local log, respond after entry applied to state machine(push to ApplyMsg)
		rf.log = append(rf.log, LogEntry{command, term})
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1

		rf.persist()

		DPrintf("[Start] me:%d command:%v index:%v rf.Logs:%v", rf.me, command, index, rf.log)
	}

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
	DPrintf("[Kill] me:%d", rf.me)
	//加锁避免AppendEntries线程里写了ApplyMsg并返回response，但是未来得及持久化
	//该线程Kill然后Make
	rf.mu.Lock()
	close(rf.receiveQuit)
	close(rf.quitCheckRoutine)
	rf.mu.Unlock()
	DPrintf("[Kill] me:%d return", rf.me)
}

func (rf *Raft) BeFollower() {
	rf.State = Follower
	if rf.currentTerm == 0 {
		DPrintf("[BeFollower] raft %d be follower in term %d\n", rf.me, rf.currentTerm)
	}
	for {
		select {
		// 在Candidate状态, Leader状态时,如果rpc调用返回失败,就会进入这个case
		// rf.changeToFollower 表示rpc调用返回的结果,其中包含着对方的term信息
		case v := <-rf.changeToFollower:
			if v.term > rf.currentTerm {
				go rf.TransitionToFollower(v)
				return
			}
			rf.changeToFollowerDone <- true
			if v.shouldResetTimer {
				rf.timer.Reset(time.Duration(rf.ElectionTimeout()) * time.Millisecond)
			}
		case <-rf.timer.C:
			DPrintf("[BeFollower] me:%d timeout", rf.me)
			go rf.BeCandidate()
			return
		case <-rf.receiveQuit:
			DPrintf("[BeFollower] me:%d quit", rf.me)
			return

		}
	}
}

func (rf *Raft) BeCandidate() {
	rf.State = Candidate
	for {
		DPrintf("[BeCandidate] raft %d be candidate in term %d\n", rf.me, rf.currentTerm+1)
		vote_ended := make(chan bool, len(rf.peers))
		go rf.StartElection(vote_ended)
		rf.timer.Reset(time.Duration(rf.ElectionTimeout()) * time.Millisecond)

		select {
		case v := <-rf.changeToFollower:
			DPrintf("[BeCandidate] raft %d trans to follower in term %d", rf.me, rf.currentTerm)
			go rf.TransitionToFollower(v)
			return

		case <-rf.receiveQuit:
			return
		case win := <-vote_ended:
			if win {
				go rf.BeLeader()
				return
			}
		case <-rf.timer.C:
			DPrintf("[BeCandidate] me:%d timeout", rf.me)

		}

	}
}

func (rf *Raft) BeLeader() {
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果下面的循环中, 状态已经转变为Follower,那么直接退出该协程
		if rf.State != Candidate {
			return
		}
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log)
		}

		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.State = Leader
		DPrintf("[BeLeader] raft %d be leader in term %d\n", rf.me, rf.currentTerm)
	}()

	for {
		select {
		case v := <-rf.changeToFollower:
			DPrintf("[Beleader] raft %d trans to follower: %v", rf.me, v)
			go rf.TransitionToFollower(v)
			return
		case <-rf.receiveQuit:
			return
		default:
			// 等待前面的go routine 完成
			DPrintf("[BeLeader] me:%d default. rf.role:%v", rf.me, rf.State)
			if rf.State == Leader {
				rf.SendLogEntryMessageToAll()
				time.Sleep(HEARTBEATTIMEOUT * time.Millisecond)
			}


		}
	}
}

func (rf *Raft) TransitionToFollower(c ChangeToFollower) {
	rf.State = Follower
	//if c.votedFor != -1 {
	//	rf.votedFor = c.votedFor
	//}
	rf.votedFor = c.votedFor
	if rf.currentTerm < c.term {
		rf.currentTerm = c.term
	}
	rf.InitNextIndex()
	rf.persist()
	rf.changeToFollowerDone <- true
	if c.shouldResetTimer {
		rf.timer.Reset(time.Duration(rf.ElectionTimeout()) * time.Millisecond)
	}

	rf.BeFollower()
}

func (rf *Raft) CheckMatchIndexAndSetCommitIndex() {
	//DPrintf("[CheckMatchIndexAndSetCommitIndex] rf.me:%v begin.", rf.me)

}

func (rf *Raft) makeAppendEntryArgs(server_index int) (*AppendEntriesArgs, int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != Leader {
		return nil, -1
	}

	newEntryIndex := rf.nextIndex[server_index]
	endIndex := len(rf.log)

	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.LeaderCommit = rf.commitIndex
	if newEntryIndex != -1 && endIndex > 0 {
		args.Entries = rf.log[newEntryIndex:endIndex]

	}
	args.PrevLogIndex = newEntryIndex - 1
	if args.PrevLogIndex < 0 {
		args.PrevLogIndex = 0
	}
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

	return args, endIndex
}
func (rf *Raft) HandleInconsistency(server_index int) {
	DPrintf("[HandleInconsistency] leader raft %d meet Inconsistence with raft %d ", rf.me, server_index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	nextIndex := rf.nextIndex[server_index]

	if nextIndex > 0 && rf.log[nextIndex].Term == rf.log[nextIndex-1].Term {
		rf.nextIndex[server_index]--
	}
}

func (rf *Raft) SendLogEntryMessageToAll() {
	//DPrintf("[SendLogEntryMessageToAll] me:%d begin", rf.me)
	n := len(rf.peers)

	for i := 0; i < n; i++ {
		if i != rf.me {
			go func(server_index int) {
				args, max_log_entry_index := rf.makeAppendEntryArgs(server_index)
				if args == nil {
					return
				}
				//DPrintf("[SendLogEntryMessageToAll] from me:%d to server_index:%d request:%v", rf.me, server_index, args)

				reply := AppendEntriesReply{}
				send_ok := rf.sendAppendEntries(server_index, args, &reply)
				rf.mu.Lock()
				sameTerm := (rf.currentTerm == args.Term)
				rf.mu.Unlock()

				if send_ok && sameTerm {
					if reply.Success {
						rf.nextIndex[server_index] = max_log_entry_index
						rf.matchIndex[server_index] = max_log_entry_index - 1
						rf.followerAppendEntries <- true
					} else {
						if reply.Term == rf.currentTerm {
							rf.HandleInconsistency(server_index)
						} else if reply.Term > rf.currentTerm {
							ch := ChangeToFollower{reply.Term, -1, false}
							rf.PushChangeToFollower(ch)
						}
					}
				}

			}(i)
		}
	}
	//DPrintf("[SendLogEntryMessageToAll] me:%d end", rf.me)

}

func (rf *Raft) InitNextIndex() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
}

func (rf *Raft) ElectionTimeout() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(ELECTIONTIMEOUT) + ELECTIONTIMEOUT
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
	DPrintf("start initialized raft_%d\n", rf.me)
	rf.currentTerm = 0
	rf.votedFor = -1
	// 提前填充一个没有意义的LogEntry, 之后的xxxIndex可以直接做下标, 但是计算len的时候记得减1
	rf.log = append(rf.log, LogEntry{-1, rf.currentTerm})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.InitNextIndex()
	rf.matchIndex = make([]int, len(peers))

	rf.receiveQuit = make(chan bool)
	rf.quitCheckRoutine = make(chan bool)
	rf.changeToFollower = make(chan ChangeToFollower)
	rf.changeToFollowerDone = make(chan bool)
	rf.followerAppendEntries = make(chan bool)

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.timer = time.NewTimer(time.Duration(rf.ElectionTimeout()) * time.Millisecond)

	go rf.BeFollower()
	//go rf.CheckMatchIndexAndSetCommitIndex()
	return rf
}
