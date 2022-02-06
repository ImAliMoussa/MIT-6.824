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
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
	//	"6.824/labgob"
	"6.824/labrpc"
)

type RfState string

const (
	LEADER    RfState = "Leader"
	FOLLOWER          = "Follower"
	CANDIDATE         = "Candidate"
)

const (
	// time in milliseconds
	LEADER_TIMEOUT = 125
)

type LogEntry struct {
	Term    int
	Command interface{}
}

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

//
// A Go object implementing a single Raft peer.
//
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
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	applyCh       chan ApplyMsg
	stepDownCh    chan bool
	commandCh     chan bool
	killCh        chan bool

	state    RfState
	numPeers int
}

// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date.
func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	lastTerm := rf.log[len(rf.log)-1].Term
	lengthOfLog := len(rf.log)

	return lastTerm <= args.LastLogTerm && lengthOfLog <= (1+args.LastLogIndex)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	trace("Server", rf.me, "is trying to acquire lock")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	trace("Server", rf.me, "has acquired the lock")

	return rf.currentTerm, rf.state == LEADER
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

//
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (r *RequestVoteArgs) String() string {
	out, _ := json.Marshal(r)
	return string(out)
}

func (r *RequestVoteReply) String() string {
	out, _ := json.Marshal(r)
	return string(out)
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	trace("Server", rf.me, "is trying to acquire lock")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	trace("Server", rf.me, "has acquired the lock")
	trace("Server", rf.me, "has received a request vote", args.String())
	reply.Term = rf.currentTerm

	// TODO when is voted for reset to -1?
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.isUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		// reset follower timer when follower grants a vote
		if rf.state == FOLLOWER {
			rf.commandCh <- true
		} else {
			rf.stepDownCh <- true
		}
		rf.state = FOLLOWER
	} else {
		reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() int {
	// TODO check response for success equal false
	// If false and term is more than currentTerm then step down and
	// become a follower
	trace("Candidate", rf.me, "trying to acquire lock in startElection")
	rf.mu.Lock()
	trace("Candidate", rf.me, "received lock in startElection")

	lengthOfLog := len(rf.log)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lengthOfLog - 1,
		LastLogTerm:  rf.log[lengthOfLog-1].Term,
	}

	rf.mu.Unlock()

	votes := 1
	for i := 0; i < rf.numPeers; i++ {
		if i == rf.me {
			continue
		}
		trace("Candidate", rf.me, "sent a request vote to", i)
		reply := RequestVoteReply{}
		rf.sendRequestVote(i, &args, &reply)

		trace("Candidate", rf.me, "has received a response from server", i, "\nResponse:", reply)
		if reply.VoteGranted {
			votes++
		}
	}
	trace("Server", rf.me, "exiting startElection")
	return votes
}

func (rf *Raft) initializeVolatileState() {
	for i := 0; i < rf.numPeers; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
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
	// rf.killCh <- true
	trace("Server", rf.me, "kill signal was received")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getElectionTimeout() time.Duration {
	minimum := LEADER_TIMEOUT * 6
	maximum := LEADER_TIMEOUT * 10
	difference := maximum - minimum
	value := minimum + (rand.Int() % difference)
	trace("Server", rf.me, "has with electionTimeout of", value)
	return time.Duration(value)
}

func (rf *Raft) stepDownToFollower(newTerm int) {
	trace("Server", rf.me, "is trying to acquire lock")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	trace("Server", rf.me, "has acquired the lock")

	trace("Server", rf.me, "is stepping down to follower")

	oldState := rf.state
	rf.state = FOLLOWER
	rf.currentTerm = newTerm

	if oldState == LEADER || oldState == FOLLOWER {
		trace("Server", rf.me, "is trying to trying to step down")
		rf.stepDownCh <- true
		trace("Server", rf.me, "has stepped down")
	}
}

func (rf *Raft) stepUpToCandidate() {
	trace("Server", rf.me, "is trying to acquire lock")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	trace("Server", rf.me, "has acquired the lock")

	rf.state = CANDIDATE
	rf.currentTerm++
}

//
// Server has become a leader so it updates state and
// sends a heartbeat as specified by paper to let other
// server know that there is a new leader.
//
func (rf *Raft) stepUpToLeader() {
	trace("Leader", rf.me, "has been promoteToLeader")
	trace("Server", rf.me, "is trying to acquire lock")
	rf.mu.Lock()
	trace("Leader", rf.me, "has acquired the lock")
	rf.state = LEADER
	rf.initializeVolatileState()
	rf.mu.Unlock()
	trace("Server", rf.me, "has acquired the lock")
	rf.broadcastAppendEntries()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		trace("Server", rf.me, "will try to acquire lock")
		rf.mu.Lock()
		trace("Server", rf.me, "has acquired the lock")
		state := rf.state
		trace("Server", rf.me, "in loop again with state", state)
		rf.mu.Unlock()
		if state == LEADER {
			select {
			case <-rf.stepDownCh:
				trace("Leader", rf.me, "is stepping down to a follower")
			case <-time.After(LEADER_TIMEOUT * time.Millisecond):
				trace("Leader", rf.me, "has timed out")
				trace("Leader", rf.me, "is sending a broadcast")
				rf.broadcastAppendEntries()
			case <-rf.killCh:
				trace("Server", rf.me, "was killed")
			}
		} else if state == FOLLOWER {
			electionTimeout := rf.getElectionTimeout() * time.Millisecond
			select {
			case <-rf.commandCh:
				// received a heartbeat from the leader
				// do nothing
				trace("Follower", rf.me, "received heartbeat from leader")
			case <-time.After(electionTimeout):
				trace("Follower", rf.me, "has timed out")
				trace("Follower", rf.me, "has become a candidate")
				rf.stepUpToCandidate()
			case <-rf.killCh:
				trace("Server", rf.me, "was killed")
			}
		} else if state == CANDIDATE {
			electionTimeout := rf.getElectionTimeout() * time.Millisecond
			select {
			case <-rf.stepDownCh:
				trace("Candidate", rf.me, "is stepping down to a follower")
			case <-time.After(electionTimeout):
				trace("Candidate", rf.me, "will try the election")
				rf.stepUpToCandidate()
				votes := rf.startElection()
				if votes > rf.numPeers / 2 {
					rf.stepUpToLeader()
				}
			case <-rf.killCh:
				trace("Server", rf.me, "was killed")
			}
		} else {
			panic("Wrong raft server state")
		}
	}
	trace("Server", rf.me, "has exited properly")
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
	// Set logging flags
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	if os.Getenv("TURN_OFF_LOGS") != "" {
		log.SetOutput(ioutil.Discard)
	}

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.numPeers = len(peers)
	rf.state = FOLLOWER

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]LogEntry, 1)
	// Add a dummy log entry to make indexing start from 1 like the paper suggested.
	// Should make some things easier
	rf.log[0] = LogEntry{Term: 0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	// TODO Reinitialize on elections
	rf.nextIndex = make([]int, rf.numPeers)
	rf.matchIndex = make([]int, rf.numPeers)

	rf.applyCh = applyCh
	rf.stepDownCh = make(chan bool)
	rf.commandCh = make(chan bool)
	rf.killCh = make(chan bool)

	rf.initializeVolatileState()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
