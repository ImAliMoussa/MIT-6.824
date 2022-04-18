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
	// "github.com/sasha-s/go-deadlock"
	"math/rand"
	"sync"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type RaftState string

const (
	LEADER    RaftState = "Leader"
	FOLLOWER  RaftState = "Follower"
	CANDIDATE RaftState = "Candidate"
)

const (
	// time in milliseconds
	LEADER_TIMEOUT = 60 * time.Millisecond
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
	mu sync.Mutex // Lock to protect shared access to this peer's state
	// mu        deadlock.Mutex
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

	applyCh      chan ApplyMsg
	commandCh    chan ApplyMsg
	stepDownCh   chan bool
	wonElectonCh chan bool
	heartbeatCh  chan bool
	killCh       chan bool

	state         RaftState
	numPeers      int
	baseIndex     int
	skipBroadcast bool
}

// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date.
func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	lastTerm := rf.getLog(rf.logLength() - 1).Term
	lengthOfLog := rf.logLength()
	if lastTerm != args.LastLogTerm {
		return lastTerm < args.LastLogTerm
	}
	return lengthOfLog <= (1 + args.LastLogIndex)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) initializeVolatileState() {
	for i := 0; i < rf.numPeers; i++ {
		rf.nextIndex[i] = rf.logLength()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.logLength()
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		trace("Server", rf.me, "has received a new command", command, "with index", index)
		entry := LogEntry{
			Term:    term,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		go rf.broadcastAppendEntries()
		rf.skipBroadcast = true
	}

	return index, term, isLeader
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(200+rand.Intn(300)) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		skip := rf.skipBroadcast
		rf.skipBroadcast = false
		trace("Server", rf.me, "new tick.\nState", state,
			"\nCurrentTerm:", rf.currentTerm,
			"\nLogs:", rf.log,
			"\nLogs length:", rf.logLength(),
			"\nCommit index:", rf.commitIndex,
			"\nMatch index", rf.matchIndex,
			"\nNext index", rf.nextIndex,
			"\nVoted for", rf.votedFor,
		)
		rf.mu.Unlock()

		// assume candidate or leader and change if leader
		timeout := rf.getElectionTimeout()

		switch state {
		case LEADER:
			timeout = LEADER_TIMEOUT
			if !skip {
				go rf.broadcastAppendEntries()
			}
		case CANDIDATE:
			go rf.startElection()
		}

		select {
		case <-rf.heartbeatCh:
		case <-rf.stepDownCh:
		case <-rf.wonElectonCh:
			rf.lead()
		case <-rf.killCh:
		case <-time.After(timeout):
			trace("Server", rf.me, "has timed out")
			if state == FOLLOWER {
				rf.becomeCandidate()
			}
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

	numPeers := len(peers)
	startTerm := 0

	// Create log with a dummy first entry to make managing edge
	// cases easier
	log := make([]LogEntry, 1)
	log[0] = LogEntry{Term: startTerm}

	N := 1000
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		currentTerm: startTerm,
		votedFor:    -1,
		log:         log,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, numPeers),
		matchIndex:  make([]int, numPeers),

		// Channels have buffered space to not block
		// which should avoid deadlock in some scenarios
		applyCh:      applyCh,
		commandCh:    make(chan ApplyMsg, N),
		stepDownCh:   make(chan bool, N),
		heartbeatCh:  make(chan bool, N),
		wonElectonCh: make(chan bool, N),
		killCh:       make(chan bool, N),

		state:     FOLLOWER,
		numPeers:  numPeers,
		baseIndex: 0,
	}

	// initialize from state persisted before a crash
	rf.ReadPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
