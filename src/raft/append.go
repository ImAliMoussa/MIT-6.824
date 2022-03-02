package raft

import (
	"fmt"
	"reflect"
	"sort"
)

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool

	//
	// Reference: https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt
	//

	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

func (rf *Raft) updateIndexes(server, newMatchIndex int) {
	// sanity check
	if newMatchIndex > rf.matchIndex[server] {
		rf.matchIndex[server] = newMatchIndex
		rf.nextIndex[server] = newMatchIndex + 1
		rf.updateCommitIndex()
	}
}

func (rf *Raft) updateCommitIndex() {
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).

	// copy all match index values to an array (except index of self)
	// and sort the array
	// the nth index of the array has n values larger than it and thus is least value in the
	// majority
	// leetcode type problem lol
	arr := make([]int, rf.numPeers)
	copy(arr, rf.matchIndex)
	sort.Ints(arr)
	newCommitIndex := arr[rf.numPeers/2+1]

	// A leader is not allowed to update commitIndex to somewhere in a previous term
	// (or, for that matter, a future term). Thus, as
	// the rule says, you specifically need to check that log[N].term == currentTerm. This is because Raft leaders cannot
	// be sure an entry is actually committed (and will not ever be changed
	// in the future) if it’s not from their current term.
	// This is illustrated by Figure 8
	if newCommitIndex > rf.commitIndex && rf.getLog(newCommitIndex).Term == rf.currentTerm {
		rf.commitIndex = newCommitIndex
		trace("Server", rf.me, "updated commit index to", rf.commitIndex)
		go rf.applyCommittedCommands()
	}
}

func (rf *Raft) applyCommittedCommands() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Persist()

	trace("Server", rf.me, "commit index:", rf.commitIndex)

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.getLog(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		trace("Server", rf.me, "applied index", rf.lastApplied, "command:", applyMsg.Command)
		rf.applyCh <- applyMsg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Persist()

	trace("Server", rf.me, "is broadcasting append entries for term", rf.currentTerm)

	for server := 0; server < rf.numPeers; server++ {
		if server != rf.me {
			if rf.nextIndex[server] <= rf.baseIndex {
				go rf.sendInstallSnapshot(server)
			} else {
				go rf.sendAppendEntries(server, rf.currentTerm)
			}
		}
	}
}

// Issued by Leader to send AppendEntriesRPC to followers
func (rf *Raft) sendAppendEntries(server, term int) {
	rf.mu.Lock()

	//
	// check state hasn't state since function was called
	// this avoids a nasty bug as follows:
	// 1) sendAppendEntries was called but hasn't acquired lock yet so is blocked
	// 2) leader steps down due to getting a response with higher term
	// 3) leader updates term and steps down
	// 4) sendAppendEntries acquires lock and sends request with updated term, when it should have sent
	// an entry with the old term. This has bad consequences.
	//

	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	nextLogIndex := rf.nextIndex[server]
	previousLogIndex := nextLogIndex - 1
	previousLogTerm := rf.getLog(previousLogIndex).Term

	// clone entries to avoid race conditions
	entriesSlice := rf.sliceLog(nextLogIndex, rf.logLength())
	entriesClone := make([]LogEntry, len(entriesSlice))
	copy(entriesClone, entriesSlice)

	args := AppendEntriesRequest{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: previousLogIndex,
		PrevLogTerm:  previousLogTerm,
		Entries:      entriesClone,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesResponse{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		trace("Server", rf.me, "has sent an append entry to server", server,
			"\nOk:", ok,
			"\nArgs:", args.String(),
			"\nReply:", reply.String())
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		// All is good
		newMatchIndex := previousLogIndex + len(args.Entries)
		rf.updateIndexes(server, newMatchIndex)
	} else if ok {
		// Successful RPC request & response but operation failed
		if reply.Term > term {
			state := rf.follow(reply.Term)
			rf.votedFor = -1
			if state == LEADER || state == CANDIDATE {
				rf.stepDownCh <- true
			}
		} else {
			rf.updateNextIndex(server, &reply, &args)
			trace("Server", rf.me, "updated next index for server ", server, "to:", rf.nextIndex[server])
		}
	}

	rf.Persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	if rf.state == FOLLOWER {
		rf.heartbeatCh <- true
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	differentTerms := (args.PrevLogIndex >= rf.logLength()) ||
		(rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm)

	if differentTerms {
		trace(
			"Server", rf.me, "rejected append entry with args:", args.String(),
			"\nDifferentTerm:", differentTerms,
			"\nLogs:", rf.log,
		)

		xTerm, xIndex, xLen := rf.getConflictingData(args)
		reply.XTerm = xTerm
		reply.XIndex = xIndex
		reply.XLen = xLen

		return
	}

	trace("Server", rf.me, "just received append entry with args:", args.String(),
		"\nLogs:", rf.log,
		"\nCommit index:", rf.commitIndex,
	)

	reply.Success = true

	// Append extra entries and remove any conflicting entries
	for i, j := args.PrevLogIndex+1, 0; j < len(args.Entries); i, j = i+1, j+1 {
		// Conflicting entries, must delete this entry and all that follow
		if i < rf.logLength() && rf.getLog(i).Term != args.Entries[j].Term {
			// Sanity check, should never delete commited logs and thus committed logs are never conflicting
			if i <= rf.commitIndex {
				errMsg := fmt.Sprintln("Panic error!!! Server", rf.me,
					"\nArgs:", args,
					"\nMyLogs:", rf.log,
					"\nCommitIndex:", rf.commitIndex,
				)
				panic(errMsg)
			}
			// Delete entries starting from index i
			rf.log = rf.sliceLog(rf.baseIndex, i)
		}

		if i >= rf.logLength() {
			// Append all remaining entries and break
			rf.log = append(rf.log, args.Entries[j:]...)
			break
		} else {
			// Sanity check
			if !reflect.DeepEqual(rf.getLog(i), args.Entries[j]) {
				errMsg := fmt.Sprintln("Why are they not equal", rf.getLog(i), args.Entries[j])
				panic(errMsg)
			}
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logLength()-1)
	}

	trace("Server", rf.me, "\nLogs:", rf.log, "\nCommit index:", rf.commitIndex)

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}
	state := rf.follow(args.Term)

	rf.Persist()

	go rf.applyCommittedCommands()

	if state == LEADER || state == CANDIDATE {
		rf.stepDownCh <- true
	}
}

//
// how to roll back quickly
//   the Figure 2 design backs up one entry per RPC -- slow!
//   lab tester may require faster roll-back
//   paper outlines a scheme towards end of Section 5.3
//     no details; here's my guess; better schemes are possible
//       Case 1      Case 2       Case 3
//   S1: 4 5 5       4 4 4        4
//   S2: 4 6 6 6 or  4 6 6 6  or  4 6 6 6
//   S2 is leader for term 6, S1 comes back to life, S2 sends AE for last 6
//     AE has prevLogTerm=6
//   rejection from S1 includes:
//     XTerm:  term in the conflicting entry (if any)
//     XIndex: index of first entry with that term (if any)
//     XLen:   log length
//   Case 1 (leader doesn't have XTerm):
//     nextIndex = XIndex
//   Case 2 (leader has XTerm):
//     nextIndex = leader's last entry for XTerm
//   Case 3 (follower's log is too short):
//     nextIndex = XLen
//
func (rf *Raft) updateNextIndex(server int, reply *AppendEntriesResponse, args *AppendEntriesRequest) {
	// for logging
	oldNextIndex := rf.nextIndex[server]

	nextIndex := -1

	if reply.XTerm == -1 {
		// Case 3, follower's log is too short
		nextIndex = reply.XLen
	} else {

		lastXtermIndexInLeader := -1
		for i := rf.baseIndex; i < rf.logLength(); i++ {
			if rf.getLog(i).Term == reply.XTerm {
				lastXtermIndexInLeader = i
			}
		}

		if lastXtermIndexInLeader == -1 {
			// Case 1, leader doesn't have xterm
			nextIndex = reply.XIndex
		} else {
			// Case 2, leader has xterm
			nextIndex = lastXtermIndexInLeader
		}
	}

	nextIndex = min(nextIndex, rf.logLength())

	trace(
		"Server", rf.me,
		"updating next index for server", server,
		"\nArgs:", args,
		"\nReply:", reply,
		"\nOld next index:", oldNextIndex,
		"\nNew next index:", nextIndex,
	)

	rf.nextIndex[server] = nextIndex
}

//
// returns XTerm, XIndex, XLen
//
func (rf *Raft) getConflictingData(args *AppendEntriesRequest) (int, int, int) {
	logLength := rf.logLength()
	if args.PrevLogIndex >= logLength {
		return -1, -1, logLength
	}

	xTerm := rf.getLog(args.PrevLogIndex).Term
	xIndex := -1
	for i := rf.baseIndex; i < logLength; i++ {
		if rf.getLog(i).Term == xTerm {
			xIndex = i
			break
		}
	}

	if xIndex == -1 {
		panic("Wrong XIndex value in getConflictingData")
	}

	return xTerm, xIndex, logLength
}
