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
	Term             int
	Success          bool
	ConflictLogIndex int
	ConflictLogTerm  int
}

func (rf *Raft) updateIndexes(server, previousLogIndex, lengthEntries int) {
	newMatchIndex := previousLogIndex + lengthEntries
	trace("Leader", rf.me, "updating match index of server", server, "\nOld:", rf.matchIndex[server], "\nNew:", newMatchIndex)

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
	arr := make([]int, rf.numPeers-1)
	for i, j := 0, 0; i < rf.numPeers; i++ {
		if i == rf.me {
			continue
		}
		arr[j] = rf.matchIndex[i]
		j++
	}
	sort.Ints(arr)
	newCommitIndex := arr[rf.numPeers/2]

	/*
		for N := len(rf.log) - 1; N >= 0; N-- {
			if rf.log[N].Term != rf.currentTerm {
				break
			}
			group := 1 // group includes leader
			for i := 0; i < rf.numPeers; i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N {
					group++
				}
			}
			trace("Server", rf.me, "with N =", N, "and group", group)
			if group > (rf.numPeers / 2) {
				if N != newCommitIndex {
					errMsg := fmt.Sprintln("Server:", rf.me, "faulty logic in sorting code N:", N, "newCommitIndex:", newCommitIndex, "lengthLog:", len(rf.log), rf.matchIndex)
					panic(errMsg)
				}
				break
			}
		}
	*/
	// A leader is not allowed to update commitIndex to somewhere in a previous term
	// (or, for that matter, a future term). Thus, as
	// the rule says, you specifically need to check that log[N].term == currentTerm. This is because Raft leaders cannot
	// be sure an entry is actually committed (and will not ever be changed
	// in the future) if it’s not from their current term.
	// This is illustrated by Figure 8
	trace("Leader", rf.me, "with logs:\n", rf.log, "\nNew commit index:", newCommitIndex, "\nOld commit index:", rf.commitIndex, "\nMatch indexes:", rf.matchIndex, "\nLogs:", rf.log)
	if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex].Term == rf.currentTerm {
		rf.commitIndex = newCommitIndex
		trace("Leader", rf.me, "updated commit index to", rf.commitIndex)
		go rf.applyCommittedCommands()
	}
}

func (rf *Raft) applyCommittedCommands() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for server := 0; server < rf.numPeers; server++ {
		if server != rf.me {
			go rf.sendAppendEntries(server, rf.currentTerm)
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
	previousLogTerm := rf.log[previousLogIndex].Term

	// clone entries to avoid race conditions
	entriesSlice := rf.log[nextLogIndex:len(rf.log)]
	entriesClone := make([]LogEntry, len(entriesSlice))
	copy(entriesClone, entriesSlice)

	trace("Server", rf.me, "nextLogIndex:", nextLogIndex, "previousLogIndex:", previousLogIndex, "enteries:", entriesClone)

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
	trace("Server", rf.me, "is going to send an append entry to server", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	trace("Server", rf.me, "has sent an append entry to", server,
		"\nOk:", ok,
		"\nArgs:", args.String(),
		"\nReply:", reply.String())

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		// All is good
		rf.updateIndexes(server, previousLogIndex, len(args.Entries))
	} else if ok {
		// Successful RPC request & response but operation failed
		if reply.Term > term {
			state, err := rf.follow(reply.Term, -1)
			if err == nil && (state == LEADER || state == CANDIDATE) {
				rf.stepDownCh <- true
			}
		} else {
			rf.updateNextIndex(server, &reply)
		}
	}
	trace("Server", rf.me, "has finished append entries to", server)
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	trace("Server", rf.me, "is tying to acquire lock")
	rf.mu.Lock()
	trace("Server", rf.me, "has acquired the lock")
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	oldTerm := args.Term < rf.currentTerm
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	differentTerms := (args.PrevLogIndex >= len(rf.log)) || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm)

	reply.Term = rf.currentTerm

	trace("Server", rf.me, "oldTerm", oldTerm, "differentTerms", differentTerms)
	if oldTerm || differentTerms {
		reply.Success = false

		conflictIndex, conflictTerm := rf.getConflictingData(args)
		reply.ConflictLogIndex = conflictIndex
		reply.ConflictLogTerm = conflictTerm

		return
	}

	reply.Success = true

	// Append extra entries and remove any conflicting entries
	for i, j := args.PrevLogIndex+1, 0; j < len(args.Entries); i, j = i+1, j+1 {
		// Conflicting entries, must delete this entry and all that follow
		if i < len(rf.log) && rf.log[i].Term != args.Entries[j].Term {
			// Delete entries starting from index i
			if i <= rf.commitIndex {
				errMsg := fmt.Sprintln("Server", rf.me, "\nArgs:", args, "\nMyLogs:", rf.log, "\nCommitIndex:", rf.commitIndex)
				panic(errMsg)
			}
			rf.log = rf.log[0:i]
		}

		if i >= len(rf.log) {
			// Append all remaining entries and break
			rf.log = append(rf.log, args.Entries[j:]...)
			break
		} else {
			// Sanity check
			if !reflect.DeepEqual(rf.log[i], args.Entries[j]) {
				errMsg := fmt.Sprintln("Why are they not equal", rf.log[i], args.Entries[j])
				panic(errMsg)
			}
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	trace("Server", rf.me, "\nLogs:", rf.log, "\nCommit index:", rf.commitIndex)

	go rf.applyCommittedCommands()

	state, err := rf.follow(args.Term, -1)
	if err == nil {
		trace("Server", rf.me, "is going to send message in channel.\nState: ", state)
		if state == FOLLOWER {
			rf.heartbeatCh <- true
		} else if state == LEADER || state == CANDIDATE {
			rf.stepDownCh <- true
		} else {
			panic("wrong state")
		}
	}
}

// Upon receiving a conflict response, the leader should first search its log for conflictTerm. If it finds an entry in its log with
// that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
//
// If it does not find an entry with that term, it should set nextIndex = conflictIndex.
//
func (rf *Raft) updateNextIndex(server int, reply *AppendEntriesResponse) {
	if reply.Success || reply.ConflictLogIndex < 0 {
		panic("This shouldn't be happening")
	}

	found := false
	for index := len(rf.log) - 1; index >= 0; index-- {
		if rf.log[index].Term == reply.ConflictLogTerm {
			found = true
			rf.nextIndex[server] = index + 1
		}
	}

	if !found {
		rf.nextIndex[server] = reply.ConflictLogIndex
	}
}

//
// If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
//
// If a follower does have prevLogIndex in its log, but the term does not match, it should return
// conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has
// term equal to conflictTerm.
//
// returns ConflictLogIndex, ConflictLogTerm
//
func (rf *Raft) getConflictingData(args *AppendEntriesRequest) (int, int) {
	logLength := len(rf.log)
	if args.PrevLogIndex >= logLength {
		return logLength, -1
	}

	conflictLogTerm := rf.log[args.PrevLogIndex].Term
	conflictLogIndex := -1

	for idx := 0; idx < logLength; idx++ {
		if rf.log[idx].Term == conflictLogTerm {
			conflictLogIndex = idx
			break
		}
	}

	return conflictLogIndex, conflictLogTerm
}
