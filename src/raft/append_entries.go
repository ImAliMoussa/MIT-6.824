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
}

func (rf *Raft) updateIndexes(server, previousLogIndex, lengthEntries int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	// A leader is not allowed to update commitIndex to somewhere in a previous term
	// (or, for that matter, a future term). Thus, as
	// the rule says, you specifically need to check that log[N].term == currentTerm. This is because Raft leaders cannot
	// be sure an entry is actually committed (and will not ever be changed
	// in the future) if it’s not from their current term.
	// This is illustrated by Figure 8
	trace("Leader", rf.me, "with logs:\n", rf.log, "\nNew commit index:", newCommitIndex, "\nOld commit index:", rf.commitIndex)
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
	// TODO remove for loop and implement next index in reply
	for !rf.killed() {
		rf.mu.Lock()
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

		// All is good
		if reply.Success {
			rf.updateIndexes(server, previousLogIndex, len(args.Entries))
			break
		}

		// successful rpc but unsuccessful append entry, should retry
		if ok {
			if reply.Term > term {
				rf.mu.Lock()
				state, err := rf.follow(reply.Term, -1)
				rf.mu.Unlock()
				if err == nil && (state == LEADER || state == CANDIDATE) {
					rf.stepDownCh <- true
				}
				break
			}
			rf.mu.Lock()
			rf.nextIndex[server]--
			if rf.nextIndex[server] < 0 {
				panic("Should always be non-negative")
			}
			rf.mu.Unlock()
			trace("Leader", rf.me, "will retry sending append entry to", server)
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
		return
	}

	reply.Success = true

	// append extra entries and remove any conflicting entries
	for i, j := args.PrevLogIndex+1, 0; j < len(args.Entries); i, j = i+1, j+1 {
		// conflicting entries, must delete this entry and all that follow
		if i < len(rf.log) && rf.log[i].Term != args.Entries[j].Term {
			// delete entries starting from index i
			rf.log = rf.log[0:i]
		}

		if i >= len(rf.log) {
			// append all remaining entries and break
			rf.log = append(rf.log, args.Entries[j:]...)
			break
		} else {
			// this should be redundant anyway so do some sort of deep equality as a sanity check
			if !reflect.DeepEqual(rf.log[i], args.Entries[j]) {
				errMsg := fmt.Sprintln("Why are they not equal", rf.log[i], args.Entries[j])
				panic(errMsg)
			}
			// rf.log[i] = args.Entries[j]
		}
	}

	trace("Server", rf.me, "with new logs:", rf.log)
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

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
