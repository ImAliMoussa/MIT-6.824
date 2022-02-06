package raft

import (
	"encoding/json"
)

// lol why do I have to implement min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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

func (a *AppendEntriesRequest) String() string {
	out, _ := json.Marshal(a)
	return string(out)
}

func (a *AppendEntriesResponse) String() string {
	out, _ := json.Marshal(a)
	return string(out)
}

func (rf *Raft) broadcastAppendEntries() {
	for server := 0; server < rf.numPeers; server++ {
		if server != rf.me {
			go rf.sendAppendEntries(server)
		}
	}
}

// Issued by Leader to send AppendEntriesRPC to followers
func (rf *Raft) sendAppendEntries(server int) {
	trace("Server", rf.me, "is trying to acquire lock")
	rf.mu.Lock()
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	trace("Server", rf.me, "has acquired the lock")
	ok := false
	// ok is a variable that stores a boolean that is false as long as the rpc call fails
	// The paper states to indefinetly retry RPC calls as long as they fail.
	for !ok {
		nextLogIndex := rf.nextIndex[server]
		previousLogIndex := nextLogIndex - 1
		trace("Server", rf.me, "indexes:", nextLogIndex, previousLogIndex)
		previousLogTerm := rf.log[previousLogIndex].Term

		args := AppendEntriesRequest{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: previousLogIndex,
			PrevLogTerm:  previousLogTerm,
			Entries:      rf.log[nextLogIndex:len(rf.log)],
			LeaderCommit: commitIndex,
		}

		reply := AppendEntriesResponse{}

		ok = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		trace("Server", rf.me, "has sent an append entry to", server,
			"\nArgs:", args.String(),
			"\nReply:", reply.String())

		rf.mu.Lock()
		term = rf.currentTerm
		rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.stepDownToFollower(reply.Term)
			return
		} else if reply.Success == false && ok {
			// ok signals that the rpc successfully reached the server and reply.Success
			// means the server rejected it
			rf.nextIndex[server]--
			ok = false
			trace("Leader", rf.me, "will retry sending append entry to", server)
		}
	}
	trace("Server", rf.me, "has finished append entries to", server)
}

func (rf *Raft) commitLogEntry(index int) {
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      rf.log[index].Command,
		CommandIndex: index,
	}
	rf.applyCh <- applyMsg
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	trace("Server", rf.me, "is tying to acquire lock")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	trace("Server", rf.me, "has acquired the lock")

	// TODO what if this raft server is a leader?

	// Reply false if term < currentTerm (§5.1)
	oldTerm := args.Term < rf.currentTerm
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	differentTerms := (args.PrevLogIndex >= len(rf.log)) || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm)

	trace("Server", rf.me, "oldTerm", oldTerm, "differentTerms", differentTerms)
	if oldTerm || differentTerms {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.commandCh <- true
	rf.state = FOLLOWER
	rf.currentTerm = args.Term

	for i, j := args.PrevLogIndex+1, 0; j < len(args.Entries); i, j = i+1, j+1 {
		if i < len(rf.log) {
			// Overwrite conflicting logs
			rf.log[i] = args.Entries[j]
		} else {
			// Append new entries
			rf.log = append(rf.log, args.Entries[j])
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// From the paper figure 2: If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)

		// Commit new entries commited by leader
		for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
			rf.commitLogEntry(i)
		}
	}

	reply.Success = true
}
