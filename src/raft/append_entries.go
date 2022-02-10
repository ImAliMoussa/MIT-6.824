package raft

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

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := 0; server < rf.numPeers; server++ {
		if server != rf.me {
			go rf.sendAppendEntries(server, rf.currentTerm, rf.commitIndex)
		}
	}
}

// Issued by Leader to send AppendEntriesRPC to followers
func (rf *Raft) sendAppendEntries(server, term, commitIndex int) {
	rf.mu.Lock()
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
	rf.mu.Unlock()

	reply := AppendEntriesResponse{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	trace("Server", rf.me, "has sent an append entry to", server,
		"\nOk:", ok,
		"\nArgs:", args.String(),
		"\nReply:", reply.String())

	if reply.Term > term {
		rf.mu.Lock()
		state, err := rf.follow(reply.Term, -1)
		rf.mu.Unlock()
		if err == nil && (state == LEADER || state == CANDIDATE) {
			rf.stepDownCh <- true
		}
	} else if !reply.Success && ok {
		// ok signals that the rpc successfully reached the server and reply.Success
		// means the server rejected it
		rf.nextIndex[server]--
		trace("Leader", rf.me, "will retry sending append entry to", server)
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
	trace("Server", rf.me, "has acquired the lock")

	// Reply false if term < currentTerm (§5.1)
	oldTerm := args.Term < rf.currentTerm
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	differentTerms := (args.PrevLogIndex >= len(rf.log)) || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm)

	reply.Term = rf.currentTerm

	trace("Server", rf.me, "oldTerm", oldTerm, "differentTerms", differentTerms)
	if oldTerm || differentTerms {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	reply.Success = true

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
	rf.mu.Unlock()
}
