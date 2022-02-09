package raft

func (rf *Raft) followAndNotify(newTerm, votedFor int) RfState {
	trace("Server", rf.me, "is trying to acquire lock")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	trace("Server", rf.me, "has acquired the lock")

	if newTerm < rf.currentTerm {
		trace("Server", rf.me, "term:", rf.currentTerm, "newTerm:", newTerm, "votedFor:", votedFor)
		panic("Don't step down for a lower term")
	}

	state := rf.state
	if state != FOLLOWER {
		trace("Server", rf.me, "is trying to trying to step down")
		rf.state = FOLLOWER
		rf.votedFor = votedFor
		rf.stepDownCh <- true
		trace("Server", rf.me, "has stepped down")
	}

	if newTerm > rf.currentTerm {
		trace("Server", rf.me, "has changed currentTerm")
		rf.currentTerm = newTerm
	}

	return state
}

func (rf *Raft) stepUpToCandidate() {
	trace("Server", rf.me, "is trying to acquire lock")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	trace("Server", rf.me, "has acquired the lock")

	rf.state = CANDIDATE
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
	defer rf.mu.Unlock()

	trace("Leader", rf.me, "has acquired the lock")
	rf.state = LEADER
	rf.initializeVolatileState()
}
