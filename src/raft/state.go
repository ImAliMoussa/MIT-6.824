package raft

import "errors"

func (rf *Raft) follow(newTerm, votedFor int) (RaftState, error) {
	if newTerm < rf.currentTerm {
		return "", errors.New("old message")
	}

	state := rf.state
	if state != FOLLOWER {
		rf.state = FOLLOWER
		trace("Server", rf.me, "has stepped down and voted for", rf.votedFor)
	}

	if newTerm > rf.currentTerm {
		trace("Server", rf.me, "has changed currentTerm to", rf.currentTerm)
		rf.currentTerm = newTerm
		rf.votedFor = votedFor
	}

	return state, nil
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = CANDIDATE
}

//
// Server has become a leader so it updates state and
// sends a heartbeat as specified by paper to let other
// server know that there is a new leader.
//
func (rf *Raft) lead() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	trace("Server", rf.me, "is now a leader for term", rf.currentTerm)

	rf.state = LEADER
	rf.initializeVolatileState()
}
