package raft

import "sync"

func (rf *Raft) getElectionArgs() *RequestVoteArgs {
	trace("Candidate", rf.me, "trying to acquire")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	trace("Candidate", rf.me, "received lock in startElection")

	rf.currentTerm++
	trace("Candidate", rf.me, "has incremented currentTerm")
	rf.votedFor = rf.me

	lengthOfLog := len(rf.log)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lengthOfLog - 1,
		LastLogTerm:  rf.log[lengthOfLog-1].Term,
	}

	return args
}

func (rf *Raft) startElection() {
	args := rf.getElectionArgs()
	currentTerm := args.Term

	votes := 1
	var votesMu sync.Mutex

	for i := 0; i < rf.numPeers; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			trace("Candidate", rf.me, "sent a request vote to", i)
			reply := &RequestVoteReply{}
			rf.sendRequestVote(i, args, reply)

			trace("Candidate", rf.me, "has received a response from server", i, "\nResponse:", reply)

			votesMu.Lock()
			defer votesMu.Unlock()

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.VoteGranted {
				votes++
				if votes == 1+(rf.numPeers/2) {
					trace("Leader win, server is", rf.me)
					rf.wonElectonCh <- true
				}
			} else if currentTerm < reply.Term {
				state, err := rf.follow(reply.Term, -1)
				// rf.mu.Unlock()
				if err == nil && (state == CANDIDATE) {
					rf.stepDownCh <- true
				}
				return
			}
			// rf.mu.Unlock()

			// If conditions is votes > rf.numPeers the channel would receive several times
		}(i)
	}

	trace("Server", rf.me, "exiting startElection")
}
