package raft

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
	for i := 0; i < rf.numPeers; i++ {
		if i == rf.me {
			continue
		}

		trace("Candidate", rf.me, "sent a request vote to", i)
		reply := &RequestVoteReply{}
		rf.sendRequestVote(i, args, reply)
		trace("Candidate", rf.me, "has received a response from server", i, "\nResponse:", reply)

		if reply.VoteGranted {
			votes++
		} else if currentTerm < reply.Term {
			rf.followAndNotify(reply.Term, -1)
			return
		}
	}

	if votes > rf.numPeers/2 {
		rf.wonElectonCh <- true
	}

	trace("Server", rf.me, "exiting startElection")
}
