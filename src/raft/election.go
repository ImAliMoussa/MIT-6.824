package raft

func (rf *Raft) getElectionArgs() *RequestVoteArgs {
	rf.currentTerm++
	rf.votedFor = rf.me
	trace("Server", rf.me, "has with new currentTerm", rf.currentTerm, "in start election and voted for self")

	rf.Persist()

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	trace("Server", rf.me, "has started an election")

	args := rf.getElectionArgs()
	currentTerm := args.Term

	votes := 1

	for i := 0; i < rf.numPeers && !rf.killed(); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)

			trace("Server", rf.me, "has received a response from server", server,
				"\nArgs:", args,
				"\nResponse:", reply,
			)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			//
			// Check state hasn't changed since sending the RPC
			//
			if rf.currentTerm != args.Term || rf.state != CANDIDATE || rf.killed() {
				return
			}

			if reply.VoteGranted {
				votes++
				// If conditions is votes > rf.numPeers the channel would receive several times
				if votes == 1+(rf.numPeers/2) {
					trace("Leader win, new leader is:", rf.me)
					rf.wonElectonCh <- true
				}
			} else if currentTerm < reply.Term {
				state, err := rf.follow(reply.Term, -1)
				rf.Persist()
				if err == nil && (state == CANDIDATE) {
					rf.stepDownCh <- true
				}
				return
			}
		}(i)
	}
}
