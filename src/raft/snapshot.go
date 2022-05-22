package raft

type InstallSnapshotReq struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotRes struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotReq, reply *InstallSnapshotRes) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.baseIndex {
		rf.mu.Unlock()
		return
	}

	if rf.state == FOLLOWER {
		rf.heartbeatCh <- true
	}

	trace("Server", rf.me, "just received install snapshot with args:", args.String(),
		"\nLogs:", rf.log,
		"\nCommit index:", rf.commitIndex,
	)

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}

	state := rf.follow(args.Term)
	if state == LEADER || state == CANDIDATE {
		rf.stepDownCh <- true
	}

	rf.Persist()
	trace("Server", rf.me, "has persisted state successfully")
	rf.mu.Unlock()

	command := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- command
}

func (rf *Raft) getInstallSnapshotsArgs() *InstallSnapshotReq {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return &InstallSnapshotReq{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.baseIndex,
		LastIncludedTerm:  rf.getLog(rf.baseIndex).Term,
		Data:              rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) sendInstallSnapshot(server int) {
	args := rf.getInstallSnapshotsArgs()
	reply := &InstallSnapshotRes{}

	ok := rf.sendInstallSnapshotRPC(server, args, reply)
	if !ok {
		// RPC failed
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		state := rf.follow(reply.Term)
		rf.votedFor = -1
		if state == LEADER || state == CANDIDATE {
			rf.stepDownCh <- true
		}
		rf.Persist()
		return
	}

	// InstallSnapshot succeeded

	// index of highest log entry known to be replicated on server is rf.baseIndex
	rf.updateIndexes(server, rf.baseIndex)
	trace("Server", rf.me, "sent an install snapshot to server", server,
		"\nArgs:", args.String(),
		"\nReply:", reply.String(),
		"\nNext index:", rf.nextIndex[server],
		"\nMatch index:", rf.matchIndex[server],
	)
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotReq, reply *InstallSnapshotRes) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}
