package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Reply false if term < currentTerm (¬ß5.1)
	// 2. If votedFor is null or candidateId, and candidate‚Äôs log is at
	// least as up-to-date as receiver‚Äôs log, grant vote (¬ß5.2, ¬ß5.4)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		state := rf.follow(args.Term)
		rf.votedFor = -1
		if state == FOLLOWER {
			rf.heartbeatCh <- true
		}
	}

	isUpToDate := rf.isUpToDate(args)
	trace("Server", rf.me, "votedFor:", rf.votedFor, "is up to date:", isUpToDate)

	if (rf.votedFor == -1 || rf.votedFor == rf.me) && rf.isUpToDate(args) {
		reply.VoteGranted = true
		// reset follower timer when follower grants a vote
		rf.follow(args.Term)
		rf.votedFor = args.CandidateId
	}

	rf.Persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	trace("Server", rf.me, "sent request vote to server", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
