package kvraft

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	currTerm, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.Trace("received get request", "\nArgs:", PP(args))

	value, exists := kv.IsDone(args.Id, args.ClientId)
	if exists {
		reply.Err = OK
		reply.Value = value
		return
	}

	op := Op{
		Type:   GET,
		Id:     args.Id,
		Key:    args.Key,
		Term:   currTerm,
		Client: args.ClientId,
	}

	value, completed := kv.WaitAndGet(op, args.ClientId)
	newTerm, isLeader := kv.rf.GetState()

	if !completed || !isLeader || currTerm != newTerm {
		reply.Err = ErrWrongLeader
		return
	}

	kv.Trace("returning get request of id", args.Id, "with value", value)

	reply.Err = OK
	reply.Value = value
}
