package kvraft

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	currTerm, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.Trace("received put request", "\nArgs:", PP(args))

	_, exists := kv.IsDone(args.Id, args.ClientId)
	if exists {
		reply.Err = OK
		return
	}

	op := Op{
		Type:   args.Op,
		Id:     args.Id,
		Key:    args.Key,
		Value:  args.Value,
		Term:   currTerm,
		Client: args.ClientId,
	}

	_, completed := kv.WaitAndGet(op, args.ClientId)
	_, isLeader = kv.rf.GetState()

	if !completed || !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.Trace("returning get request of id", args.Id)
	reply.Err = OK
}
