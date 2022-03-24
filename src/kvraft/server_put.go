package kvraft

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	currTerm, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.Trace("received put request", "\nArgs:", PP(args))

	_, exists := kv.IsDone(args.Id)
	if exists {
		reply.Err = OK
		return
	}

	op := Op{
		Type:  args.Op,
		Id:    args.Id,
		Key:   args.Key,
		Value: args.Value,
		Term:  currTerm,
	}

	_, completed := kv.WaitAndGet(op)
	_, isLeader = kv.rf.GetState()

	if !completed || !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}
