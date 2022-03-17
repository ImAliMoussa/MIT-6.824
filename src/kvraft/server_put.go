package kvraft

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

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
	}

	_, completed := kv.WaitAndGet(op)
	if !completed {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}
