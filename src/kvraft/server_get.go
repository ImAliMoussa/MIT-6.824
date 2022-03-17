package kvraft

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	value, exists := kv.IsDone(args.Id)
	if exists {
		reply.Err = OK
		reply.Value = value
		return
	}

	op := Op{
		Type: GET,
		Id:   args.Id,
		Key:  args.Key,
	}

	value, completed := kv.WaitAndGet(op)
	if !completed {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	reply.Value = value
}
