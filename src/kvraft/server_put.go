package kvraft

import (
	"log"
	"time"
)

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

	start := time.Now()
	_, completed := kv.WaitAndGet(op)
	log.Println("Wait and get in put took", time.Since(start))

	start = time.Now()
	_, isLeader = kv.rf.GetState()
	log.Println("Get state in put took", time.Since(start))

	if !completed || !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}
