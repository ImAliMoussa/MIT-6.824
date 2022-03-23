package kvraft

import (
	"log"
	"time"
)

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	currTerm, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.Trace("received get request", "\nArgs:", PP(args))

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
		Term: currTerm,
	}

	start := time.Now()
	value, completed := kv.WaitAndGet(op)
	log.Println("Wait and get took", time.Since(start))

	start = time.Now()
	newTerm, isLeader := kv.rf.GetState()
	log.Println("Wait and get took", time.Since(start))

	if !completed || !isLeader || currTerm != newTerm {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	reply.Value = value
}
