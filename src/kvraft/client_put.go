package kvraft

import "time"

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
}

type PutAppendReply struct {
	Err Err
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    nrand(),
	}
	server := ck.lastServer
	for {
		reply := PutAppendReply{}
		select {
		case <-ck.PutAppendRPC(server, &args, &reply):
			if reply.Err == OK {
				ck.lastServer = server
				return
			}
		case <-time.After(ClerkTimeout):
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) PutAppendRPC(server int, args *PutAppendArgs, reply *PutAppendReply) chan bool {
	ch := make(chan bool, 1)
	go func() {
		ck.servers[server].Call("KVServer.PutAppend", args, reply)
		ch <- true
	}()
	return ch
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
