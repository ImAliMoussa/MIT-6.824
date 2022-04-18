package kvraft

// import "time"

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
	ck.Trace("started new operation with id, ", args.Id, ". \nArgs:", PP(args))
	server := ck.lastServer
	for {
		reply := PutAppendReply{}

		ck.Trace("sending put append request to server", server, "\nArgs:", PP(args))
		ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		ck.Trace(
			"received put append request to server", server, "with id", args.Id,
			"\nArgs:", PP(args),
			"\nReply:", PP(reply),
		)

		if reply.Err == OK {
			ck.Trace("received new operation with id:", args.Id,
				"\nArgs:", PP(args), "\nReply:", PP(reply))
			ck.lastServer = server
			return
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
