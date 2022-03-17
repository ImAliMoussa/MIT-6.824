package kvraft

// import "time"

type GetArgs struct {
	Key string
	Id  int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		Id:  nrand(),
	}
	server := ck.lastServer
	ck.Trace("started new operation\nArgs:", PP(args))
	for {
		reply := GetReply{}

		ck.Trace("sending get request to server", server, "\nArgs:", PP(args))
		ck.servers[server].Call("KVServer.Get", &args, &reply)
		ck.Trace(
			"received put append request to server", server,
			"\nArgs:", PP(args),
			"\nReply:", PP(reply),
		)

		if reply.Err == OK {
			ck.lastServer = server
			ck.Trace("received new operation\nArgs:", PP(args), "\nReply:", PP(reply))
			return reply.Value
		}
		server = (server + 1) % len(ck.servers)
	}
}
