package kvraft

import "time"

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
	for {
		reply := GetReply{}
		select {
		case <-ck.GetRPC(server, &args, &reply):
			if reply.Err == OK {
				ck.lastServer = server
				return reply.Value
			}
		case <-time.After(ClerkTimeout):
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) GetRPC(server int, args *GetArgs, reply *GetReply) chan bool {
	ch := make(chan bool, 1)
	go func() {
		ck.servers[server].Call("KVServer.Get", args, reply)
		ch <- true
	}()
	return ch
}
