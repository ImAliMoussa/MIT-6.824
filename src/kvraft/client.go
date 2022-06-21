package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)

var clients int32 = 0

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastServer int
	clientId   int
	req        int64
}

func (ck *Clerk) getId() int64 {
	// return nrand()
	ck.req++
	return ck.req
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	clients++
	ck := &Clerk{
		servers:    servers,
		lastServer: 0,
		clientId:   int(atomic.AddInt32(&clients, 1)),
		req:        0,
	}
	return ck
}
