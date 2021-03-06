package kvraft

import (
	"log"

	"github.com/sasha-s/go-deadlock"

	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type   string
	Id     int64
	Client int
	Key    string
	Value  string
	Term   int
}

type ClientSession struct {
	LastCompleted       int64
	LastCompletedResult string
}

type KVServer struct {
	// mu      sync.Mutex
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	lastLeaderTerm int
	keyValueDict   map[string]string
	sessions       map[int]*ClientSession
	channelMap     map[int]map[int64]chan interface{}
	commitIndex    int
	commandIndex   map[int64]int
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})

	N := 1000
	applyCh := make(chan raft.ApplyMsg, N)

	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      applyCh,
		rf:           raft.Make(servers, me, persister, applyCh),
		keyValueDict: make(map[string]string),
		sessions:     make(map[int]*ClientSession),
		channelMap:   make(map[int]map[int64]chan interface{}),
		commandIndex: make(map[int64]int),
	}

	data := kv.rf.Persistor().ReadSnapshot()
	kv.readSnapshot(data)

	if len(kv.keyValueDict) > 0 {
		kv.rf.ResetLastApplied()
	}

	kv.Trace("Started server with key value dict", PP(kv.keyValueDict))

	go kv.listener()

	return kv
}
