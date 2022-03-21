package kvraft

import (
	"time"
)

func (kv *KVServer) IsDone(id int64) (string, bool) {
	kv.Trace("Requesting lock")
	kv.mu.Lock()
	kv.Trace("Received lock")
	defer kv.mu.Unlock()

	value, exists := kv.completedOps[id]
	return value, exists
}

func (kv *KVServer) WaitAndGet(op Op) (string, bool) {
	id := op.Id

	kv.mu.Lock()
	kv.Trace("Operation:", PP(op), "\nCurrent term:", kv.lastLeaderTerm)
	if _, exists := kv.channelMap[id]; !exists {
		kv.channelMap[id] = make(chan interface{}, 10)
		kv.Trace("Started op", op)
		go kv.rf.Start(op)
	} else if op.Term > kv.lastLeaderTerm {
		go kv.rf.Start(Op{
			Type: NO_OP,
		})
		kv.lastLeaderTerm = op.Term
	}

	channel := kv.channelMap[id]
	kv.mu.Unlock()
	// blocking receive
	select {
	case <-channel:
	case <-time.After(ClerkTimeout):
	}

	kv.Trace("Requesting lock")
	kv.mu.Lock()
	kv.Trace("Received lock")
	defer kv.mu.Unlock()

	value, exists := kv.completedOps[id]
	return value, exists
}

func (kv *KVServer) MarkAsComplete(operation Op) {
	kv.Trace("Requesting lock")
	kv.mu.Lock()
	kv.Trace("Received lock")
	defer kv.mu.Unlock()
	if operation.Term > kv.lastLeaderTerm {
		kv.lastLeaderTerm = operation.Term
	}

	if operation.Type == NO_OP {
		return
	}

	if _, alreadyFinished := kv.completedOps[operation.Id]; alreadyFinished {
		return
	}
	kv.execute(operation)
	kv.completedOps[operation.Id] = kv.keyValueDict[operation.Key]
	if ch, channelExists := kv.channelMap[operation.Id]; channelExists {
		close(ch)
		// delete(kv.channelMap, operation.Id)
	}
}

func (kv *KVServer) execute(operation Op) {
	if operation.Type == GET || operation.Type == NO_OP {
		// do nothing
	} else if operation.Type == PUT {
		kv.keyValueDict[operation.Key] = operation.Value
	} else if operation.Type == APPEND {
		kv.keyValueDict[operation.Key] += operation.Value
	} else {
		panic("Wrong operation type")
	}
}
