package kvraft

import (
	"time"
)

func (kv *KVServer) IsDone(id int64) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exists := kv.completedOps[id]
	return value, exists
}

func (kv *KVServer) WaitAndGet(op Op) (string, bool) {
	id := op.Id

	kv.mu.Lock()
	kv.Trace("Operation:", PP(op), "\nCurrent term:", kv.lastLeaderTerm)
	_, exists := kv.channelMap[id]
	kv.Trace("Operation:", PP(op), "exists:", exists)
	if !exists {
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			kv.mu.Unlock()
			return "", false
		}
		kv.channelMap[id] = make(chan interface{}, 10)
		kv.commandIndex[id] = index
		kv.Trace("Started op", op)
	} else if op.Term > kv.lastLeaderTerm {
		// Operation was submmitted to an old leader which lost leadership or was part of a minority.
		// So we should send the operation again to the new leader.
		kv.rf.Start(Op{
			Type: NO_OP,
		})
		kv.lastLeaderTerm = op.Term
	}

	if kv.commitIndex > kv.commandIndex[id] {
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			kv.mu.Unlock()
			return "", false
		}
		kv.commandIndex[id] = index
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

func (kv *KVServer) MarkAsComplete(operation Op, index int) {
	kv.Trace("Requesting lock")
	kv.mu.Lock()
	kv.Trace("Received lock")
	defer kv.mu.Unlock()
	if operation.Term > kv.lastLeaderTerm {
		kv.lastLeaderTerm = operation.Term
	}

	if index > kv.commitIndex {
		kv.commitIndex = index
	}

	if kv.maxraftstate != -1 && kv.rf.Persistor().RaftStateSize() > kv.maxraftstate {
		go kv.snapshotData(index)
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
	kv.Trace("executing command", PP(operation))
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
