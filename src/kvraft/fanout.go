package kvraft

import "time"

func (kv *KVServer) IsDone(id int64) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.completedOps[id]
	return value, exists
}

func (kv *KVServer) WaitAndGet(op Op) (string, bool) {
	id := op.Id

	kv.mu.Lock()
	if _, exists := kv.channelMap[id]; !exists {
		kv.channelMap[id] = make(chan interface{}, 10)
		go kv.rf.Start(op)
	}

	channel := kv.channelMap[id]
	kv.mu.Unlock()
	// blocking receive
	select {
	case <-channel:
	case <-time.After(ClerkTimeout):
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exists := kv.completedOps[id]
	return value, exists
}

func (kv *KVServer) MarkAsComplete(operation Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
	if operation.Type == GET {
		// do nothing
	} else if operation.Type == PUT {
		kv.keyValueDict[operation.Key] = operation.Value
	} else if operation.Type == APPEND {
		kv.keyValueDict[operation.Key] += operation.Value
	} else {
		panic("Wrong operation type")
	}
}
