package kvraft

import (
	"fmt"
	"time"
)

func (kv *KVServer) getSession(clientId int) *ClientSession {
	session, exists := kv.sessions[clientId]
	if exists {
		return session
	}
	kv.Trace("Creating new session", clientId)
	kv.sessions[clientId] = &ClientSession{}
	return kv.sessions[clientId]
}

func (kv *KVServer) isDoneNoLock(id int64, clientId int) (string, bool) {
	clientSession := kv.getSession(clientId)

	kv.Trace("cliensession", PP(clientSession), "id", id)

	kv.Trace("clientsession.lastcompleted", clientSession.LastCompleted, "id", id)
	if clientSession.LastCompleted >= id {
		return clientSession.LastCompletedResult, true
	}

	return "", false
}

func (kv *KVServer) IsDone(id int64, clientId int) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.isDoneNoLock(id, clientId)
}

func (kv *KVServer) WaitAndGet(op Op, clientId int) (string, bool) {
	id := op.Id

	kv.mu.Lock()
	kv.Trace("Operation:", PP(op), "\nCurrent term:", kv.lastLeaderTerm)
	_, exists := kv.channelMap[clientId][id]
	kv.Trace("Operation:", PP(op), "exists:", exists)
	if !exists {
		index, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			kv.mu.Unlock()
			return "", false
		}
		if _, init := kv.channelMap[clientId]; !init {
			kv.channelMap[clientId] = make(map[int64]chan interface{})
		}
		kv.channelMap[clientId][id] = make(chan interface{}, 10)
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

	channel := kv.channelMap[clientId][id]
	kv.mu.Unlock()
	// blocking receive
	select {
	case <-channel:
	case <-time.After(ClerkTimeout):
	}

	return kv.IsDone(id, clientId)
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
	kv.Trace("size and max", kv.rf.Persistor().RaftStateSize(), kv.maxraftstate)
	if kv.maxraftstate != -1 && kv.rf.Persistor().RaftStateSize() > kv.maxraftstate {
		go kv.snapshotData(index)
	}

	if operation.Type == NO_OP {
		return
	}

	if _, alreadyFinished := kv.isDoneNoLock(operation.Id, operation.Client); alreadyFinished {
		return
	}
	kv.Trace("executing command", PP(operation))
	kv.execute(operation)

	session := kv.getSession(operation.Client)
	if session.LastCompleted+1 != operation.Id {
		e := fmt.Sprintf("kv(%d) a7a neek %s %s\n", kv.me, PP(operation), PP(session))
		panic(e)
	}
	session.LastCompleted = operation.Id
	session.LastCompletedResult = kv.keyValueDict[operation.Key]

	if ch, channelExists := kv.channelMap[operation.Client][operation.Id]; channelExists {
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
