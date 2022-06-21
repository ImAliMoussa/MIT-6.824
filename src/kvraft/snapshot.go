package kvraft

import (
	"bytes"
	"fmt"

	"6.824/labgob"
)

func (kv *KVServer) snapshotData(index int) {
	kv.Trace("made snapshot for index", index)
	buffer := new(bytes.Buffer)

	e := labgob.NewEncoder(buffer)

	kv.mu.Lock()
	kvDict := kv.keyValueDict
	// sessions := kv.sessions
	sessions := make(map[int]ClientSession, 0)
	for key, val := range kv.sessions {
		sessions[key] = *val
		kv.Trace("snapshotting saving", PP(key), PP(val.LastCompleted))
	}

	kv.Trace("Saving data", PP(kvDict), PP(sessions))

	e.Encode(kvDict)
	e.Encode(sessions)
	kv.mu.Unlock()

	data := buffer.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvDict map[string]string
	var sessions map[int]ClientSession

	if d.Decode(&kvDict) != nil ||
		d.Decode(&sessions) != nil {
		kv.Trace("Decode error in readSnapshot")
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.keyValueDict = kvDict
		kv.sessions = make(map[int]*ClientSession)
		for key, val := range sessions {
			kv.sessions[key] = &ClientSession{
				LastCompleted:       val.LastCompleted,
				LastCompletedResult: val.LastCompletedResult,
			}
			kv.Trace("snapshotting restoring", PP(key), PP(kv.sessions[key].LastCompleted))
		}

		for key, val := range sessions {
			if val.LastCompleted != kv.sessions[key].LastCompleted {
				e := fmt.Sprintf("kv %d %d %d\n", key, val.LastCompleted, kv.sessions[key].LastCompleted)
				panic(e)
			}
		}
	}
}
