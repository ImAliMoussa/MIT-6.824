package kvraft

import (
	"6.824/labgob"
	"bytes"
)

func (kv *KVServer) snapshotData(index int) {
	kv.Trace("made snapshot for index", index)
	buffer := new(bytes.Buffer)

	e := labgob.NewEncoder(buffer)

	kv.mu.Lock()
	kvDict := kv.keyValueDict
	ops := kv.completedOps

	e.Encode(kvDict)
	e.Encode(ops)
	kv.mu.Unlock()

	data := buffer.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvDict map[string]string
	var ops map[int64]string

	if d.Decode(&kvDict) != nil ||
		d.Decode(&ops) != nil {
		// panic("Decode error in readSnapshot")
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.keyValueDict = kvDict
		kv.completedOps = ops
		kv.Trace("Just woke up", "Dict: ", PP(kv.keyValueDict))
	}
}
