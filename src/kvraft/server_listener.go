package kvraft

import (
	"fmt"
)

func (kv *KVServer) listener() {
	for !kv.killed() {
		command := <-kv.applyCh
		kv.Trace(PP(command))
		kv.Trace("KeyValueDict", PP(kv.keyValueDict))

		if command.SnapshotValid {
			kv.Trace("made snapshot for index", command.SnapshotIndex)
			ok := kv.rf.CondInstallSnapshot(command.SnapshotTerm, command.SnapshotIndex, command.Snapshot)
			if ok {
				kv.readSnapshot(command.Snapshot)
			}
			continue
		}

		if command.Command == nil {
			continue
		}

		op, ok := command.Command.(Op)

		if !ok {
			e := fmt.Sprintln("Error casting to op", PP(command))
			panic(e)
		}

		kv.Trace("listener received command", PP(op))
		kv.MarkAsComplete(op, command.CommandIndex)
	}
}
