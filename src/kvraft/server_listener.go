package kvraft

import (
	"fmt"
)

func (kv *KVServer) listener() {
	for !kv.killed() {
		command := <-kv.applyCh

		op, ok := command.Command.(Op)

		if !ok {
			e := fmt.Sprintln("Error casting to op")
			panic(e)
		}

		kv.Trace("listener received command", PP(op))
		kv.MarkAsComplete(op, command.CommandIndex)
	}
}
