package raft

import (
	"encoding/json"
)

// lol why do I have to implement min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (a AppendEntriesRequest) String() string {
	out, _ := json.Marshal(a)
	return string(out)
}

func (a AppendEntriesResponse) String() string {
	out, _ := json.Marshal(a)
	return string(out)
}
