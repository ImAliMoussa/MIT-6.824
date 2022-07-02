package raft

import (
	"bytes"
	"fmt"

	"6.824/labgob"
)

func (rf *Raft) getRaftState() []byte {
	buffer := new(bytes.Buffer)

	e := labgob.NewEncoder(buffer)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.baseIndex)
	e.Encode(rf.log)

	data := buffer.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) Persist() {
	data := rf.getRaftState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) Persistor() *Persister {
	return rf.persister
}

//
// restore previously persisted state.
//
func (rf *Raft) ReadPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var baseIndex int

	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&baseIndex) != nil ||
		d.Decode(&log) != nil {
		panic("Decode error in readPersist")
	} else {
		trace(currentTerm, votedFor, log)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.baseIndex = baseIndex
		rf.lastApplied = baseIndex
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex < rf.baseIndex || rf.lastApplied > lastIncludedIndex {
		return false
	}

	if lastIncludedIndex < rf.logLength() && rf.getLog(lastIncludedIndex).Term == lastIncludedTerm {
		// TODO figure out if I need to delete what's after lastIncludedIndex in case that
		// rf.getLog(lastIncludedIndex).term != lastIncludedTerm
		rf.log = rf.sliceLog(lastIncludedIndex, rf.logLength())
	} else {
		rf.log = make([]LogEntry, 1)
	}
	rf.log[0].Term = lastIncludedTerm
	rf.baseIndex = lastIncludedIndex
	rf.lastApplied = rf.baseIndex
	rf.commitIndex = max(rf.commitIndex, rf.baseIndex)

	raftState := rf.getRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)

	trace(
		"Server", rf.me,
		"\nBase index:", rf.baseIndex,
		"\nLogs:", rf.log,
		"\nLog length:", rf.logLength(),
		"\nLast applied", rf.lastApplied,
	)

	return true
}

//
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//
func (rf *Raft) Snapshot(baseIndex int, snapshot []byte) {
	// Your code here (2D).
	trace("Server", rf.me, "is trying to acquire the lock in Snapshot")
	rf.mu.Lock()
	trace("Server", rf.me, "has acquired the lock in Snapshot")
	defer rf.mu.Unlock()

	if baseIndex <= rf.baseIndex || baseIndex >= rf.logLength() {
		errMsg := fmt.Sprintln("Out of range index:", baseIndex, "base index is:", rf.baseIndex, "log length", rf.logLength())
		// panic(errMsg)
		trace("Server", rf.me, errMsg)
		return
	}

	rf.log = rf.sliceLog(baseIndex, rf.logLength())
	rf.baseIndex = baseIndex
	rf.lastApplied = baseIndex
	rf.commitIndex = max(rf.commitIndex, rf.baseIndex)

	raftState := rf.getRaftState()

	rf.persister.SaveStateAndSnapshot(raftState, snapshot)

	trace("Server", rf.me, "is releasing the lock")
}
