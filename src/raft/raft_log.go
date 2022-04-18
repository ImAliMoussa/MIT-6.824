package raft

func (rf *Raft) getLog(i int) LogEntry {
	trace("Server", rf.me, "getting log", i, "with base index", rf.baseIndex)
	return rf.log[i-rf.baseIndex]
}

func (rf *Raft) setLog(i int, entry LogEntry) {
	rf.log[i-rf.baseIndex] = entry
}

func (rf *Raft) logLength() int {
	return rf.baseIndex + len(rf.log)
}

//
// similar to rf.log[start:finish]
//
func (rf *Raft) sliceLog(start, finish int) []LogEntry {
	start = start - rf.baseIndex
	finish = finish - rf.baseIndex
	return rf.log[start:finish]
}
