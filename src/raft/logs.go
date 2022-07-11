package raft

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

func (rf *Raft) LastLogIndex() int {
	if len(rf.logs) > 0 {
		return len(rf.logs) - 1
	}
	return 0
}

func (rf *Raft) LastLogTerm() int {
	if len(rf.logs) > 0 {
		return rf.logs[len(rf.logs)-1].Term
	}
	return 0
}
