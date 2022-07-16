package raft

import (
	"sort"
)

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term      int
	Success   bool
	NextIndex int

	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.becomeFollower(args.Term, args.LeaderId)

	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term, args.LeaderId)
		rf.voteFor = Non
		rf.persist()
	}

	if args.PrevLogIndex > rf.LastLogIndex() {
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = Non
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		index := args.PrevLogIndex
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		for ; index > 0 && rf.logs[index].Term == rf.logs[args.PrevLogIndex].Term; index-- {
		}

		reply.ConflictIndex = index + 1
		DPrintf("rf.me %d rf.ConflictIndex %d", rf.me, reply.ConflictIndex)
		return
	}

	reply.Success = true

	index := args.PrevLogIndex
	for i, entry := range args.Entries {
		index++
		if index < len(rf.logs) {
			if rf.logs[index].Term == entry.Term {
				continue
			}

			// Clean the old logs
			rf.logs = rf.logs[:index]
		}

		rf.logs = append(rf.logs, args.Entries[i:]...)
		rf.persist()
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)-1)
	}

	reply.NextIndex = len(rf.logs)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) serverAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("sendAppendEntries rf.me %d rf.term %d", rf.me, rf.currentTerm)

	if rf.state != Leader {
		return
	}

	rf.heartsbeatTimer.Reset(randHeartsBeatTime())

	term := rf.currentTerm

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(index int) {
				rf.mu.Lock()

				if rf.currentTerm != term || rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				args := new(AppendEntryArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex

				nextIndex := rf.nextIndex[index]

				if nextIndex > len(rf.logs)-1 {
					args.PrevLogTerm = rf.LastLogTerm()
					args.PrevLogIndex = rf.LastLogIndex()
				} else {
					args.PrevLogIndex = nextIndex - 1
					args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
					args.Entries = append([]Entry{}, rf.logs[rf.nextIndex[index]:]...)
				}
				DPrintf("send to %d args.PrecLogIndex %d args.Entries %v", index, args.PrevLogIndex, args.Entries)

				rf.mu.Unlock()

				reply := new(AppendEntryReply)
				ok := rf.sendAppendEntries(index, args, reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Old rpc
				if rf.currentTerm != args.Term || rf.state != Leader {
					return
				}

				if reply.Success {

					if len(args.Entries) > 0 {
						rf.nextIndex[index] = reply.NextIndex
						rf.matchIndex[index] = rf.nextIndex[index] - 1

						copy := make([]int, len(rf.matchIndex)-1)

						p := 0
						for i := 0; i < len(rf.matchIndex); i++ {
							if i != rf.me {
								copy[p] = rf.matchIndex[i]
								p++
							}
						}

						sort.Ints(copy)

						N := copy[len(rf.matchIndex)/2]

						if N > rf.commitIndex && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
							rf.commitIndex = N
						}
					}

				}

				if !reply.Success {
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term, Non)
						rf.voteFor = Non
						rf.heartsbeatTimer.Stop()
						rf.persist()
					} else {

						// RPC erro
						if reply.ConflictIndex == -1 {
							return
						}

						// Index
						rf.nextIndex[index] = reply.ConflictIndex

						if rf.nextIndex[index] == 0 {
							// Zero is an empty
							rf.nextIndex[index] = 1
						}
					}
				}

			}(i)
		}
	}

}
