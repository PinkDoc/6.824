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
		DPrintf("AppendEntries  rf.me %v args.Term %v < rf.currentTerm %v", rf.me, args.Term, rf.currentTerm)
		return
	}

	rf.becomeFollower(args.Term, args.LeaderId)
	rf.persist()

	// Student guide
	//If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
	argsLastIndex := args.PrevLogIndex + len(args.Entries)

	if argsLastIndex < rf.LastLogIndex() {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = -1
		return
	}

	if args.PrevLogIndex > rf.LastLogIndex() {
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = Non
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		index := 0
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		for ; index > 0 && rf.logs[index].Term == rf.logs[args.PrevLogIndex].Term; index-- {
		}

		index = index + 1

		reply.ConflictIndex = index
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
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)-1)
	}

	reply.NextIndex = len(rf.logs)
	rf.persist()

	DPrintf("AppendEntries rf.me %v len(rf.logs) %v rf.commitIndex %d", rf.me, len(rf.logs), rf.commitIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) doHeartsBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	DPrintf("doHeartsBeat rf.me %v rf.term %v", rf.me, rf.currentTerm)

	rf.heartsbeatTimer.Reset(randHeartsBeatTime())

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(index int) {
				rf.mu.Lock()
				args := new(AppendEntryArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex

				nextIndex := rf.nextIndex[index]

				if nextIndex > len(rf.logs)-1 {
					args.PrevLogTerm = rf.LastLogTerm()
					args.PrevLogIndex = len(rf.logs) - 1
				} else {
					args.PrevLogIndex = nextIndex - 1
					args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
					args.Entries = append([]Entry{}, rf.logs[rf.nextIndex[index]:]...)
				}

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

					// Not the first log
					if len(args.Entries) > 0 {
						rf.nextIndex[index] = reply.NextIndex
						rf.matchIndex[index] = rf.nextIndex[index] - 1

						// Update Commit index
						copy := make([]int, len(rf.matchIndex)-1)

						// Copy matchIndex
						p := 0
						for i := 0; i < len(rf.matchIndex); i++ {
							if i != rf.me {
								copy[p] = rf.matchIndex[i]
								p++
							}
						}

						sort.Ints(copy)

						N := copy[len(rf.matchIndex)/2]

						if N > rf.commitIndex {
							DPrintf("doHeartsBeat rf.me %d len(rf.logs) %v Update commitIndex to %d", rf.me, len(rf.logs), N)
							rf.commitIndex = N
						}
					}

				}

				if !reply.Success {
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term, Non)
						rf.heartsbeatTimer.Stop()
						rf.persist()
					} else {

						// RPC error
						if reply.ConflictIndex == -1 {
							return
						}

						// Index

						if reply.ConflictTerm == Non {
							rf.nextIndex[index] = reply.ConflictIndex
						} else {
							lastIndex := len(rf.logs) - 1
							for ; lastIndex > 0; lastIndex-- {
								if rf.logs[lastIndex].Term == reply.ConflictTerm {
									break
								}
							}

							if lastIndex == 0 {
								rf.nextIndex[index] = reply.ConflictIndex
							} else {
								rf.nextIndex[index] = lastIndex + 1
							}

						}

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
