package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, Non)
		rf.voteFor = Non
	}

	if rf.currentTerm == args.Term && rf.voteFor != Non && rf.voteFor != args.CandidateId {
		return
	}

	// Find the new one :)
	if args.LastLogTerm < rf.LastLogTerm() || (args.LastLogTerm == rf.LastLogTerm() && args.LastLogIndex < rf.logs[len(rf.logs)-1].Index) {
		return
	}

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	rf.electionTimer.Reset(randElectionTime())
	rf.voteFor = args.CandidateId
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) doElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("do Election rf.me %d rf.term %d", rf.me, rf.currentTerm)

	rf.becomeCandidate()
	rf.increaseTerm()
	rf.persist()

	voteCount := 1
	rf.voteFor = rf.me
	term := rf.currentTerm

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(index int) {
				reply := new(RequestVoteReply)
				rf.mu.Lock()

				if rf.currentTerm != term || rf.state != Candidate {
					rf.mu.Unlock()
					return
				}

				args := new(RequestVoteArgs)
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = rf.logs[len(rf.logs)-1].Index
				args.LastLogTerm = rf.LastLogTerm()
				rf.mu.Unlock()

				ok := rf.sendRequestVote(index, args, reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}

				if reply.VoteGranted {
					voteCount++
					if voteCount > len(rf.peers)/2 {
						rf.becomeLeader()
						rf.persist()
					}
				}

				if !reply.VoteGranted && reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term, Non)
					rf.voteFor = Non
					rf.persist()
				}

			}(i)
		}
	}

}
