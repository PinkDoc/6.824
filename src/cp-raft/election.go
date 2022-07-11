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
	//Index       int // Index of this raft endpoint
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	//reply.Index = rf.me

	defer DPrintf("RequestVote rf.me %v reply %v", rf.me, reply)

	if args.Term < rf.currentTerm || (rf.currentTerm == args.Term && rf.voteFor != Non && rf.voteFor != args.CandidateId) {
		return
	}

	// Find the new one :)
	if args.LastLogTerm < rf.LastLogTerm() || (args.LastLogTerm == rf.LastLogTerm() && args.LastLogIndex < rf.LastLogIndex()) {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, Non)
		rf.voteFor = Non
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

	rf.becomeCandidate()
	rf.increaseTerm()
	rf.persist()

	voteCount := 1

	DPrintf("doElection rf.me %v rf.term %v", rf.me, rf.currentTerm)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(index int) {
				reply := new(RequestVoteReply)
				//reply.Index = index

				rf.mu.Lock()
				args := new(RequestVoteArgs)
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = rf.LastLogIndex()
				args.LastLogTerm = rf.LastLogTerm()
				rf.mu.Unlock()

				ok := rf.sendRequestVote(index, args, reply)

				if !ok {
					DPrintf("sendRequestVote networkingfail!")
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}

				if reply.VoteGranted {
					voteCount++
					DPrintf("doElecton rf.me %v rf.currentTerm %v message voteCount %v", rf.me, rf.currentTerm, voteCount)
					if voteCount > len(rf.peers)/2 {
						DPrintf("doElecton rf.me %v rf.currentTerm %v message Become Leader", rf.me, rf.currentTerm)
						rf.becomeLeader()
						rf.persist()
					}
				}

				if !reply.VoteGranted && reply.Term > rf.currentTerm {
					DPrintf("doElection find a higher term, become follower")
					rf.becomeFollower(reply.Term, Non)
					rf.persist()
				}

			}(i)
		}
	}

}
