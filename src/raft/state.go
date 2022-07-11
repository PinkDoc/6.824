package raft

import (
	"math/rand"
	"time"
)

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
	Non       = -1
)

// 150-350 ms
func randElectionTime() time.Duration {
	randTime := rand.Int31n(200) + 150
	return time.Duration(randTime) * time.Millisecond
}

// 100 ms
func randHeartsBeatTime() time.Duration {
	randTime := rand.Int31n(100)
	return time.Duration(randTime) * time.Millisecond
}

// state = follower , currentTerm = term , leaderId = leaderId , voteFor = Non
func (rf *Raft) becomeFollower(term int, leaderId int) {
	rf.state = Follower
	if term > 0 {
		rf.currentTerm = term
	}
	if leaderId > 0 {
		rf.leaderId = leaderId
	}
	rf.electionTimer.Reset(randElectionTime())
	rf.heartsbeatTimer.Stop()
}

// state = Candidate
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.heartsbeatTimer.Stop()
	rf.electionTimer.Reset(randElectionTime())
}

// state = Leader
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.electionTimer.Stop()
	rf.heartsbeatTimer.Reset(0)

	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) increaseTerm() {
	rf.currentTerm++
}
