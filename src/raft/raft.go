package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// About apply
	applyChannel chan ApplyMsg

	// About vote
	voteFor     int
	state       int
	currentTerm int
	leaderId    int

	logs        []Entry
	lastApplied int
	commitIndex int
	nextIndex   []int
	matchIndex  []int

	// Helper
	electionTimer   *time.Timer
	heartsbeatTimer *time.Timer

	killChannel chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.voteFor)
	e.Encode(rf.currentTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var vote int
	var term int

	if d.Decode(&rf.logs) != nil || d.Decode(&vote) != nil || d.Decode(&term) != nil {
		// TODO
	} else {
		rf.voteFor = vote
		rf.currentTerm = term
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}

	log := Entry{rf.currentTerm, len(rf.logs), command}
	rf.logs = append(rf.logs, log)
	index = len(rf.logs) - 1
	term = rf.currentTerm

	DPrintf("Start Leader %d command %v", rf.me, command)

	rf.persist()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.doElection()
		case <-rf.heartsbeatTimer.C:
			rf.doHeartsBeat()
		case <-rf.killChannel:
			rf.mu.Lock()
			rf.dead = 1
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) doApply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied < rf.commitIndex && rf.commitIndex > 0 {

		// Figure 8
		if rf.logs[rf.commitIndex].Term != rf.currentTerm {
			return
		}

		msgs := make([]ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid:  true,
				Command:       rf.logs[i].Command,
				CommandIndex:  rf.logs[i].Index,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			})
		}

		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyChannel <- msg
		}

		rf.mu.Lock()
		rf.lastApplied = rf.commitIndex

	}

}

func (rf *Raft) applier() {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond)
		isLocked := true
		rf.mu.Lock()

		DPrintf("Logs rf.me %v rf.commitIndex %d rf.logs %v", rf.me, rf.commitIndex, rf.logs)

		if rf.lastApplied < rf.commitIndex && rf.commitIndex > 0 && rf.commitIndex < len(rf.logs) {

			// Figure 8
			if rf.logs[rf.commitIndex].Term != rf.currentTerm {
				rf.mu.Unlock()
				continue
			}

			msgs := make([]ApplyMsg, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msgs = append(msgs, ApplyMsg{
					CommandValid:  true,
					Command:       rf.logs[i].Command,
					CommandIndex:  rf.logs[i].Index,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				})
			}

			rf.mu.Unlock()

			for _, msg := range msgs {
				rf.applyChannel <- msg
			}

			rf.mu.Lock()
			isLocked = true
			rf.lastApplied = rf.commitIndex

		}

		if isLocked {
			rf.mu.Unlock()
		}

	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:        peers,
		persister:    persister,
		dead:         0,
		applyChannel: applyCh,
		voteFor:      Non,
		me:           me,
		state:        Follower,
		logs:         make([]Entry, 1),
		nextIndex:    make([]int, len(peers)),
		matchIndex:   make([]int, len(peers)),

		lastApplied: 0,
		commitIndex: 0,
		killChannel: make(chan int),
	}

	rf.electionTimer = time.NewTimer(randElectionTime())
	rf.heartsbeatTimer = time.NewTimer(randHeartsBeatTime())

	rf.logs[0].Term = 0
	rf.currentTerm = 1

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}
