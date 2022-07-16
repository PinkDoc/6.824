package kvraft

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id       int64
	seqId    int64
	leaderId int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	return ck
}

func (ck *Clerk) leader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	return ck.leaderId
}

func (ck *Clerk) retryLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	leader := ck.leaderId
	leader = (leader + 1) % len(ck.servers)
	ck.leaderId = leader
	return leader
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.id,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}

	leader := ck.leader()

	for {
		reply := new(GetReply)
		DPrintf("Client {%d} Call {%d} SeqId {%d} ", ck.id, leader, args.SeqId)
		if ck.servers[leader].Call("KVServer.Get", &args, reply) {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			} else {
				leader = ck.retryLeader()
			}
		} else {
			leader = ck.retryLeader()
		}

		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.id,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}

	leader := ck.leader()

	for {
		reply := new(PutAppendReply)
		DPrintf("Client {%d} Call {%d} SeqId {%d} ", ck.id, leader, args.SeqId)
		if ck.servers[leader].Call("KVServer.PutAppend", &args, reply) {
			if reply.Err == OK {
				return
			} else {
				leader = ck.retryLeader()
			}
		} else {
			leader = ck.retryLeader()
		}

		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
