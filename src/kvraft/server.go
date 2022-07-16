package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Term     int
	Index    int
	ClientId int64
	SeqId    int64
	Type     string
	Key      string
	Value    string
}

type OpContext struct {
	O        *Op
	C        chan byte
	Ok       bool
	ExistKey bool
	Closed   bool
}

const (
	timeout = time.Second * 2
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	store      map[string]string
	contextMap map[int64]*OpContext
	seqMap     map[int64]int64

	seqId int64
}

func (kv *KVServer) GetId() int64 {
	return atomic.AddInt64(&kv.seqId, 1)
}

func (kv *KVServer) makeContext(op *Op) *OpContext {
	c := new(OpContext)
	c.O = op
	c.C = make(chan byte)
	c.Ok = false
	c.ExistKey = false
	c.Closed = false
	kv.contextMap[op.ClientId] = c
	return c
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Term:     0,
		Index:    0,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Type:     GET,
		Key:      args.Key,
		Value:    "",
	}

	kv.mu.Lock()

	ok := false
	op.Index, op.Term, ok = kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	//DPrintf("Method Node {%d}client {%d} seqId {%d}", kv.me, op.ClientId, op.SeqId)

	c := kv.makeContext(&op)

	alarm := time.NewTimer(timeout)

	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		alarm.Stop()
		delete(kv.contextMap, op.ClientId)
		//DPrintf("Node {%v} reply.Err %s reply.Value %s", kv.me, reply.Err, reply.Value)
	}()

	kv.mu.Unlock()
	select {
	case <-c.C:
		kv.mu.Lock()
		if !c.Ok {
			reply.Err = ErrWrongLeader
		} else {
			if c.ExistKey {
				reply.Value = op.Value
				reply.Err = OK
			} else {
				reply.Value = ""
				reply.Err = ErrNoKey
			}
		}
		kv.mu.Unlock()
		return
	case <-alarm.C:
		kv.mu.Lock()
		reply.Err = ErrTimeOut
		kv.mu.Unlock()
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Term:     0,
		Index:    0,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}

	kv.mu.Lock()

	ok := false
	op.Index, op.Term, ok = kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf("PutAppend Node {%d} Key %s Value %s client {%d} seqId {%d}", kv.me, args.Key, args.Value, op.ClientId, op.SeqId)
	if temp, exist := kv.store[args.Key]; exist {
		DPrintf("KeyIs %s ValueIs %s", args.Key, temp)
	}

	c := kv.makeContext(&op)

	alarm := time.NewTimer(timeout)
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		alarm.Stop()
		delete(kv.contextMap, op.ClientId)
		DPrintf("Node {%d} reply.Err {%s} client {%d} seqId {%d} ", kv.me, reply.Err, args.ClientId, args.SeqId)
	}()

	kv.mu.Unlock()
	select {
	case <-c.C:
		kv.mu.Lock()
		reply.Err = OK
		kv.mu.Unlock()
		return
	case <-alarm.C:
		kv.mu.Lock()
		reply.Err = ErrTimeOut
		kv.mu.Unlock()
		return
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		cmd := <-kv.applyCh
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			op := cmd.Command.(Op)

			clientId := op.ClientId

			c, existContext := kv.contextMap[clientId]
			if c != nil {
				DPrintf("OpContext {%v} ", *c)
			}
			oldSeqId, existOldSeqId := kv.seqMap[clientId]
			kv.seqMap[clientId] = op.SeqId

			termOk := true

			if existContext && (cmd.CommandTerm != c.O.Term) {
				DPrintf("cmd.CommandIndex {%v} c.O.Index {%v} ", cmd.CommandIndex, c.O.Index)
				termOk = false
			}

			if op.Type == GET {
				if existContext && termOk {
					c.Ok = true
					c.O.Value, c.ExistKey = kv.store[op.Key]
				}
			} else if op.Type == PUT {
				if !existOldSeqId || oldSeqId < op.SeqId {
					kv.store[op.Key] = op.Value
				}
				if existContext && termOk {
					c.Ok = true
				}
			} else if op.Type == PUTAPPEND {
				if !existOldSeqId || oldSeqId < op.SeqId {
					val, ok := kv.store[op.Key]
					if ok {
						kv.store[op.Key] = val + op.Value
					} else {
						kv.store[op.Key] = op.Value
					}
					if existContext && termOk {
						c.Ok = true
					}
				}
			}

			if existContext && !c.Closed {
				DPrintf("Close channel Node {%d} ClientId {%d} SeqId {%d}", kv.me, c.O.ClientId, c.O.SeqId)
				c.Closed = true
				close(c.C)
			}

			if !existOldSeqId || oldSeqId < op.SeqId {
				DPrintf("Node {%d} apply client {%d} seqId {%d}", kv.me, op.ClientId, op.SeqId)
			}

		}()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.store = make(map[string]string)
	kv.contextMap = make(map[int64]*OpContext)
	kv.seqMap = make(map[int64]int64)

	kv.seqId = 0

	go kv.applier()

	return kv
}
