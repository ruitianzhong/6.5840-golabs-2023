package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	PUT    OpType = "Put"
	GET    OpType = "Get"
	APPEND OpType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    OpType
	PutAppend PutAppendArgs
	Get       GetArgs
	ClientId  int64
	SeqNum    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap map[string]string
	dup   map[int64]CachedReply
}

type CachedReply struct {
	GetReply       GetReply
	PutAppendReply PutAppendReply
	Type           OpType
	SeqNum         int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op{OpType: GET, Get: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for !kv.killed() {
		currentTerm, _ := kv.rf.GetState()
		if currentTerm != term {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()

		k, ok := kv.dup[args.ClientId]
		if ok {
			if k.SeqNum == op.SeqNum {
				reply.Err = k.GetReply.Err
				reply.Value = k.GetReply.Value
				kv.mu.Unlock()
				return
			} else if k.SeqNum > op.SeqNum {
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	}

	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{OpType: OpType(args.Op), PutAppend: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	for !kv.killed() {
		currentTerm, _ := kv.rf.GetState()
		if currentTerm != term {
			DPrintf("KVServer %v term change curTerm:%v expectedTerm:%v", kv.me, currentTerm, term)
			reply.Err = ErrWrongLeader
		}
		kv.mu.Lock()
		k, ok := kv.dup[op.ClientId]
		if ok {
			if k.SeqNum == op.SeqNum {
				reply.Err = k.PutAppendReply.Err
				kv.mu.Unlock()
				return
			} else if k.SeqNum > op.SeqNum {
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.dup = make(map[int64]CachedReply)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.rxMsg()

	return kv
}

func (kv *KVServer) rxMsg() {
	for !kv.killed() {
		msg := <-kv.applyCh
		command, ok := msg.Command.(Op)
		if !ok {
			panic("Unexpected type")
		}

		kv.mu.Lock()
		id, seq := command.ClientId, command.SeqNum
		v, ok := kv.dup[id]
		if !ok || v.SeqNum < seq {
			reply := new(CachedReply)
			kv.applyCommand(reply, command)
			kv.dup[id] = *reply
		}

		kv.mu.Unlock()
	}

}

func (kv *KVServer) applyCommand(reply *CachedReply, op Op) {

	reply.Type = op.OpType
	reply.SeqNum = op.SeqNum
	if op.OpType == PUT || op.OpType == APPEND {

		v, ok := kv.kvMap[op.PutAppend.Key]
		if ok && op.OpType == APPEND {
			v += op.PutAppend.Value
		} else {
			v = op.PutAppend.Value
		}
		kv.kvMap[op.PutAppend.Key] = v
		reply.PutAppendReply.Err = OK

	} else if op.OpType == GET {
		v, ok := kv.kvMap[op.Get.Key]
		if ok {
			reply.GetReply.Value = v
			reply.GetReply.Err = OK
		} else {
			reply.GetReply.Err = ErrNoKey
		}
	} else {
		panic(op.OpType)
	}

}
