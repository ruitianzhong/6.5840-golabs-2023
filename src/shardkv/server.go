package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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
	PUT          OpType = "Put"
	GET          OpType = "Get"
	APPEND       OpType = "Append"
	CONFIG       OpType = "Config"
	InstallShard OpType = "InstallShard"
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

	Config       shardctrler.Config
	InstallShard InstallShardArgs
}

type CachedReply struct {
	GetReply       GetReply
	PutAppendReply PutAppendReply
	Type           OpType
	SeqNum         int
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap          map[string]string
	dup            map[int64]CachedReply
	mp             map[ShardKey]Shard
	lastApplyIndex int
	persister      *raft.Persister
	dead           int32
	clerk          *shardctrler.Clerk
	config         shardctrler.Config

	curConfigNum [shardctrler.NShards]int
}

type Shard struct {
	KVMap  map[string]string
	Result map[int64]CachedReply
	Mark   bool
}

type ShardKey struct {
	Shard, Num int
}

type InstallShardArgs struct {
	ShardKey ShardKey
	Shard    Shard
}

type InstallShardReply struct {
	Err Err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.checkKey(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

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
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock() // quickly return ErrWrongGroup
	if kv.checkKey(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
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
			DPrintf("ShardKV %v term change curTerm:%v expectedTerm:%v", kv.me, currentTerm, term)
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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.config.Groups = make(map[int][]string)

	kv.kvMap = make(map[string]string)
	kv.dup = make(map[int64]CachedReply)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	// You may need initialization code here.
	kv.initSnapshot()
	kv.maxraftstate = maxraftstate
	DPrintf("ShardServer %v starting lastApplyIndex:%v", kv.me, kv.lastApplyIndex)
	go kv.rxMsg()
	go kv.updateConfig()
	return kv
}

func (kv *ShardKV) rxMsg() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			command, ok := msg.Command.(Op)
			if !ok {
				panic("Unexpected type")
			}
			kv.mu.Lock()
			if command.OpType == GET || command.OpType == PUT || command.OpType == APPEND {
				id, seq := command.ClientId, command.SeqNum
				v, ok := kv.dup[id]
				if !ok || v.SeqNum < seq {
					reply := new(CachedReply)
					kv.applyCommand(reply, command)
					kv.dup[id] = *reply
				}
			} else if command.OpType == CONFIG {
				kv.applyConfigCommand(command)
			} else if command.OpType == InstallShard {
				kv.applyInstallShardCommand(command)
			}

			DPrintf("ShardServer %v apply index:%v command:%v", kv.me, msg.CommandIndex, msg.Command)
			kv.lastApplyIndex = msg.CommandIndex
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.handleSnapshotMsg(msg)
			kv.mu.Unlock()
		} else {
			panic("Unexpected applyMsg type")
		}
		kv.mu.Lock()
		kv.checkIfNeedSnapshot()
		kv.mu.Unlock()
	}

}

func (kv *ShardKV) applyCommand(reply *CachedReply, op Op) {

	reply.Type = op.OpType
	reply.SeqNum = op.SeqNum
	if op.OpType == PUT || op.OpType == APPEND {
		if kv.checkKey(op.PutAppend.Key) {
			reply.PutAppendReply.Err = ErrWrongGroup
			return
		}

		v, ok := kv.kvMap[op.PutAppend.Key]
		if ok && op.OpType == APPEND {
			v += op.PutAppend.Value
		} else {
			v = op.PutAppend.Value
		}
		kv.kvMap[op.PutAppend.Key] = v
		DPrintf("ShardServer %v set %v = %v", kv.me, op.PutAppend.Key, v)
		reply.PutAppendReply.Err = OK

	} else if op.OpType == GET {
		if kv.checkKey(op.Get.Key) {
			reply.GetReply.Err = ErrWrongGroup
			return
		}
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

func (kv *ShardKV) checkKey(key string) bool {
	shard := key2shard(key)
	return kv.config.Shards[shard] != kv.me
}

func (kv *ShardKV) applyConfigCommand(op Op) {
	if kv.config.Num < op.Config.Num {
		oc, nc := kv.config, op.Config
		for i := 0; i < shardctrler.NShards; i++ {
			if oc.Shards[i] == kv.me && nc.Shards[i] != kv.me {
				go kv.sendInstallShard(i, nc.Num, nc.Shards[i])
			}
		}

		kv.config = op.Config

	}
}
func (kv *ShardKV) applyInstallShardCommand(op Op) {
	kv.mu.Lock()
	if kv.curConfigNum[op.InstallShard.ShardKey.Shard] < op.InstallShard.ShardKey.Num {
		kv.curConfigNum[op.InstallShard.ShardKey.Shard] = op.InstallShard.ShardKey.Num
		kv.mp[op.InstallShard.ShardKey] = op.InstallShard.Shard
	}
	kv.mu.Unlock()
}
func (kv *ShardKV) sendInstallShard(shard, config, gid int) {
	for {
		if servers, ok := kv.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply InstallShardReply
				args := InstallShardArgs{}
				args.ShardKey = ShardKey{Shard: shard, Num: config}
				kv.mu.Lock()
				args.Shard = kv.mp[args.ShardKey]
				kv.mu.Unlock()
				ok := srv.Call("ShardKV.InstallShard", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.dup)
	e.Encode(kv.lastApplyIndex)
	return w.Bytes()
}
func (kv *ShardKV) decodeSnapshot(snapshot []byte) {

	w := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(w)
	d.Decode(&kv.kvMap)
	d.Decode(&kv.dup)
	d.Decode(&kv.lastApplyIndex)
}

func (kv *ShardKV) handleSnapshotMsg(applyMsg raft.ApplyMsg) {
	if kv.lastApplyIndex < applyMsg.SnapshotIndex {
		DPrintf("ShardServer %v received snapshot preIndex:%v now:%v", kv.me, kv.lastApplyIndex, applyMsg.SnapshotIndex)
		kv.decodeSnapshot(applyMsg.Snapshot)
		kv.lastApplyIndex = applyMsg.SnapshotIndex
	} else {
		DPrintf("ShardServer %v received kv.index:%v applyMsg.index:%v", kv.me, kv.lastApplyIndex, applyMsg.SnapshotIndex)
	}
}

func (kv *ShardKV) checkIfNeedSnapshot() {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < int(float32(kv.persister.RaftStateSize())*0.8) {
		return
	}
	kv.rf.Snapshot(kv.lastApplyIndex, kv.encodeSnapshot())
	DPrintf("ShardServer %v Snapshot lastApplyIndex %v", kv.me, kv.lastApplyIndex)
}

func (kv *ShardKV) initSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.decodeSnapshot(snapshot)
	}
}

func (kv *ShardKV) updateConfig() {
	for !kv.killed() {
		newConfig := kv.clerk.Query(kv.config.Num + 1)
		oldConfig := kv.config
		kv.mu.Lock()
		if oldConfig.Num == newConfig.Num {
			kv.mu.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		kv.mu.Unlock()

		args := Op{OpType: CONFIG, Config: newConfig}
		for {
			_, _, isLeader := kv.rf.Start(args)
			if !isLeader {
				break
			}
			time.Sleep(30 * time.Millisecond)
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if kv.config.Num == newConfig.Num {
				break
			}
		}
	}

}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {

	kv.mu.Lock()

	if args.ShardKey.Num <= kv.curConfigNum[args.ShardKey.Shard] {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	op := Op{}
	op.OpType = InstallShard
	op.InstallShard = *args
	for {
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
		if args.ShardKey.Num <= kv.curConfigNum[args.ShardKey.Shard] {
			kv.mu.Unlock()
			reply.Err = OK
			return
		}
		kv.mu.Unlock()
	}

}
