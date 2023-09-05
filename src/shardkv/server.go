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
	DeleteShard  OpType = "DeleteShard"
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

	Config          shardctrler.Config
	InstallShard    InstallShardArgs
	DeleteShardArgs DeleteShardArgs
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
	dup            map[int64]CachedReply
	mp             map[ShardKey]Shard
	lastApplyIndex int
	persister      *raft.Persister
	dead           int32
	clerk          *shardctrler.Clerk
	config         shardctrler.Config

	curConfigNum [shardctrler.NShards]int

	sent int

	sendMap map[ShardKey]Shard
}

type Shard struct {
	KVMap  map[string]string
	Result map[int64]CachedReply
	Gid    int
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

type DeleteShardArgs struct {
	Key ShardKey
}

type DeleteShardReply struct {
	Err Err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{OpType: GET, Get: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}

	for !kv.killed() {
		kv.mu.Lock()
		if kv.checkKey(args.Key) {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		DPrintf("Client %v gid %v shard %v mp:%v", kv.me, kv.gid, key2shard(args.Key), kv.mp)
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()

		k, ok := kv.dup[args.ClientId]
		if ok {
			if k.SeqNum == op.SeqNum && k.GetReply.Err != ErrRetry {
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

	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	// Your code here.
	op := Op{OpType: OpType(args.Op), PutAppend: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	for !kv.killed() {
		kv.mu.Lock() // quickly return ErrWrongGroup
		if kv.checkKey(args.Key) {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		DPrintf("Client %v gid %v shard %v mp:%v", kv.me, kv.gid, key2shard(args.Key), kv.mp)
		kv.mu.Unlock()
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		time.Sleep(80 * time.Millisecond)
		kv.mu.Lock()
		k, ok := kv.dup[op.ClientId]
		if ok {
			if k.SeqNum == op.SeqNum && k.PutAppendReply.Err != ErrRetry {
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
	kv.rf.SetGid(gid)
	kv.config.Groups = make(map[int][]string)
	kv.dup = make(map[int64]CachedReply)
	kv.mp = map[ShardKey]Shard{}
	kv.persister = persister
	kv.sendMap = map[ShardKey]Shard{}
	// You may need initialization code here.
	kv.initSnapshot()
	kv.maxraftstate = maxraftstate
	DPrintf("ShardServer %v gid %v starting lastApplyIndex:%v", kv.me, kv.gid, kv.lastApplyIndex)
	go kv.rxMsg()
	go kv.updateConfig()
	go kv.sendShard()
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
				if !ok || v.SeqNum < seq || v.PutAppendReply.Err == ErrRetry || v.GetReply.Err == ErrRetry { // allow to retry
					reply := new(CachedReply)
					kv.applyCommand(reply, command)
					kv.dup[id] = *reply
				}
			} else if command.OpType == CONFIG {
				kv.applyConfigCommand(command)
			} else if command.OpType == InstallShard {
				kv.applyInstallShardCommand(command)
			} else if command.OpType == DeleteShard {
				kv.applyDeleteShardCommand(command)
			}

			DPrintf("ShardServer %v gid %v apply index:%v command:%v", kv.me, kv.gid, msg.CommandIndex, msg.Command)
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

func (kv *ShardKV) applyDeleteShardCommand(op Op) {

	key := op.DeleteShardArgs.Key
	_, ok := kv.mp[key]
	DPrintf("KVShard %v gid %v prepare to delete num %v shard %v", kv.me, kv.gid, key.Num, key.Shard)
	if ok {
		delete(kv.mp, key)
		key.Num++
		delete(kv.sendMap, key)
		kv.sent--
		DPrintf("KVShard %v gid %v delete successfully num %v shard %v mp:%v", kv.me, kv.gid, key.Num, key.Shard, kv.mp)
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
		shard := key2shard(op.PutAppend.Key)
		key := ShardKey{Shard: shard, Num: kv.config.Num}
		DPrintf("Client %v gid %v apply command shard:%v num:%v", kv.me, kv.gid, key.Shard, key.Num)
		s, exist := kv.mp[key]
		DPrintf("applyCommand %v", kv.mp)
		if exist {
			e, o := s.Result[op.ClientId]
			if !o || e.SeqNum < op.SeqNum || e.PutAppendReply.Err == ErrRetry {

				v, ok := s.KVMap[op.PutAppend.Key]
				if ok && op.OpType == APPEND {
					v += op.PutAppend.Value
				} else {
					v = op.PutAppend.Value
				}
				s.KVMap[op.PutAppend.Key] = v
				DPrintf("ShardServer %v gid %v set %v = %v", kv.me, kv.gid, op.PutAppend.Key, v)
				reply.PutAppendReply.Err = OK
				s.Result[op.ClientId] = *reply
			} else {
				*reply = e
			}
		} else {
			reply.PutAppendReply.Err = ErrRetry
			DPrintf("ShardServer %v gid %v reply with ErrRetry to put/append shard:%v num:%v mp:%v", kv.me, kv.gid, key.Shard, key.Num, kv.mp)
			for k := range kv.mp {
				DPrintf("ShardServer %v gid %v have shard:%v num:%v", kv.me, kv.gid, k.Shard, k.Num)
			}
		}

	} else if op.OpType == GET {
		if kv.checkKey(op.Get.Key) {
			reply.GetReply.Err = ErrWrongGroup
			return
		}
		shard := key2shard(op.Get.Key)
		key := ShardKey{Shard: shard, Num: kv.config.Num}
		s, exist := kv.mp[key]
		if exist {
			e, o := s.Result[op.ClientId]
			if !o || e.SeqNum < op.SeqNum || e.GetReply.Err == ErrRetry {
				v, ok := s.KVMap[op.Get.Key]
				if ok {
					reply.GetReply.Value = v
					reply.GetReply.Err = OK
				} else {
					reply.GetReply.Err = ErrNoKey
				}
				s.Result[op.ClientId] = *reply
			} else {
				*reply = e
			}
		} else {
			reply.GetReply.Err = ErrRetry
			DPrintf("ShardServer %v gid %v reply with ErrRetry to get shard:%v num:%v mp:%v", kv.me, kv.gid, key.Shard, key.Num, kv.mp)
			for k := range kv.mp {
				DPrintf("ShardServer %v gid %v have shard:%v num:%v", kv.me, kv.gid, k.Shard, k.Num)

			}
		}
	} else {
		panic(op.OpType)
	}

}

func (kv *ShardKV) checkKey(key string) bool {
	shard := key2shard(key)
	return kv.config.Shards[shard] != kv.gid
}

func (kv *ShardKV) applyConfigCommand(op Op) {
	if kv.config.Num < op.Config.Num {
		oc, nc := kv.config, op.Config
		for i := 0; i < shardctrler.NShards; i++ {
			if oc.Shards[i] == kv.gid && nc.Shards[i] != kv.gid {
				key := ShardKey{Num: oc.Num, Shard: i}
				s := kv.copyShard(kv.mp[key])
				s.Gid = nc.Shards[i]
				key.Num = nc.Num
				kv.sendMap[key] = s
				kv.sent++
				DPrintf("ShardKV %v gid %v will install shard:%v num:%v", kv.me, kv.gid, key.Shard, key.Num)
			} else if oc.Shards[i] == kv.gid && nc.Shards[i] == kv.gid {
				key := ShardKey{Num: oc.Num, Shard: i}
				shard, ok := kv.mp[key]
				if !ok {
					panic("Unexpected behavior")
				}
				delete(kv.mp, key)
				key.Num++
				kv.mp[key] = shard
				DPrintf("ShardKV %v gid %v copy original shard:%v num:%v", kv.me, kv.gid, key.Shard, key.Num)

			} else if oc.Shards[i] == 0 && nc.Shards[i] == kv.gid {
				key := ShardKey{Num: nc.Num, Shard: i}
				shard := initShard()
				kv.mp[key] = shard
				DPrintf("ShardKV %v gid %v init shard:%v num:%v", kv.me, kv.gid, key.Shard, key.Num)
			} else if oc.Shards[i] != kv.gid && nc.Shards[i] == kv.gid {
				DPrintf("ShardKV %v gid %v need shard %v from %v", kv.me, kv.gid, i, nc.Shards[i])

			}
		}

		kv.config = op.Config
	}
}
func (kv *ShardKV) applyInstallShardCommand(op Op) {
	DPrintf("ShardKV %v gid %v apply InstallShardCommand cur:%v new:%v", kv.me, kv.gid, kv.curConfigNum[op.InstallShard.ShardKey.Shard], op.InstallShard.ShardKey.Num)
	if kv.curConfigNum[op.InstallShard.ShardKey.Shard] < op.InstallShard.ShardKey.Num {
		kv.curConfigNum[op.InstallShard.ShardKey.Shard] = op.InstallShard.ShardKey.Num
		kv.mp[op.InstallShard.ShardKey] = kv.copyShard(op.InstallShard.Shard) // race condition here
		DPrintf("kv.mp:%v", kv.mp)
	}
	// dead lock here before
}
func (kv *ShardKV) sendInstallShard(key ShardKey, shard Shard) {
	args := InstallShardArgs{}
	args.ShardKey = key
	args.Shard = shard
	gid := shard.Gid
	kv.mu.Lock()
	config := kv.config
	kv.mu.Unlock()
	servers, ok := config.Groups[gid]
	if !ok {
		DPrintf("%v %v %v", config.Groups, config.Num, config.Shards)
		panic(gid)
	}
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply InstallShardReply
			ok := srv.Call("ShardKV.InstallShard", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("ShardKV %v gid %v successfully sent", kv.me, kv.gid)
				darg := DeleteShardArgs{}
				key.Num--
				darg.Key = key
				kv.deleteShard(&darg)
				return
			}
			DPrintf("ShardKV %v gid %v send to %v shard %v num %v failed due to %v", kv.me, kv.gid, gid, args.ShardKey.Shard, args.ShardKey.Num, reply.Err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) copyShard(shard Shard) Shard {
	nshard := Shard{}
	result, kvmap := map[int64]CachedReply{}, map[string]string{}
	for k, v := range shard.Result {
		result[k] = v
	}
	for k, v := range shard.KVMap {
		kvmap[k] = v

	}
	nshard.Result = result
	nshard.KVMap = kvmap
	DPrintf("ShardKV %v gid %v kv.mp:%v nshard.KVMap:%v shard.KVMap %v", kv.me, kv.gid, kv.mp, kvmap, shard.KVMap)
	return nshard
}

func initShard() Shard {
	shard := Shard{}
	shard.KVMap = make(map[string]string)
	shard.Result = make(map[int64]CachedReply)
	return shard
}
func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.mp)
	e.Encode(kv.dup)

	e.Encode(kv.lastApplyIndex)
	e.Encode(kv.config)
	e.Encode(kv.curConfigNum)
	e.Encode(kv.sendMap)
	e.Encode(kv.sent)
	return w.Bytes()
}
func (kv *ShardKV) decodeSnapshot(snapshot []byte) {

	w := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(w)
	d.Decode(&kv.mp)
	d.Decode(&kv.dup)
	d.Decode(&kv.lastApplyIndex)
	d.Decode(&kv.config)
	d.Decode(&kv.curConfigNum)
	d.Decode(&kv.sendMap)
	d.Decode(&kv.sent)
}

func (kv *ShardKV) handleSnapshotMsg(applyMsg raft.ApplyMsg) {
	if kv.lastApplyIndex < applyMsg.SnapshotIndex {
		DPrintf("ShardServer %v gid:%v received snapshot preIndex:%v now:%v", kv.me, kv.gid, kv.lastApplyIndex, applyMsg.SnapshotIndex)
		kv.decodeSnapshot(applyMsg.Snapshot)
		kv.lastApplyIndex = applyMsg.SnapshotIndex
	} else {
		DPrintf("ShardServer %v gid:%v received kv.index:%v applyMsg.index:%v", kv.me, kv.gid, kv.lastApplyIndex, applyMsg.SnapshotIndex)
	}
}

func (kv *ShardKV) checkIfNeedSnapshot() {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < int(float32(kv.persister.RaftStateSize())*0.8) {
		return
	}
	kv.rf.Snapshot(kv.lastApplyIndex, kv.encodeSnapshot())
	DPrintf("ShardServer %v gid %v Snapshot lastApplyIndex %v", kv.me, kv.gid, kv.lastApplyIndex)
}

func (kv *ShardKV) initSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.decodeSnapshot(snapshot)
	}
}

func (kv *ShardKV) updateConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		newConfig := kv.clerk.Query(kv.config.Num + 1)
		DPrintf("ShardKV %v gid %v  get config Num %v shard %v", kv.me, kv.gid, newConfig.Num, newConfig.Shards)

		DPrintf("ShardKV %v gid %v configNum %v shard:%v unsent %v recAll:%v", kv.me, kv.gid, kv.config.Num, kv.config.Shards, kv.sent, kv.checkReceivedAll())
		oldConfig := kv.config
		if oldConfig.Num == newConfig.Num || kv.sent != 0 || !kv.checkReceivedAll() {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Unlock()

		args := Op{OpType: CONFIG, Config: newConfig}
		for {
			_, _, isLeader := kv.rf.Start(args)
			if !isLeader {
				DPrintf("ShardKV %v gid %v is not leader", kv.me, kv.gid)
				time.Sleep(100 * time.Millisecond) // can not exit
			} else {
				DPrintf("ShardKV %v gid %v is leader", kv.me, kv.gid)

			}
			time.Sleep(100 * time.Millisecond)
			kv.mu.Lock()
			DPrintf("ShardKV %v gid %v config.num %v newconfig %v", kv.me, kv.gid, kv.config.Num, newConfig.Num)

			if kv.config.Num >= newConfig.Num {
				DPrintf("ShardKV %v gid %v update successfully", kv.me, kv.gid)
				kv.mu.Unlock()
				break
			}
			kv.mu.Unlock()
		}
	}

}

func (kv *ShardKV) checkReceivedAll() bool {
	for i, v := range kv.config.Shards {

		if v == kv.gid {
			k := ShardKey{Shard: i, Num: kv.config.Num}
			_, ok := kv.mp[k]
			if !ok {
				return false
			}
		}
	}
	return true

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
	DPrintf("ShardKV %v gid %v InstallShard request started from %v", kv.me, kv.gid, args.Shard.Gid)

	for {
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			DPrintf("ShardKV %v gid %v InstallShard reject with ErrWrongLeader to %v", kv.me, kv.gid, args.Shard.Gid)
			reply.Err = ErrWrongLeader
			return
		} else {
			DPrintf("ShardKV %v gid %v InstallShard is leader request from %v", kv.me, kv.gid, args.Shard.Gid)
		}
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		if args.ShardKey.Num <= kv.curConfigNum[args.ShardKey.Shard] {
			kv.mu.Unlock()
			DPrintf("ShardKV %v gid %v reply with OK", kv.me, kv.gid)
			reply.Err = OK
			return
		}
		kv.mu.Unlock()
	}

}

func (kv *ShardKV) deleteShard(args *DeleteShardArgs) {
	op := Op{OpType: DeleteShard, DeleteShardArgs: *args}
	for {
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(50 * time.Millisecond)
		}

		kv.mu.Lock()
		_, ok := kv.mp[args.Key]
		if !ok {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

	}
}

func (kv *ShardKV) sendShard() {
	for {
		kv.mu.Lock()
		for k, v := range kv.sendMap {
			DPrintf("KVServer %v gid %v start to send Num %v Shard %v", kv.me, kv.gid, k.Num, k.Shard)
			go kv.sendInstallShard(k, v)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
