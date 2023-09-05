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

	Config           shardctrler.Config
	InstallShardArgs InstallShardArgs
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

	request    []ShardKey
	dst        []int
	prevConfig shardctrler.Config
}

type Shard struct {
	KVMap  map[string]string
	Result map[int64]CachedReply
}

type ShardKey struct {
	Shard, Num int
}

type RequestShardArgs struct {
	ShardKey ShardKey
}

type InstallShardArgs struct {
	Key   ShardKey
	Shard Shard
}

type RequestShardReply struct {
	Err   Err
	Shard Shard
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
			DPrintf("Client %v gid %v num %v", kv.me, kv.gid, kv.config.Num)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		DPrintf("Client %v gid %v shard %v Get %v", kv.me, kv.gid, key2shard(args.Key), args.Key)
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
			DPrintf("Client %v gid %v num %v", kv.me, kv.gid, kv.config.Num)

			kv.mu.Unlock()
			return
		}
		DPrintf("Client %v gid %v shard %v ", kv.me, kv.gid, key2shard(args.Key))
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
	kv.persister = persister
	// You may need initialization code here.
	kv.initSnapshot()
	kv.maxraftstate = maxraftstate
	DPrintf("ShardServer %v gid %v starting lastApplyIndex:%v", kv.me, kv.gid, kv.lastApplyIndex)
	go kv.rxMsg()
	go kv.updateConfig()
	go kv.requestShard()
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
			} else {
				panic("")
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
				DPrintf("ShardKV %v gid %v will accept request for shard:%v num:%v", kv.me, kv.gid, i, oc.Num)
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
				DPrintf("ShardKV %v gid %v need shard %v from %v", kv.me, kv.gid, i, oc.Shards[i])
				key := ShardKey{Num: oc.Num, Shard: i}
				kv.request = append(kv.request, key)
				kv.dst = append(kv.dst, oc.Shards[i])
			}
		}
		kv.prevConfig = kv.config
		kv.config = op.Config
		DPrintf("KVShard %v gid %v change from %v to %v", kv.me, kv.gid, oc.Num, nc.Num)
	}
}
func (kv *ShardKV) applyInstallShardCommand(op Op) {
	DPrintf("ShardKV %v gid %v apply InstallShardCommand cur:%v new:%v", kv.me, kv.gid, kv.curConfigNum[op.InstallShardArgs.Key.Shard], op.InstallShardArgs.Key.Num)
	if kv.curConfigNum[op.InstallShardArgs.Key.Shard] < op.InstallShardArgs.Key.Num {
		kv.curConfigNum[op.InstallShardArgs.Key.Shard] = op.InstallShardArgs.Key.Num
		kv.mp[op.InstallShardArgs.Key] = kv.copyShard(op.InstallShardArgs.Shard) // race condition here
		len := len(kv.dst)
		DPrintf("gid %v delete request %v %v", kv.gid, kv.request, op.InstallShardArgs.Key.Shard)
		for i := 0; i < len; i++ {
			if kv.request[i].Shard == op.InstallShardArgs.Key.Shard {
				kv.request = append(kv.request[:i], kv.request[i+1:]...)
				kv.dst = append(kv.dst[:i], kv.dst[i+1:]...)
				break
			}
		}

	}
	// dead lock here before
}
func (kv *ShardKV) sendRequestShard(key ShardKey, expectedConfig int, gid int) {

	for !kv.killed() {
		args := RequestShardArgs{}
		args.ShardKey = key
		kv.mu.Lock()
		config := kv.config
		if config.Num != expectedConfig {
			DPrintf("gid %v expect:%v actual:%v", kv.gid, expectedConfig, config.Num)
			return
		}
		servers, ok := kv.prevConfig.Groups[gid]
		kv.mu.Unlock()
		if !ok {
			DPrintf("%v %v %v", config.Groups, config.Num, config.Shards)
			panic(gid)
		}
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply RequestShardReply
			ok := srv.Call("ShardKV.RequestShard", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("ShardKV %v gid %v successfully RequestShard num:%v shard:%v", kv.me, kv.gid, key.Num, key.Shard)
				args.ShardKey.Num++
				args := InstallShardArgs{Key: args.ShardKey, Shard: reply.Shard}
				kv.installShard(&args)
				return
			}
			if ok && reply.Err == ErrNoShard {
				DPrintf("ShardKV %v gid %v  RequestShard num:%v shard:%v NoShard", kv.me, kv.gid, key.Num, key.Shard)
				return
			}
			DPrintf("ShardKV %v gid %v send to request %v shard %v num %v failed due to %v", kv.me, kv.gid, gid, args.ShardKey.Shard, args.ShardKey.Num, reply.Err)
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
	e.Encode(kv.request)
	e.Encode(kv.dst)
	e.Encode(kv.prevConfig)
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
	d.Decode(&kv.request)
	d.Decode(&kv.dst)
	d.Decode(&kv.prevConfig)
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
	} else {
		kv.dup = make(map[int64]CachedReply)
		kv.mp = map[ShardKey]Shard{}
	}
}

func (kv *ShardKV) updateConfig() {
	for !kv.killed() {
		kv.mu.Lock()
		newConfig := kv.clerk.Query(kv.config.Num + 1)
		DPrintf("ShardKV %v gid %v  get config Num %v shard %v", kv.me, kv.gid, newConfig.Num, newConfig.Shards)
		oldConfig := kv.config
		if oldConfig.Num == newConfig.Num || len(kv.request) != 0 {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Unlock()

		args := Op{OpType: CONFIG, Config: newConfig}
		for !kv.killed() {
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

func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	key := args.ShardKey
	kv.mu.Lock()
	v, ok := kv.mp[key]
	if ok {
		reply.Shard = kv.copyShard(v)
		reply.Err = OK
	} else {
		reply.Err = ErrNoShard
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) installShard(args *InstallShardArgs) {
	op := Op{OpType: InstallShard, InstallShardArgs: *args}
	for !kv.killed() {
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(50 * time.Millisecond)
		}

		kv.mu.Lock()
		_, ok := kv.mp[args.Key]
		if ok || kv.config.Num >= args.Key.Num {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) requestShard() {
	for !kv.killed() {
		kv.mu.Lock()
		for i, v := range kv.request {
			DPrintf("KVServer %v gid %v start to request Num %v Shard %v current configNum %v", kv.me, kv.gid, v.Num, v.Shard, kv.config.Num)
			go kv.sendRequestShard(v, kv.config.Num, kv.dst[i])
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}
