package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	seqNumber int
}

type GetResult struct {
	ok    bool
	reply GetReply
}

type PutAppendResult struct {
	ok    bool
	reply PutAppendReply
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				args := GetArgs{}
				args.Key = key
				args.ClientId = ck.clientId
				args.SeqNum = ck.seqNumber

				srv := ck.make_end(servers[si])
				var reply GetReply
				var result GetResult
				var mu sync.Mutex
				DPrintf("Client %v seq %v ready to send Get key %v ", ck.clientId, ck.seqNumber, args.Key)
				go asyncSendGet(srv, &mu, &args, &result)
				time.Sleep(100 * time.Millisecond)
				mu.Lock()
				reply = result.reply
				ok := result.ok
				mu.Unlock()
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.seqNumber++
					DPrintf("Clent %v Get key %v value %v", ck.clientId, key, reply.Value)
					return reply.Value
				}
				DPrintf("Client %v seq %v failed to Get key %v due to %v ok:%v", ck.clientId, ck.seqNumber, args.Key, reply.Err, ok)
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

}

func asyncSendGet(srv *labrpc.ClientEnd, mu *sync.Mutex, args *GetArgs, result *GetResult) {
	var reply GetReply
	ok := srv.Call("ShardKV.Get", args, &reply)
	mu.Lock()
	result.ok = ok
	result.reply = reply
	mu.Unlock()
}

func aysncSendPutAppend(srv *labrpc.ClientEnd, mu *sync.Mutex, args *PutAppendArgs, result *PutAppendResult) {
	var reply PutAppendReply
	ok := srv.Call("ShardKV.PutAppend", args, &reply)
	mu.Lock()
	result.ok = ok
	result.reply = reply
	mu.Unlock()
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {

	for {

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				args := PutAppendArgs{}
				args.Key = key
				args.Value = value
				args.Op = op
				args.ClientId = ck.clientId
				args.SeqNum = ck.seqNumber

				DPrintf("Client %v send to %v", ck.clientId, servers[si])
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf("Client %v seq %v ready to send %v key %v value %v", ck.clientId, ck.seqNumber, args.Op, args.Key, args.Value)

				var result PutAppendResult
				var mu sync.Mutex
				go aysncSendPutAppend(srv, &mu, &args, &result)
				time.Sleep(100 * time.Millisecond)
				mu.Lock()
				reply = result.reply
				ok := result.ok
				mu.Unlock()
				if ok && reply.Err == OK {
					ck.seqNumber++
					return
				}
				DPrintf("Client %v seq %v failed to %v key %v due to %v ok:%v", ck.clientId, ck.seqNumber, args.Op, args.Key, reply.Err, ok)
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader

			}
		}
		time.Sleep(50 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
