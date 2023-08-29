package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	clientId     int64
	cachedLeader int
	// You will have to modify this struct.
	seqNumber int
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
	ck.clientId = nrand()
	ck.cachedLeader = 0
	ck.seqNumber = 0
	// You'll have to add code here.
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{}
	args.ClientId = ck.clientId
	args.Key = key
	args.SeqNum = ck.seqNumber
	var reply GetReply
	ok := false
	i := ck.cachedLeader
	for !ok {
		ch := make(chan GetReply)
		go ck.asyncSendGet(i%len(ck.servers), &args, ch)
		t := time.After(40 * time.Millisecond)
		for {
			b := false
			select {
			case <-t:
				b = true
				i++
				DPrintf("Client %v Get Timeout leader:%v", ck.clientId, i%len(ck.servers))
			case reply = <-ch:
				if reply.Err == ErrWrongLeader {
					i++
					DPrintf("Client %v Get WrongLeader:%v", ck.clientId, i%len(ck.servers))
				} else if reply.Err == OK || reply.Err == ErrNoKey {
					ok = true
					DPrintf("Client %v Get %v", ck.clientId, reply.Err)
				} else if reply.Err == ErrDisconnected {
					i++
				}
				b = true
			}
			if b {
				break
			}
		}

	}
	ck.cachedLeader = i % len(ck.servers)
	ck.seqNumber += 1
	if reply.Err == ErrNoKey {
		DPrintf("Client  %v No Key %v", ck.clientId, key)
		return ""
	} else {
		DPrintf("Client %v GET Key:%v Value:%v", ck.clientId, key, reply.Value)
		return reply.Value
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.ClientId = ck.clientId
	args.Key = key
	args.Value = value
	args.SeqNum = ck.seqNumber
	args.Op = op
	var reply PutAppendReply
	ok := false
	i := ck.cachedLeader
	for !ok {
		ch := make(chan PutAppendReply, 1)
		go ck.asyncSendPutAppend(i%len(ck.servers), &args, ch)
		t := time.After(40 * time.Millisecond)
		for !ok {
			b := false
			select {
			case reply = <-ch:
				if reply.Err == OK {
					ok = true
					DPrintf("Client %v PutAppend OK Leader:%v Seq %v", ck.clientId, i%len(ck.servers), ck.seqNumber)
				} else if reply.Err == ErrWrongLeader {
					DPrintf("Client %v PutAppend WrongLeader Leader:%v Seq %v", ck.clientId, i%len(ck.servers), ck.seqNumber)
					i++
				} else if reply.Err == ErrDisconnected {
					i++
				}
				b = true
			case <-t:
				b = true
				i++
				DPrintf("Client %v PutAppend Timeout Leader:%v Seq %v", ck.clientId, i%len(ck.servers), ck.seqNumber)
			}
			if b {
				break
			}

		}

	}
	ck.seqNumber += 1
	ck.cachedLeader = i % len(ck.servers)

}

func (ck *Clerk) asyncSendGet(server int, args *GetArgs, ch chan GetReply) {

	reply := GetReply{}
	ok := ck.sendGet(server, args, &reply)
	if !ok {
		reply.Err = ErrDisconnected
	}
	ch <- reply
}

func (ck *Clerk) asyncSendPutAppend(server int, args *PutAppendArgs, ch chan PutAppendReply) {
	reply := PutAppendReply{}
	ok := ck.sendPutAppend(server, args, &reply)
	if !ok {
		reply.Err = ErrDisconnected
	}
	ch <- reply
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
	DPrintf("Client %v Put Key %v Value:%v", ck.clientId, key, value)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
	DPrintf("Client %v Append Key:%v Value:%v", ck.clientId, key, value)
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}
