package shardctrler

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const wait = 1 * time.Millisecond

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	dup map[int64]CtrlerReply
}
type CtrlerOpType string

const (
	MOVE  CtrlerOpType = "Move"
	JOIN  CtrlerOpType = "Join"
	LEAVE CtrlerOpType = "Leave"
	QUERY CtrlerOpType = "Query"
)

type Op struct {
	// Your data here.
	ClientId     int64
	SeqNumber    int
	CtrlerOpType CtrlerOpType
	JoinArgs     JoinArgs
	LeaveArgs    LeaveArgs
	MoveArgs     MoveArgs
	QueryArgs    QueryArgs
}

type CtrlerReply struct {
	SeqNumber    int
	CtrlerOpType CtrlerOpType
	JoinReply    JoinReply
	LeaveReply   LeaveReply
	MoveReply    MoveReply
	QueryReply   QueryReply
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{ClientId: args.ClientId, SeqNumber: args.SeqNumber, CtrlerOpType: JOIN, JoinArgs: *args}
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	for {
		currentTerm, _ := sc.rf.GetState()
		if currentTerm != term {
			reply.WrongLeader = true
			return
		}
		sc.mu.Lock()
		v, ok := sc.dup[args.ClientId]
		if ok {
			if args.SeqNumber == v.SeqNumber {
				*reply = v.JoinReply
				sc.mu.Unlock()
				return
			} else if args.SeqNumber < v.SeqNumber {
				reply.WrongLeader = true
				sc.mu.Unlock()
				return
			}
		}
		sc.mu.Unlock()
		time.Sleep(wait)
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{ClientId: args.ClientId, SeqNumber: args.SeqNumber, CtrlerOpType: LEAVE, LeaveArgs: *args}
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	for {
		currentTerm, _ := sc.rf.GetState()
		if currentTerm != term {
			reply.WrongLeader = true
			return
		}
		sc.mu.Lock()
		v, ok := sc.dup[args.ClientId]
		if ok {
			if args.SeqNumber == v.SeqNumber {
				*reply = v.LeaveReply
				sc.mu.Unlock()
				return
			} else if args.SeqNumber < v.SeqNumber {
				reply.WrongLeader = true
				sc.mu.Unlock()
				return
			}
		}
		sc.mu.Unlock()
		time.Sleep(wait)
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{ClientId: args.ClientId, SeqNumber: args.SeqNumber, CtrlerOpType: MOVE, MoveArgs: *args}
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	for {
		currentTerm, _ := sc.rf.GetState()
		if currentTerm != term {
			reply.WrongLeader = true
			return
		}
		sc.mu.Lock()
		v, ok := sc.dup[args.ClientId]
		if ok {
			if args.SeqNumber == v.SeqNumber {
				*reply = v.MoveReply
				sc.mu.Unlock()
				return
			} else if args.SeqNumber < v.SeqNumber {
				reply.WrongLeader = true
				sc.mu.Unlock()
				return
			}
		}
		sc.mu.Unlock()
		time.Sleep(wait)
	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{ClientId: args.ClientId, SeqNumber: args.SeqNumber, CtrlerOpType: QUERY, QueryArgs: *args}
	_, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	for {
		currentTerm, _ := sc.rf.GetState()
		if currentTerm != term {
			reply.WrongLeader = true
			return
		}
		sc.mu.Lock()
		v, ok := sc.dup[args.ClientId]
		if ok {
			if args.SeqNumber == v.SeqNumber {
				*reply = v.QueryReply
				sc.mu.Unlock()
				return
			} else if args.SeqNumber < v.SeqNumber {
				reply.WrongLeader = true
				sc.mu.Unlock()
				return
			}
		}
		sc.mu.Unlock()
		time.Sleep(wait)
	}
	// Your code here.
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.rxMsg()
	return sc
}

func (sc *ShardCtrler) rxMsg() {

	for msg := range sc.applyCh {
		sc.mu.Lock()

		if msg.CommandValid {
			command, ok := msg.Command.(Op)
			if !ok {
				panic("Unexpected command type")
			}
			v, exist := sc.dup[command.ClientId]
			if !exist || v.SeqNumber < command.SeqNumber {
				switch command.CtrlerOpType {
				case JOIN:
					sc.applyJoin(command)
				case LEAVE:
					sc.applyLeave(command)
				case MOVE:
					sc.applyMove(command)
				case QUERY:
					sc.applyQuery(command)
				default:
					panic("Unexpected CtrlerOpType")
				}
			}
		} else {
			panic("Unexpected applyMsg type")
		}

		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyJoin(op Op) {

}

func (sc *ShardCtrler) applyLeave(op Op) {

}

func (sc *ShardCtrler) applyQuery(op Op) {

}

func (sc *ShardCtrler) applyMove(op Op) {

}
