package shardctrler

import (
	"sort"
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

	ngroup int
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
	sc.dup = make(map[int64]CtrlerReply)
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
	args, reply := op.JoinArgs, JoinReply{}

	creply := CtrlerReply{CtrlerOpType: JOIN, JoinReply: reply, SeqNumber: op.SeqNumber}
	sc.dup[op.ClientId] = creply
	reply.Err = OK
	config := cloneConfig(sc.configs[len(sc.configs)-1])
	config.Num++
	for k, v := range args.Servers {
		config.Groups[k] = v
	}
	sc.ngroup += len(args.Servers)
	sc.rebalance(&config)
	sc.configs = append(sc.configs, config)

}

func (sc *ShardCtrler) applyLeave(op Op) {
	args, reply := op.LeaveArgs, LeaveReply{}

	creply := CtrlerReply{CtrlerOpType: LEAVE, LeaveReply: reply, SeqNumber: op.SeqNumber}
	sc.dup[op.ClientId] = creply
	reply.Err = OK
	config := cloneConfig(sc.configs[len(sc.configs)-1])
	config.Num++
	for i := 0; i < len(args.GIDs); i++ {
		for j := 0; j < NShards; j++ {
			if config.Shards[j] == args.GIDs[i] {
				config.Shards[j] = 0
			}
		}
		delete(config.Groups, args.GIDs[i])
	}
	sc.ngroup -= len(args.GIDs)
	sc.rebalance(&config)
	sc.configs = append(sc.configs, config)

}

func (sc *ShardCtrler) applyQuery(op Op) {
	args, reply := op.QueryArgs, QueryReply{}
	i := args.Num
	if args.Num == -1 || args.Num >= len(sc.configs) {
		i = len(sc.configs) - 1
	}
	reply.Config = cloneConfig(sc.configs[i])
	reply.Err = OK
	creply := CtrlerReply{CtrlerOpType: QUERY, QueryReply: reply, SeqNumber: op.SeqNumber}
	sc.dup[op.ClientId] = creply
}

func (sc *ShardCtrler) applyMove(op Op) {
	args, reply := op.MoveArgs, MoveReply{}
	config := cloneConfig(sc.configs[len(sc.configs)-1])
	config.Shards[args.Shard] = args.GID
	config.Num++
	reply.Err = OK
	creply := CtrlerReply{CtrlerOpType: MOVE, MoveReply: reply, SeqNumber: op.SeqNumber}
	sc.dup[op.ClientId] = creply
	sc.configs = append(sc.configs, config)
}

func cloneConfig(config Config) Config {
	nconfig := Config{Num: config.Num}
	nconfig.Shards = config.Shards
	nconfig.Groups = map[int][]string{}
	for k, v := range config.Groups {
		nconfig.Groups[k] = v
	}
	return nconfig
}

func (sc *ShardCtrler) rebalance(config *Config) {

	g2s, pending := sc.group2Shards(config)
	target := makeTarget(NShards, sc.ngroup)
	for i := 0; i < sc.ngroup; i++ {
		d := len(g2s[i].shard) - target[i]
		if d > 0 {
			pending = append(pending, g2s[i].shard[:d]...)
			g2s[i].shard = g2s[i].shard[d:]
		} else if d < 0 {
			g2s[i].shard = append(g2s[i].shard, pending[:-d]...)
			pending = pending[-d:]
		}
	}

	for i := 0; i < sc.ngroup; i++ {
		for j := 0; j < len(g2s[i].shard); j++ {
			config.Shards[g2s[i].shard[j]] = g2s[i].gid
		}
	}

}
func makeTarget(nshard, ngroup int) []int {
	if ngroup == 0 {
		return nil
	}
	arr := make([]int, ngroup)
	r := nshard % ngroup
	for i := 0; i < ngroup; i++ {
		arr[i] = nshard / ngroup
		if i < r {
			arr[i]++
		}
	}
	return arr
}

func (sc *ShardCtrler) group2Shards(config *Config) ([]Groups2Shards, []int) {
	m := map[int][]int{}
	for k := range config.Groups {
		m[k] = []int{}
	}
	for i := 0; i < NShards; i++ {
		m[config.Shards[i]] = append(m[config.Shards[i]], i)
	}
	arr := []Groups2Shards{}
	pending := []int{}
	for k, v := range m {
		if k != 0 {
			arr = append(arr, Groups2Shards{gid: k, shard: v})
		} else {
			pending = append(pending, v...)
		}
	}
	sort.Sort(Key(arr))
	return arr, pending
}

type Groups2Shards struct {
	gid   int
	shard []int
}
type Key []Groups2Shards

func (k Key) Less(i, j int) bool {
	return len(k[i].shard) > len(k[j].shard) || (len(k[i].shard) == len(k[j].shard) && k[i].gid > k[j].gid)
}

func (k Key) Len() int { return len(k) }

func (k Key) Swap(i, j int) { k[i], k[j] = k[j], k[i] }
