package shardctrler

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	serverRequest map[int64]int64
	committed     map[int]chan Op
	//storeValues   map[string]string
	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	Args         interface{}
	Id           int64
	RequestCount int64
	Operation    string
}

func waitAndCheckOp(chann chan Op, op Op) bool {
	select {
	case commit := <-chann:
		if commit.Id == op.Id && commit.RequestCount == op.RequestCount {
			return false
		} else {
			return true
		}
	case <-time.After(400 * time.Millisecond):
		return true
	}
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader = true
	operation := Op{Args: *args, Id: args.Id, RequestCount: args.RequestCount, Operation: join}
	ind, _, isLeader := sc.rf.Start(operation)
	if isLeader{
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Lock()
		chann, exists := sc.committed[ind]

		if !exists {
			sc.committed[ind] = make(chan Op, 1)
			chann = sc.committed[ind]
		}
		sc.mu.Unlock()
		reply.WrongLeader = waitAndCheckOp(chann, operation)
		//println("Err: ", reply.Err)
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader = true
	operation := Op{Args: *args, Id: args.Id, RequestCount: args.RequestCount, Operation: leave}
	ind, _, isLeader := sc.rf.Start(operation)
	if isLeader{
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Lock()
		chann, exists := sc.committed[ind]

		if !exists {
			sc.committed[ind] = make(chan Op, 1)
			chann = sc.committed[ind]
		}
		sc.mu.Unlock()
		reply.WrongLeader = waitAndCheckOp(chann, operation)
		//println("Err: ", reply.Err)
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = true
	operation := Op{Args: *args, Id: args.Id, RequestCount: args.RequestCount, Operation: move}
	ind, _, isLeader := sc.rf.Start(operation)
	if isLeader{
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Lock()
		chann, exists := sc.committed[ind]

		if !exists {
			sc.committed[ind] = make(chan Op, 1)
			chann = sc.committed[ind]
		}
		sc.mu.Unlock()
		reply.WrongLeader = waitAndCheckOp(chann, operation)
		//println("Err: ", reply.Err)
	}
}
func (sc *ShardCtrler) replyLatest(num int) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if num >= len(sc.configs) || num < 0 {
		return sc.configs[len(sc.configs)-1]
	} 
	return sc.configs[num]

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.WrongLeader = true
	operation := Op{Args: *args, Id: args.Id, RequestCount: args.RequestCount, Operation: query}
	ind, _, isLeader := sc.rf.Start(operation)
	if isLeader{
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Lock()
		chann, exists := sc.committed[ind]

		if !exists {
			sc.committed[ind] = make(chan Op, 1)
			chann = sc.committed[ind]
		}
		sc.mu.Unlock()
		reply.WrongLeader = waitAndCheckOp(chann, operation)
		//println("Err: ", reply.Err)
		reply.Config  = sc.replyLatest(args.Num)
	}
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

func (sc *ShardCtrler) getShards() [10] int {
	return sc.configs[len(sc.configs)-1].Shards
}

func (sc *ShardCtrler) getGroups() map[int][]string {
	return sc.configs[len(sc.configs)-1].Groups
}

func (sc *ShardCtrler) deletes(found int, last int, gid int) {
	for i := 0; i < len(sc.configs[last].Shards); i++ {
		if sc.configs[last].Shards[i] == gid {
			sc.configs[last].Shards[i] = found
		}
	}
	delete(sc.configs[last].Groups, gid)
}

func (sc *ShardCtrler) handleOp(op string, args interface{}) {
	con := Config{Num: len(sc.configs), Groups: map[int][]string{}, Shards: sc.getShards()}

	for k, v := range sc.getGroups() {
		con.Groups[k] = v
	}

	sc.configs = append(sc.configs, con)
	
	last := len(sc.configs)-1
	if op == move {
		moveArg := args.(MoveArgs)
		sc.configs[last].Shards[moveArg.Shard]= moveArg.GID
	} else if op == leave {
		leaveArg := args.(LeaveArgs)
		shards := make(map[int][]int)
	 	tmp := 0
		for g := range sc.configs[last].Groups {
			found := true
			for _, deleted := range leaveArg.GIDs {
				if g != deleted { continue }
				found = false	
			}
			if found {
				tmp = g
				break
			}
		}
		for _, gid := range leaveArg.GIDs {
			sc.deletes(tmp, last, gid)
		}	
		for g := range sc.configs[last].Groups {
			shards[g] = []int{}
		}
		for shard, g := range sc.configs[last].Shards {
			shards[g] = append(shards[g], shard)
		}

		if len(sc.configs[last].Groups) != 0 {
			mid := NShards / len(sc.configs[last].Groups)
			num := calcNum(shards, mid)
	
			for i := 0; i < num; i++ {
				currMin := -1
				min := 0
				for k, v := range shards {
					if currMin > len(v) || currMin == -1 {
						currMin = len(v)
						min = k
					}
				}
				currMax := -1
				max := 0
				for k, v := range shards {
					if currMax < len(v) {
						currMax = len(v)
						max = k
					}
				}
				sc.configs[last].Shards[shards[max][0]] = min
				shards[min] = append(shards[min], shards[max][0])
				shards[max] = shards[max][1:]
			}
	
		} else {
			for i := 0; i < len(sc.configs[last].Shards); i++ {
				sc.configs[last].Shards[i] = 0
			}
		} 

	} else if op == join {
		joinArg := args.(JoinArgs)
		for gid, servers := range joinArg.Servers {
			sc.configs[last].Groups[gid] = servers
			for i := 0; i < len(sc.configs[last].Shards); i++ {
				if  sc.configs[last].Shards[i] == 0 {
					sc.configs[last].Shards[i] = gid
				}
			}
		}
		sc.joinHandler(last)			
	}
}

func calcNum(shards map[int][]int, mid int) int {
	num := 0
	for _, shard := range shards {
		if mid < len(shard) {
			num = num + len(shard) - mid
		} else {
			continue
		}
	}
	return num
}

func (sc *ShardCtrler) joinHandler(last int) {
	shards := make(map[int][]int)

	for g := range sc.configs[last].Groups {
		shards[g] = []int{}
	}
	for shard, g := range sc.configs[last].Shards {
		shards[g] = append(shards[g], shard)
	}

	if len(sc.configs[last].Groups) != 0 {
		mid := NShards / len(sc.configs[last].Groups)
		num := calcNum(shards, mid)

		for i := 0; i < num; i++ {
			currMax := -1
			max := 0
			for k, v := range shards {
				if currMax < len(v){
					currMax = len(v)
					max = k
				}
			}
			currMin := -1
			min := 0
			for k, v := range shards {
				if currMin > len(v) || currMin == -1{
					currMin = len(v)
					min = k
				}
			}

			sc.configs[last].Shards[shards[max][0]] = min
			shards[min] = append(shards[min], shards[max][0])

			shards[max] = shards[max][1:]
		}

	} else {
		for i := range sc.configs[last].Shards {
			sc.configs[last].Shards[i] = 0
		}
	} 

}

func (sc *ShardCtrler) checkCommits() {
	for i := 0; ; i++ {

		applyMsg := <-sc.applyCh

		sc.mu.Lock()
		_, ok := sc.serverRequest[(applyMsg.Command.(Op).Id)]
		if !ok || applyMsg.Command.(Op).RequestCount > sc.serverRequest[(applyMsg.Command.(Op).Id)] {
			sc.serverRequest[(applyMsg.Command.(Op).Id)] = applyMsg.Command.(Op).RequestCount
			if applyMsg.Command.(Op).Operation != get && applyMsg.Command.(Op).Operation != query {
				sc.handleOp(applyMsg.Command.(Op).Operation, applyMsg.Command.(Op).Args)
			}
		}
		chann, exists := sc.committed[applyMsg.CommandIndex]
		sc.mu.Unlock()
		if exists {
			select {
			case <-chann:
			default:
			}
		} else {
			sc.mu.Lock()
			sc.committed[applyMsg.CommandIndex] = make(chan Op, 1)
			chann = sc.committed[applyMsg.CommandIndex]
			sc.mu.Unlock()
		}

		chann <- applyMsg.Command.(Op)
	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs           = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})

	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})

	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})

	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})

	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf      = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.serverRequest = make(map[int64]int64)
	sc.committed     = make(map[int]chan Op)
	//sc.storeValues   = make(map[string]string)
	go sc.checkCommits()
	return sc
}
