package kvraft

import (
	"bytes"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestCount int64
	Id           int64
	Operation    string
	Key          string
	Value        string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	serverRequest map[int64]int64
	committed     map[int]chan Op
	storeValues   map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Id:           args.Id,
		RequestCount: args.RequestCount,
		Key:          args.Key,
		Operation:    args.Op,
	}
	ind, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		kv.mu.Lock()
		chann, exists := kv.committed[ind]
		if !exists {
			kv.committed[ind] = make(chan Op, 1)
			chann = kv.committed[ind]
		}
		kv.mu.Unlock()
		reply.Err = waitAndCheckOp(chann, op)
		if reply.Err == OK {
			kv.mu.Lock()
			reply.Value = kv.storeValues[args.Key]
			kv.mu.Unlock()
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Id:           args.Id,
		RequestCount: args.RequestCount,
		Key:          args.Key,
		Operation:    args.Op,
		Value:        args.Value,
	}
	ind, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		kv.mu.Lock()
		chann, exists := kv.committed[ind]

		if !exists {
			kv.committed[ind] = make(chan Op, 1)
			chann = kv.committed[ind]
		}
		kv.mu.Unlock()
		reply.Err = waitAndCheckOp(chann, op)
		//println("Err: ", reply.Err)
	}

}

func waitAndCheckOp(chann chan Op, op Op) Err {
	select {
	case commit := <-chann:
		if commit.Id == op.Id && commit.RequestCount == op.RequestCount {
			return OK
		} else {
			return ErrWrongLeader
		}
	case <-time.After(400 * time.Millisecond):
		return ErrWrongLeader
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

func (kv *KVServer) checkCommits() {
	for i := 0; ; i++ {

		applyMsg := <-kv.applyCh

		if applyMsg.SnapshotValid && !applyMsg.CommandValid{
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := labgob.NewDecoder(r)
			kv.mu.Lock()
			d.Decode(&kv.storeValues)
			d.Decode(&kv.serverRequest)
			kv.mu.Unlock()
			continue
		}
	

		kv.mu.Lock()
		_, ok := kv.serverRequest[(applyMsg.Command.(Op).Id)]
		if !ok || applyMsg.Command.(Op).RequestCount > kv.serverRequest[(applyMsg.Command.(Op).Id)] {
			if applyMsg.Command.(Op).Operation == app {
				kv.storeValues[applyMsg.Command.(Op).Key] += applyMsg.Command.(Op).Value
			} else if applyMsg.Command.(Op).Operation == put {
				kv.storeValues[applyMsg.Command.(Op).Key] = applyMsg.Command.(Op).Value
			}
			kv.serverRequest[(applyMsg.Command.(Op).Id)] = applyMsg.Command.(Op).RequestCount

		}
		chann, exists := kv.committed[applyMsg.CommandIndex]
		kv.mu.Unlock()
		if exists {
			select {
			case <-chann:
			default:
			}
		} else {
			kv.mu.Lock()
			kv.committed[applyMsg.CommandIndex] = make(chan Op, 1)
			chann = kv.committed[applyMsg.CommandIndex]
			kv.mu.Unlock()
		}

		chann <- applyMsg.Command.(Op)
		kv.checkSnapshots(applyMsg.CommandIndex)
	}

}
func (kv *KVServer) checkSnapshots(ind int) {
	if kv.maxraftstate != -1 && kv.rf.Size() > kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		kv.mu.Lock()
		e.Encode(kv.storeValues)
		e.Encode(kv.serverRequest)
		kv.mu.Unlock()
		go kv.rf.Snapshot(ind, w.Bytes())
	}
}



func (kv *KVServer) setUp(persister *raft.Persister) {
	kv.serverRequest = make(map[int64]int64)
	kv.committed = make(map[int]chan Op)
	kv.storeValues = make(map[string]string)
	snapshot := persister.ReadSnapshot()
	if  len(snapshot) != 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		d.Decode(&kv.storeValues)
		d.Decode(&kv.serverRequest) 
	}

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
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.setUp(persister)
	go kv.checkCommits()
	return kv
}
