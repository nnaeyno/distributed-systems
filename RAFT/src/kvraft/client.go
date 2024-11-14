package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prevLeaderId int
	lock         sync.Mutex
	id           int64
	requestCount int64
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
	// You'll have to add code here.
	ck.prevLeaderId = 0
	ck.id = nrand()
	ck.requestCount = 0
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
	args := GetArgs{}
	ck.lock.Lock()
	ck.requestCount++
	currLeader := ck.prevLeaderId
	args.Key = key
	args.Id = ck.id
	args.Op = get
	args.RequestCount = ck.requestCount
	len := len(ck.servers)
	ck.lock.Unlock()
	// You will have to modify this function.
	ok := false
	foundLeader := true
	for !ok || !foundLeader {
		reply := GetReply{}
		ok = ck.servers[currLeader].Call("KVServer.Get", &args, &reply)
		foundLeader = true
		if reply.Err == ErrWrongLeader || !ok {
			currLeader = (currLeader + 1) % len
			foundLeader = false
		} else {
			ck.lock.Lock()
			ck.prevLeaderId = currLeader
			ck.lock.Unlock()
			return reply.Value
		}
	}
	return ErrNoKey
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
	
	ck.lock.Lock()
	ck.requestCount++
	currLeader := ck.prevLeaderId
	args.Key = key
	args.Value = value
	args.Op = op
	args.Id = ck.id
	args.RequestCount = ck.requestCount
	len := len(ck.servers)
	ck.lock.Unlock()
	ok := false
	foundLeader := true
	for !ok || !foundLeader {
		reply := PutAppendReply {}
		ok = ck.servers[currLeader].Call("KVServer.PutAppend", &args, &reply)
		foundLeader = true
		if reply.Err == ErrWrongLeader || !ok {
			currLeader = (currLeader + 1) % len
			foundLeader = false
		} 		
	}
	ck.lock.Lock()
	ck.prevLeaderId = currLeader
	ck.lock.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, app)
}
