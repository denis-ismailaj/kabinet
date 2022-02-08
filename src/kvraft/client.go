package kvraft

import (
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardkv"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	mu         sync.Mutex
	id         int64
	seqNr      int
	lastLeader *int64
}

func randInt() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = randInt()
	return ck
}

// Get fetches the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.seqNr++
	args := Op{
		ClerkId: ck.id,
		SeqNr:   ck.seqNr,
		Key:     key,
		Type:    shardkv.Get,
	}
	ck.mu.Unlock()

	for {
		reply := GetReply{}

		leader := ck.getLeader()
		ok := ck.servers[leader].Call("KVServer.Get", args, &reply)

		if reply.Err == OK {
			DPrintf("clerk found %s @ %d to be %s", key, leader, reply.Value)

			return reply.Value
		} else if reply.Err == ErrNoKey {
			DPrintf("clerk found %s @ %d to be missing", key, leader)

			return ""
		} else if reply.Err == ErrWrongLeader || !ok {
			ck.invalidateLeader()
		} else {
			DPrintf("clerk %d requiring %s from %d got error %s", ck.id, key, leader, reply.Err)
		}
	}
}

//
// PutAppend
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, opType shardkv.OpType) {
	ck.mu.Lock()
	ck.seqNr++
	args := Op{
		ClerkId: ck.id,
		SeqNr:   ck.seqNr,
		Key:     key,
		Value:   value,
		Type:    opType,
	}
	ck.mu.Unlock()

	for {
		reply := PutAppendReply{}

		leader := ck.getLeader()
		ok := ck.servers[leader].Call("KVServer.PutAppend", args, &reply)

		if reply.Err == OK {
			break
		} else if reply.Err == ErrWrongLeader || !ok {
			ck.invalidateLeader()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, shardkv.Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, shardkv.Append)
}

func (ck *Clerk) getLeader() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	if ck.lastLeader == nil {
		randChoice := raft.RandomIntInRange(0, int64(len(ck.servers)-1))
		ck.lastLeader = &randChoice
	}

	return *ck.lastLeader
}

func (ck *Clerk) invalidateLeader() {
	ck.mu.Lock()
	ck.lastLeader = nil
	ck.mu.Unlock()
}
