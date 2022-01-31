package kvraft

import (
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardkv"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	return ck
}

// Get fetches the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	DPrintf("clerk getting %s", key)

	args := GetArgs{
		Key: key,
	}

	for {
		reply := GetReply{}

		server := raft.RandomIntInRange(0, int64(len(ck.servers)-1))
		DPrintf("clerk trying %d for %s", server, key)

		_ = ck.servers[server].Call("KVServer.Get", &args, &reply)

		if reply.Err == OK {
			DPrintf("clerk found %s to be %s", key, reply.Value)

			return reply.Value
		} else if reply.Err == ErrNoKey {
			DPrintf("clerk found %s to be missing", key)

			return ""
		}
	}
}

//
// PutAppend
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op shardkv.OpType) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}

	for {
		reply := PutAppendReply{}

		server := raft.RandomIntInRange(0, int64(len(ck.servers)-1))
		_ = ck.servers[server].Call("KVServer.PutAppend", &args, &reply)

		if reply.Err == OK {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, shardkv.Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, shardkv.Append)
}
