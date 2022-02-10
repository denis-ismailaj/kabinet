package kvraft

import (
	"6.824/raft"
	"6.824/shardkv"
	"sync"
)

type Op struct {
	ClerkId int64
	SeqNr   int
	Key     string
	Value   string
	Type    shardkv.OpType
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big

	data map[string]string

	applyCond   *sync.Cond
	appliedReqs map[int64]int
}

func (kv *KVServer) Get(args Op, reply *GetReply) {
	err := kv.SubmitForAgreement(args)

	if err == OK {
		kv.mu.Lock()
		val, exists := kv.data[args.Key]
		kv.mu.Unlock()

		if !exists {
			*reply = GetReply{
				Err: ErrNoKey,
			}
		} else {
			*reply = GetReply{
				Err:   OK,
				Value: val,
			}
		}
	} else {
		*reply = GetReply{
			Err: err,
		}
	}
}

func (kv *KVServer) PutAppend(args Op, reply *PutAppendReply) {
	err := kv.SubmitForAgreement(args)

	*reply = PutAppendReply{
		Err: err,
	}
}

func (kv *KVServer) SubmitForAgreement(args Op) Err {
	DPrintf("%d going to start %v", kv.me, args)
	index, term, isLeader := kv.rf.Start(args)

	if !isLeader {
		return ErrWrongLeader
	}

	DPrintf("%d starting %d-%d %v", kv.me, term, index, args)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	for {
		cTerm, _ := kv.rf.GetState()
		if cTerm > term {
			return "Sorry"
		}

		if kv.appliedReqs[args.ClerkId] >= args.SeqNr {
			return OK
		}

		kv.applyCond.Wait()
	}
}
