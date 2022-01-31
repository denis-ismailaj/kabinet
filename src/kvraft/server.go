package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardkv"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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

	data        map[string]string
	applyCond   *sync.Cond
	commitEntry raft.ApplyMsg

	pendingOps  map[int]Op
	appliedReqs map[int64]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		*reply = GetReply{
			Err: ErrWrongLeader,
		}
		return
	}

	kv.mu.Lock()
	val, ok := kv.data[args.Key]
	kv.mu.Unlock()

	if !ok {
		*reply = GetReply{
			Err: ErrNoKey,
		}
	}

	*reply = GetReply{
		Err:   OK,
		Value: val,
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		ClerkId: args.ClerkId,
		SeqNr:   args.SeqNr,
		Type:    args.Op,
		Value:   args.Value,
		Key:     args.Key,
	}

	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		*reply = PutAppendReply{
			Err: ErrWrongLeader,
		}
		return
	}

	DPrintf("%d starting %d-%d %v", kv.me, term, index, op)

	kv.mu.Lock()
	for {
		kv.applyCond.Wait()

		if val, exists := kv.pendingOps[index]; exists {
			if val == op {
				*reply = PutAppendReply{
					Err: OK,
				}
			} else {
				*reply = PutAppendReply{
					Err: "Sorry",
				}
			}
			break
		}
	}
	kv.mu.Unlock()
}

//
// Kill is called by the tester when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// `me` is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardkv.Put)
	labgob.Register(shardkv.Append)

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxRaftState

	kv.applyCond = sync.NewCond(&kv.mu)

	kv.data = make(map[string]string)
	kv.appliedReqs = map[int64]int{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.pendingOps = make(map[int]Op)

	go func() {
		for i := range kv.applyCh {
			if !i.CommandValid {
				continue
			}

			kv.mu.Lock()

			if i.CommandIndex <= kv.commitEntry.CommandIndex {
				kv.mu.Unlock()
				continue
			}

			op := i.Command.(Op)
			kv.pendingOps[i.CommandIndex] = op

			if kv.appliedReqs[op.ClerkId] < op.SeqNr {
				DPrintf("Updating appliedReqs for %d to %d", op.ClerkId, op.SeqNr)
				kv.appliedReqs[op.ClerkId] = op.SeqNr

				if op.Type == shardkv.Append {
					kv.data[op.Key] = kv.data[op.Key] + op.Value
				} else if op.Type == shardkv.Put {
					kv.data[op.Key] = op.Value
				}
			} else {
				DPrintf("Ignoring req %d for %d, already processed %d", op.SeqNr, op.ClerkId, kv.appliedReqs[op.ClerkId])
			}

			kv.commitEntry = i
			kv.mu.Unlock()

			kv.applyCond.Broadcast()
		}
	}()

	return kv
}
