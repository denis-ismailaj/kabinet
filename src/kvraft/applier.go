package kvraft

import "6.824/shardkv"

func (kv *KVServer) applier() {
	for i := range kv.applyCh {
		DPrintf("%d apply %v", kv.me, i)

		if !i.CommandValid {
			continue
		}

		// This is a no-op entry.
		// Leader may have been deposed so wake up applier.
		if i.Command == nil {
			kv.applyCond.Broadcast()
			continue
		}

		op := i.Command.(Op)

		kv.mu.Lock()
		if kv.appliedReqs[op.ClerkId] < op.SeqNr {
			DPrintf("%d Updating appliedReqs for %d to %d, was %d", kv.me, op.ClerkId, op.SeqNr, kv.appliedReqs[op.ClerkId])
			kv.appliedReqs[op.ClerkId] = op.SeqNr

			switch op.Type {
			case shardkv.Append:
				kv.data[op.Key] += op.Value
			case shardkv.Put:
				kv.data[op.Key] = op.Value
			}
		} else {
			DPrintf("%d Ignoring req %d for %d, already processed %d", kv.me, op.SeqNr, op.ClerkId, kv.appliedReqs[op.ClerkId])
		}
		kv.mu.Unlock()

		kv.applyCond.Broadcast()
	}
}
