package kvraft

import (
	"6.824/raft"
	"time"
)

func (kv *KVServer) handleSnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)

	if !ok {
		return
	}

	if msg.SnapshotIndex > kv.latestRaftIndex {
		kv.latestRaftIndex = msg.SnapshotIndex
	}

	kv.restoreFromSnapshotNoCheck(msg.Snapshot)
}

func (kv *KVServer) trimmer() {
	for {
		time.Sleep(200 * time.Millisecond)

		kv.mu.Lock()
		if kv.maxRaftState != -1 && kv.latestRaftIndex > kv.latestTrimRequest && kv.persister.RaftStateSize() > kv.maxRaftState/2 {
			DPrintf("%d trimming logs at %d because %d > %d\n", kv.me, kv.latestRaftIndex, kv.persister.RaftStateSize(), kv.maxRaftState/2)
			state := kv.getStateBytes()

			kv.rf.Snapshot(kv.latestRaftIndex, state)
			kv.latestTrimRequest = kv.latestRaftIndex
		}
		kv.mu.Unlock()
	}
}
