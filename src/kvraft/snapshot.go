package kvraft

import (
	"6.824/raft"
	"log"
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
}

func (kv *KVServer) trimmer() {
	for {
		time.Sleep(10 * time.Millisecond)

		if kv.maxRaftState != -1 && kv.persister.RaftStateSize() > kv.maxRaftState/2 {
			kv.mu.Lock()
			log.Printf("%d trimming logs at %d\n", kv.me, kv.latestRaftIndex)
			state := kv.getStateBytes()

			kv.rf.Snapshot(kv.latestRaftIndex, state)
			kv.mu.Unlock()
		}
	}
}
