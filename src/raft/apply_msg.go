package raft

import (
	"sync/atomic"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// For other kinds of messages (e.g., snapshots) on the applyCh,
// set CommandValid to false.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) notifyUpdateCommitIndex() {
	// Only apply commits if another coroutine isn't already doing it
	if !atomic.CompareAndSwapUint32(&rf.applyLock, 0, 1) {
		return
	}
	defer atomic.StoreUint32(&rf.applyLock, 0)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		DPrintf("%d updated lastApplied to %d\n", rf.me, rf.lastApplied)

		entry := rf.log[rf.lastApplied]

		rf.mu.Unlock()
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		rf.mu.Lock()
	}
}

// Update leader's commit index
func (rf *Raft) refreshCommitIndex() {
	rf.mu.Lock()

	for i := rf.commitIndex + 1; i <= rf.lastLogIndex(); i++ {
		// Only log entries from the leader’s current term are committed by counting replicas;
		// once an entry from the current term has been committed in this way,
		// then all prior entries are committed indirectly because of the Log Matching Property.
		if rf.termForEntry(i) == rf.currentTerm && rf.IsIndexCommitted(i) {
			rf.commitIndex = i
			DPrintf("%d found entry %d-%d from it's current term %d to be committed.\n", rf.me, rf.termForEntry(i), i, rf.currentTerm)
		}
	}

	rf.mu.Unlock()

	rf.notifyUpdateCommitIndex()
}
