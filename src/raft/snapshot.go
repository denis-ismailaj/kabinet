package raft

// CondInstallSnapshot
// A service wants to switch to snapshot. Only do so if Raft doesn't
// have more recent info since it sent the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	DPrintf("%d CondInstall term %d index %d\n", rf.me, lastIncludedTerm, lastIncludedIndex)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex < rf.lastApplied {
		DPrintf("%d refusing CondInstall %d-%d because lastApplied is %d\n", rf.me, lastIncludedTerm, lastIncludedIndex, rf.lastApplied)
		return false
	}

	rf.saveSnapshot(lastIncludedIndex, lastIncludedTerm, snapshot)

	return true
}

// Snapshot
// The service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("%d Service snapshotted index %d\n", rf.me, index)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.saveSnapshot(index, rf.termForEntry(index), snapshot)
}

func (rf *Raft) saveSnapshot(index int, term int, snapshot []byte) {
	DPrintf("%d Saving snapshot with index %d term %d\n", rf.me, index, term)

	// Delete log entries up to the snapshot index
	for i := rf.lastSnapshotIndex; i <= index; i++ {
		delete(rf.log, i)
		DPrintf("%d deleted unneeded index %d because of snapshot.\n", rf.me, i)
	}

	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = term

	rf.lastApplied = index

	// Persist changes to lastSnapshotIndex and lastSnapshotTerm.
	rf.persist()

	rf.persister.SaveSnapshot(snapshot)
}
