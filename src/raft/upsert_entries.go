package raft

// Insert or update follower's log entries
func (rf *Raft) upsertEntries(entries []Entry) {
	if len(entries) == 0 {
		return
	}

	for _, v := range entries {
		DPrintf("%d Follower logging entry %d-%d command %v\n", rf.me, v.Term, v.Index, v.Command)
		rf.log[v.Index] = v
	}

	// If there are old entries with higher indices than the ones supplied by the leader, delete them.
	deleteStart := entries[len(entries)-1].Index + 1

	// ...but not the ones that have been committed already
	if deleteStart <= rf.commitIndex {
		deleteStart = rf.commitIndex + 1
	}

	for i := deleteStart; true; i++ {
		_, exists := rf.log[i]
		if exists {
			delete(rf.log, i)
			DPrintf("%d Follower deleted stale index %d, last index now is %d\n", rf.me, i, rf.lastLogIndex())
		} else {
			break
		}
	}

	// Changes to the log need to be persisted.
	rf.persist()
}

// Update follower's commitIndex to that of the leader
// Note: Some committed entries may not be here yet, so in that case set the commit index to the last log entry.
func (rf *Raft) advanceCommitIndex(leaderCommitIndex int) {
	rf.mu.Lock()

	if leaderCommitIndex > rf.commitIndex {
		if leaderCommitIndex < rf.lastLogIndex() {
			rf.commitIndex = leaderCommitIndex
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
	}

	rf.mu.Unlock()

	rf.notifyUpdateCommitIndex()
}
