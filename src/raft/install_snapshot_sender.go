package raft

func (rf *Raft) sendSnapshot(i int, term int) {
	rf.mu.Lock()

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	ok := rf.peers[i].Call("Raft.InstallSnapshot", &args, reply)

	if !ok {
		DPrintf("%d Leader failed sending snapshot %d-%d follower %d\n", rf.me, args.LastIncludedTerm, args.LastIncludedIndex, i)
		// Ignore failed message. Just wait for another to be sent.
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Ignore response if the server has moved on.
	if rf.currentTerm > term {
		return
	}

	if rf.checkTerm(reply.Term); rf.status != Leader {
		return
	}

	// Reset nextIndex to default
	rf.nextIndex[i] = rf.lastLogIndex() + 1
}
