package raft

func (rf *Raft) updateFollower(i int, term int) {
	rf.mu.Lock()

	nextIndex := rf.nextIndex[i]

	if nextIndex <= rf.lastSnapshotIndex {
		DPrintf("%d Leader sending snapshot to follower %d because nextIndex %d lastSnapshot %d\n", rf.me, i, nextIndex, rf.lastSnapshotIndex)
		rf.mu.Unlock()
		rf.sendSnapshot(i, term)
		return
	}

	// Get all entries that aren't replicated (if there are any).
	var entries []Entry
	for i := nextIndex; i <= rf.lastLogIndex(); i++ {
		entries = append(entries, rf.log[i])
	}

	args := AppendEntriesArgs{
		Term:              term,
		LeaderId:          rf.me,
		PrevLogIndex:      nextIndex - 1,
		PrevLogTerm:       rf.termForEntry(nextIndex - 1),
		Entries:           entries,
		LeaderCommitIndex: rf.commitIndex,
	}

	rf.mu.Unlock()

	// Handle reply if RPC succeeds, otherwise ignore
	reply := new(AppendEntriesReply)
	ok := rf.peers[i].Call("Raft.AppendEntries", &args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()

	// Ignore response if the server has moved on.
	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		return
	}

	if ok := rf.checkTerm(reply.Term); !ok || rf.status != Leader {
		rf.mu.Unlock()
		return
	}

	// If nextIndex has already been changed, then some other RPC must have updated it already.
	if rf.nextIndex[i] != nextIndex {
		rf.mu.Unlock()
		return
	}

	// Handle failure
	if !reply.Success {
		DPrintf("%d Leader failed to send AE %d-%d to %d with last entry %d-%d.\n", rf.me, args.PrevLogTerm, args.PrevLogIndex, i, reply.LastLogTerm, reply.LastLogIndex)

		// Try again with a different index.
		if rf.termForEntry(reply.LastLogIndex) == reply.LastLogTerm || reply.LastLogIndex <= rf.lastSnapshotIndex {
			DPrintf("%d Leader found term of index %d of %d to be %d same as mine %d.\n", rf.me, reply.LastLogIndex, i, reply.LastLogTerm, rf.termForEntry(reply.LastLogIndex))

			rf.nextIndex[i] = reply.LastLogIndex + 1
		} else {
			DPrintf("%d Leader found term of index %d of %d to be %d different from mine %d.\n", rf.me, reply.LastLogIndex, i, reply.LastLogTerm, rf.termForEntry(reply.LastLogIndex))

			var termToSkip int
			if reply.LastLogIndex > rf.lastLogIndex() {
				termToSkip = args.Term
			} else {
				termToSkip = rf.termForEntry(reply.LastLogIndex)
			}

			DPrintf("%d Leader skipping term %d for %d.\n", rf.me, termToSkip, i)

			rf.skipTermForFollower(i, termToSkip)
		}

		DPrintf("%d Leader set nextIndex for %d to %d.\n", rf.me, i, rf.nextIndex[i])

		rf.mu.Unlock()
		return
	}

	if reply.LastLogIndex > rf.lastLogIndex() {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = rf.lastLogIndex()
	} else {
		rf.nextIndex[i] = reply.LastLogIndex + 1
		rf.matchIndex[i] = reply.LastLogIndex
	}

	rf.mu.Unlock()

	rf.refreshCommitIndex()
}

func (rf *Raft) skipTermForFollower(i int, term int) {
	for k := rf.lastLogIndex(); k > 0; k-- {
		if rf.log[k].Term < term {
			rf.nextIndex[i] = k + 1
		}
	}
}
