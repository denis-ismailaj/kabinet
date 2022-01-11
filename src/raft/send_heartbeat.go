package raft

import "fmt"

func (rf *Raft) sendHeartbeat(i int, term int) {
	rf.mu.Lock()

	// Retrieving the last known replicated entry for the server.
	matchIndex := rf.matchIndex[i]

	if matchIndex < rf.lastSnapshotIndex {
		DPrintf("%d Leader sending snapshot to follower %d because matchIndex %d lastSnapshot %d\n", rf.me, i, matchIndex, rf.lastSnapshotIndex)
		rf.mu.Unlock()
		rf.sendSnapshot(i, term)
		return
	}

	// Find the term associated with the match index.
	matchTerm := 0
	if matchIndex > 0 {
		matchTerm = rf.log[matchIndex].Term
	}

	// Get all entries that aren't replicated (if there are any).
	var entries []Entry
	for i := matchIndex + 1; i <= rf.lastLogIndex(); i++ {
		entries = append(entries, rf.log[i])
	}

	args := AppendEntriesArgs{
		Term:              term,
		LeaderId:          rf.me,
		PrevLogIndex:      matchIndex,
		PrevLogTerm:       matchTerm,
		Entries:           entries,
		LeaderCommitIndex: rf.commitIndex,
	}

	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.peers[i].Call("Raft.AppendEntries", &args, reply)

	if !ok {
		// Ignore failed heartbeat. Just wait for another to be sent.
		return
	}

	rf.mu.Lock()

	// Ignore response if the server has moved on.
	if rf.currentTerm > term {
		rf.mu.Unlock()
		return
	}

	// Convert to follower if term is out of date.
	if reply.Term > rf.currentTerm {
		rf.BecomeFollowerOrUpdateTerm(reply.Term)

		rf.mu.Unlock()
		return
	}

	// Handle failure
	if !reply.Success {
		DPrintf("%d Leader failed to send AppendEntries with PrevLogIndex %d to %d.\n", rf.me, args.PrevLogIndex, i)

		if rf.matchIndex[i] == 0 {
			panic(fmt.Sprintf(
				"Follower %d with term %d didn't accept entry 1 from %d with term %d\n",
				rf.me, rf.currentTerm, args.LeaderId, args.Term,
			))
		}

		// Try again with a smaller index
		rf.matchIndex[i] = rf.matchIndex[i] - 1

		rf.mu.Unlock()
		return
	}

	// If new entries were sent, update the follower's matchIndex
	if rf.matchIndex[i] != reply.LastLogIndex {
		DPrintf("%d Leader setting matchIndex to %d for follower %d\n", rf.me, reply.LastLogIndex, i)
		rf.matchIndex[i] = reply.LastLogIndex

		rf.mu.Unlock()
		rf.refreshCommitIndex()
		return
	}

	rf.mu.Unlock()
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
