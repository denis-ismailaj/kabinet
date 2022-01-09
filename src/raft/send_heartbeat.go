package raft

import "fmt"

func (rf *Raft) sendHeartbeat(i int, term int) {
	rf.mu.Lock()

	// Retrieving the last known replicated entry for the server.
	matchIndex := rf.matchIndex[i]

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
	defer rf.mu.Unlock()

	// Ignore response if the server has moved on.
	if rf.currentTerm > term {
		return
	}

	// Convert to follower if term is out of date.
	if reply.Term > rf.currentTerm {
		rf.BecomeFollowerOrUpdateTerm(reply.Term)
		return
	}

	if reply.Success {
		DPrintf("%d Leader setting matchIndex to %d for follower %d\n", rf.me, reply.LastLogIndex, i)
		rf.matchIndex[i] = reply.LastLogIndex

		rf.refreshCommitIndex()
	} else {
		DPrintf("%d Leader failed to send AppendEntries with PrevLogIndex %d to %d.\n", rf.me, args.PrevLogIndex, i)
		if rf.matchIndex[i] > 0 {
			// Try again with a smaller index
			rf.matchIndex[i] = rf.matchIndex[i] - 1
		} else {
			panic(fmt.Sprintf(
				"Follower %d with term %d didn't accept entry 1 from %d with term %d\n",
				rf.me, rf.currentTerm, args.LeaderId, args.Term,
			))
		}
	}
}

// Update leader's commit index
func (rf *Raft) refreshCommitIndex() {
	newIndex := rf.commitIndex

	for rf.IsIndexCommitted(newIndex + 1) {
		newIndex++
	}

	rf.updateCommitIndex(newIndex)
}