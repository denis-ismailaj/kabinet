package raft

import "time"

// The dispatcher go routine sends heartbeats
func (rf *Raft) dispatcher() {
	for rf.killed() == false {
		rf.mu.Lock()

		// If the server is not a leader, wait until the status has changed to check again.
		if rf.status != Leader {
			rf.statusCond.Wait()

			rf.mu.Unlock()
			continue
		}

		// Saving heartbeat term while under the same lock as the status check, because otherwise
		// it could happen that we arrive at the loop below as a leader, suddenly become a follower on a new term,
		// and the heartbeat sends over the new term making it seem like there are two leaders.
		term := rf.currentTerm

		rf.mu.Unlock()

		DPrintf("%d Leader dispatching heartbeats on term %d\n", rf.me, term)

		rf.dispatchAppendEntries(term)

		// Wait some time before sending again
		time.Sleep(120 * time.Millisecond)
	}
}

func (rf *Raft) dispatchAppendEntries(term int) {
	for i := range rf.peers {
		if i != rf.me {
			go rf.updateFollower(i, term)
		}
	}
}
