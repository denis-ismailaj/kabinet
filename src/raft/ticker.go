package raft

import "time"

// The ticker goroutine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()

		// If the server is a leader, wait until the status has changed before checking again.
		if rf.status == Leader {
			rf.statusCond.Wait()
			rf.mu.Unlock()
			continue
		}

		timeout := rf.electionTimeout
		shouldStartElection := rf.NoHeartbeatIn(timeout)

		rf.mu.Unlock()

		if shouldStartElection {
			DPrintf("%d received no heartbeat in %d milliseconds\n", rf.me, timeout)
			rf.startElection()
		}

		rf.mu.Lock()
		timeToSleep := rf.timeUntilPossibleTimeout()
		rf.mu.Unlock()

		time.Sleep(timeToSleep)
	}
}
