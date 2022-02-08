package raft

import (
	"time"
)

type Status int64

const (
	Leader    Status = 1
	Candidate Status = 2
	Follower  Status = 3
)

func (rf *Raft) checkTerm(term int) bool {
	if term < rf.currentTerm {
		return false
	}

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = nil

		if rf.status != Follower {
			DPrintf("%d converting to follower on term %d\n", rf.me, term)
			rf.status = Follower
			rf.statusCond.Broadcast()
		}

		// Changes to term and votedFor need to be persisted
		rf.persist()

		rf.ResetElectionTimer()
	}

	return true
}

func (rf *Raft) ConvertToCandidate() {
	// To begin an election, a follower increments its current term and transitions to candidate state (§5.2).
	rf.currentTerm++
	rf.status = Candidate

	// It then votes for itself (§5.2).
	rf.votedFor = &rf.me

	// Each candidate restarts its randomized election timeout at the start of an election,
	// and it waits for that timeout to elapse before starting the next election;
	// this reduces the likelihood of another split vote in the new election (§5.2).
	rf.ResetElectionTimer()

	// Changes to term and votedFor need to be persisted.
	rf.persist()
}

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status == Leader {
		// already a leader. ignoring silently
		return
	}

	DPrintf("%d self-declared leader on term %d.\n", rf.me, rf.currentTerm)

	rf.ResetFollowerIndices()

	rf.status = Leader
	rf.statusCond.Broadcast()

	// The Leader Completeness Property guarantees that a leader has all committed entries,
	// but at the start of its term, it may not know which those are.
	// To find out, it needs to commit an entry from its term.
	// Raft handles this by having each leader commit a blank no-op entry into the log at the start of its term.
	rf.createLogEntry(nil)
}

func (rf *Raft) ResetFollowerIndices() {
	nextIndex := rf.lastLogIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = nextIndex
	}
}

func (rf *Raft) ResetElectionTimer() {
	// Assumes a locked raft instance
	rf.electionTimeout = RandomIntInRange(300, 400)
	rf.lastHeartbeat = time.Now()
}
