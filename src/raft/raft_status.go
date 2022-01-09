package raft

import "time"

type Status int64

const (
	Leader    Status = 1
	Candidate Status = 2
	Follower  Status = 3
)

func (rf *Raft) BecomeFollowerOrUpdateTerm(term int) {
	// It is safe to call if the server is already a follower

	if rf.status != Follower {
		DPrintf("%d converting to follower on term %d\n", rf.me, term)
		rf.status = Follower
		rf.votedFor = nil
		rf.statusCond.Broadcast()
	}

	rf.currentTerm = term

	// Changes to term and votedFor need to be persisted
	rf.persist()

	// Reset election timer.
	rf.lastHeartbeat = time.Now()
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
}

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status == Leader {
		// already a leader. ignoring silently
		return
	}

	DPrintf("%d self-declared leader on term %d.\n", rf.me, rf.currentTerm)

	rf.status = Leader
	rf.statusCond.Broadcast()
}

func (rf *Raft) ResetElectionTimer() {
	// Assumes a locked raft instance
	rf.electionTimeout = randomIntInRange(300, 400)
	rf.lastHeartbeat = time.Now()
}
