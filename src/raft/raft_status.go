package raft

import "time"

type Status int64

const (
	Leader    Status = 69
	Candidate Status = 44
	Follower  Status = 420
)

func (rf *Raft) RevertToFollower(term int) {
	// Assumes a locked raft instance

	if rf.currentTerm != term {
		DPrintf("%d reverting to follower on term %d\n", rf.me, rf.currentTerm)
		rf.currentTerm = term
	}

	rf.status = Follower
	rf.votedFor = nil
	rf.votesReceived = 0

	rf.ResetElectionTimer()
}

func (rf *Raft) ConvertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.status = Candidate

	rf.votedFor = &rf.me
	rf.votesReceived = 1

	rf.ResetElectionTimer()

	DPrintf("%d starting election on term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) BecomeLeader() {
	// Assumes a locked raft instance
	rf.status = Leader
}

func (rf *Raft) ResetElectionTimer() {
	// Assumes a locked raft instance
	rf.electionTimeout = randomIntInRange(250, 400)
	rf.lastHeartbeatTimestamp = time.Now()
}
