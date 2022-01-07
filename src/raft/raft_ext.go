package raft

import "time"

func (rf *Raft) timeUntilPossibleTimeout() time.Duration {
	if rf.lastHeartbeatTimestamp.IsZero() {
		return time.Duration(rf.electionTimeout) * time.Millisecond
	}

	timePassed := time.Now().UnixMilli() - rf.lastHeartbeatTimestamp.UnixMilli()
	timeLeft := rf.electionTimeout - timePassed

	return time.Duration(timeLeft) * time.Millisecond
}

func (rf *Raft) NoHeartbeatIn(milliseconds int64) bool {
	threshold := time.Now().Add(time.Duration(-milliseconds) * time.Millisecond)
	return rf.lastHeartbeatTimestamp.Before(threshold)
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log)
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[rf.lastLogIndex()].Term
}

func (rf *Raft) IsIndexCommitted(index int) bool {
	// Assumes a locked raft instance
	count := 0
	for _, v := range rf.matchIndex {
		if v >= index {
			count++
		}
	}
	return count > len(rf.peers)/2
}

func (rf *Raft) appendLogEntry(entry Entry) int {
	// Assumes a locked raft instance
	index := rf.lastLogIndex() + 1
	rf.log[index] = entry

	return index
}
