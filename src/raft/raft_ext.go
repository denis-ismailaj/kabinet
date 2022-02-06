package raft

import "time"

// Extension methods for Raft
// These methods simplify some common read-only operations and do not mutate any state.
// All of them assume a locked Raft instance.

func (rf *Raft) timeUntilPossibleTimeout() time.Duration {
	if rf.lastHeartbeat.IsZero() {
		return time.Duration(rf.electionTimeout) * time.Millisecond
	}

	timePassed := time.Now().UnixMilli() - rf.lastHeartbeat.UnixMilli()
	timeLeft := rf.electionTimeout - timePassed

	return time.Duration(timeLeft) * time.Millisecond
}

func (rf *Raft) NoHeartbeatIn(milliseconds int64) bool {
	threshold := time.Now().Add(time.Duration(-milliseconds) * time.Millisecond)
	return rf.lastHeartbeat.Before(threshold)
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastSnapshotIndex + len(rf.log)
}

func (rf *Raft) lastLogTerm() int {
	return rf.termForEntry(rf.lastLogIndex())
}

func (rf *Raft) termForEntry(index int) int {
	if index == rf.lastSnapshotIndex {
		return rf.lastSnapshotTerm
	}
	return rf.log[index].Term
}

func (rf *Raft) IsIndexCommitted(index int) bool {
	count := 0
	for _, v := range rf.matchIndex {
		if v >= index {
			count++
		}
	}
	return count > len(rf.peers)/2
}

func (rf *Raft) canVoteFor(id int) bool {
	return rf.votedFor == nil || *rf.votedFor == id
}
