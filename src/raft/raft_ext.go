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
	return len(rf.log) - 1
}
