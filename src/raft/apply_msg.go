package raft

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) updateCommitIndex(newIndex int) {
	// Send an ApplyMessage for each commitIndex increment
	for i := rf.commitIndex + 1; i <= newIndex; i++ {
		DPrintf("%d updated commitIndex to %d with command %v\n", rf.me, i, rf.log[i].Command)

		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
	}

	rf.commitIndex = newIndex
}
