package raft

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// For other kinds of messages (e.g., snapshots) on the applyCh,
// set CommandValid to false.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) notifyUpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		DPrintf("%d updated lastApplied to %d\n", rf.me, rf.lastApplied)

		entry := rf.log[rf.lastApplied]

		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
	}
}
