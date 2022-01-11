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

func (rf *Raft) notifyUpdateCommitIndex() {
	rf.mu.Lock()
	oldIndex := rf.lastApplied
	newIndex := rf.commitIndex
	rf.mu.Unlock()

	// Send an ApplyMessage for each commitIndex increment
	for i := oldIndex + 1; i <= newIndex; i++ {
		rf.mu.Lock()
		command := rf.log[i].Command
		DPrintf("%d updated commitIndex to %d with command %v nc %d\n", rf.me, i, command, rf.lastApplied)
		rf.mu.Unlock()

		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: i,
		}

		rf.mu.Lock()
		DPrintf("%d updated lastApplied to %d\n", rf.me, i)
		rf.lastApplied = i
		rf.mu.Unlock()
	}
}
