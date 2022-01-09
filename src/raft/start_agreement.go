package raft

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != Leader {
		return 0, 0, false
	}

	index := rf.createLogEntry(command)

	// Relying on the change to be propagated with future heartbeats.

	return index, rf.currentTerm, true
}

func (rf *Raft) createLogEntry(command interface{}) int {
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   rf.lastLogIndex() + 1,
	}

	DPrintf("%d Leader logging entry with index %d term %d command %v\n", rf.me, entry.Index, entry.Term, entry.Command)

	// Insert entry to log
	rf.log[entry.Index] = entry
	rf.persist()

	// Update own matchIndex
	rf.matchIndex[rf.me] = entry.Index

	return entry.Index
}
