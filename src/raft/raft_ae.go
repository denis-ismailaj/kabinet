package raft

import "time"

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

	if rf.status != Leader {
		defer rf.mu.Unlock()
		return 0, 0, false
	}

	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   rf.lastLogIndex() + 1,
	}
	rf.appendLogEntry(entry)

	rf.matchIndex[rf.me] = entry.Index

	rf.mu.Unlock()

	DPrintf("%d Leader logging entry with index %d term %d command %v\n", rf.me, entry.Index, entry.Term, entry.Command)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.sendAppendEntry(i, entry.Index)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return entry.Index, rf.currentTerm, true
}

func (rf *Raft) sendAppendEntry(i int, index int) {
	rf.mu.Lock()
	firstEntry := rf.log[index]

	prevLogTerm := 0

	if index > 0 {
		prevLogTerm = rf.log[index-1].Term
	}

	entries := []Entry{}

	latestIndex := rf.matchIndex[rf.me]

	for i := index; i <= latestIndex; i++ {
		entries = append(entries, rf.log[i])
	}

	args := AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      firstEntry.Index - 1,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}

	rf.peers[i].Call("Raft.AppendEntries", &args, reply)

	rf.mu.Lock()

	if rf.status != Leader {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		DPrintf("%d Leader setting matchindex to %d for follower %d\n", rf.me, latestIndex, i)
		rf.matchIndex[i] = latestIndex

		if rf.IsIndexCommitted(firstEntry.Index) && firstEntry.Index > rf.commitIndex {
			DPrintf("%d Leader setting commitIndex to %d with command %v\n", rf.me, firstEntry.Index, rf.log[firstEntry.Index].Command)

			for i := rf.commitIndex; i <= firstEntry.Index; i++ {
				rf.applyChan <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
			}
			rf.commitIndex = firstEntry.Index
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.RevertToFollower(reply.Term)
		} else if rf.matchIndex[i] < index {
			if index <= reply.LastLogIndex+1 {
				DPrintf("%d Leader failed to append entry index %d to follower %d. Trying again with index %d...\n", rf.me, index, i, index-1)

				defer rf.sendAppendEntry(i, index-1)
			} else {
				DPrintf("%d Leader failed to append entry index %d to follower %d. Trying again with index %d...\n", rf.me, index, i, reply.LastLogIndex+1)

				defer rf.sendAppendEntry(i, reply.LastLogIndex+1)
			}
		}
	}

	rf.mu.Unlock()
}

// AppendEntriesArgs
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []Entry
	LeaderCommitIndex int
}

// AppendEntriesReply
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term         int
	Success      bool
	LastLogIndex int
}

//
// AppendEntries RPC handler.
//
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn't contain an entry at prevLogIndex
// 	whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// 	but different terms), delete the existing entry and all that
// 	follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex,
//	set commitIndex = min(leaderCommit, index of last new entry)
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf("%d ignoring entry from %d with old term %d. Current term is %d.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false

		return
	}

	rf.lastHeartbeatTimestamp = time.Now()
	if rf.status != Follower {
		rf.RevertToFollower(args.Term)
	}

	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%d ignoring log entry term %d error %d on %d\n", rf.me, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term, args.PrevLogIndex)
		reply.Success = false
		reply.LastLogIndex = rf.lastLogIndex()

		return
	}

	reply.Success = true

	for _, v := range args.Entries {
		DPrintf("%d Follower logging entry with index %d term %d command %v\n", rf.me, v.Index, v.Term, v.Command)
		rf.log[v.Index] = v
	}

	if args.LeaderCommitIndex > rf.commitIndex && rf.commitIndex != rf.lastLogIndex() {
		var newIndex int

		if args.LeaderCommitIndex <= rf.lastLogIndex() {
			newIndex = args.LeaderCommitIndex
		} else {
			newIndex = rf.lastLogIndex()
		}

		DPrintf("%d updated commitIndex to %d with command %v\n", rf.me, newIndex, rf.log[newIndex].Command)

		for i := rf.commitIndex; i <= newIndex; i++ {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}

		rf.commitIndex = newIndex
	}
}
