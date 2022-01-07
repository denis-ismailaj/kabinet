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
	index := rf.appendLogEntry(entry)

	rf.mu.Unlock()

	DPrintf("%d storing log entry %d on term %d\n", rf.me, index, rf.currentTerm)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      rf.lastLogIndex() - 1,
				PrevLogTerm:       rf.lastLogTerm(),
				Entries:           []Entry{entry},
				LeaderCommitIndex: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}

			rf.peers[i].Call("Raft.AppendEntries", &args, reply)

			rf.mu.Lock()
			if reply.Success {
				DPrintf("%d setting matchindex for %d to %d\n", rf.me, i, entry.Index)
				rf.matchIndex[i] = entry.Index

				if rf.IsIndexCommitted(entry.Index) {
					DPrintf("%d leader setting commitindex to %d\n", rf.me, entry.Index)
					rf.commitIndex = entry.Index

					rf.applyChan <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[rf.commitIndex].Command,
						CommandIndex: rf.commitIndex,
					}

					DPrintf("%d leader commitIndex to %d value %v\n", rf.me, rf.commitIndex, rf.log[rf.commitIndex].Command)
				}
			}
			rf.mu.Unlock()
		}(i)
	}

	DPrintf("%d leader claiming to commit %d\n", rf.me, entry.Index)

	return entry.Index, rf.currentTerm, rf.status == Leader
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
	Term    int
	Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term >= rf.currentTerm {
		rf.lastHeartbeatTimestamp = time.Now()
		if rf.status != Follower {
			rf.RevertToFollower(args.Term)
		}
	}
	if len(args.Entries) == 0 {
		// ignore heartbeats for now
	} else if args.Term < rf.currentTerm || args.PrevLogIndex > rf.lastLogIndex() {
		DPrintf("%d ignoring log entry term %d prevleader %d lli %d\n", rf.me, rf.currentTerm, args.PrevLogIndex, rf.lastLogIndex())
		reply.Success = false
	} else if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%d ignoring log entry term %d error %d on %d\n", rf.me, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term, args.PrevLogIndex)
		reply.Success = false
	} else {
		DPrintf("%d appending entries on term %d\n", rf.me, rf.currentTerm)

		for _, v := range args.Entries {
			DPrintf("%d before appending lli %d key %d value %v\n", rf.me, rf.lastLogIndex(), v.Index, v)
			rf.log[v.Index] = v
			DPrintf("%d after appending lli %d\n", rf.me, rf.lastLogIndex())
		}

		reply.Success = true
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		DPrintf("%d got higher ci %d from leader after %d lli %d\n", rf.me, args.LeaderCommitIndex, rf.commitIndex, rf.lastLogIndex())

		if args.LeaderCommitIndex < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommitIndex
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}

		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.commitIndex].Command,
			CommandIndex: rf.commitIndex,
		}

		DPrintf("%d entry setting commitIndex to %d value %v\n", rf.me, rf.commitIndex, rf.log[rf.commitIndex].Command)
	}
}
