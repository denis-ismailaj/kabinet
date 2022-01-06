package raft

import "time"

// AppendEntriesArgs
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// AppendEntriesReply
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term        int
	VoteGranted bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		rf.lastHeartbeatTimestamp = time.Now()
		rf.RevertToFollower(args.Term)
	}
}
