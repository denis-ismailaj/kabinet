package raft

import (
	"time"
)

// AppendEntriesArgs
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term              int     // leader’s term
	LeaderId          int     // so follower can redirect clients
	PrevLogIndex      int     // index of log entry immediately preceding new ones
	PrevLogTerm       int     // term of prevLogIndex entry
	Entries           []Entry // log entries to store (empty for heartbeat)
	LeaderCommitIndex int     // leader’s commitIndex
}

// AppendEntriesReply
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term         int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	LastLogIndex int  // follower's last log entry, for leader to retry in case of failure
}

//
// AppendEntries RPC handler.
//
// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
//
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

	// Ignore AppendEntries with old terms and send back the current term for the leader to update itself.
	if args.Term < rf.currentTerm {
		DPrintf("%d ignoring entry from %d with old term %d. Current term is %d.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		*reply = AppendEntriesReply{
			Success: false,
			Term:    rf.currentTerm,
		}
		return
	}

	// If leader is legitimate and the server is not currently a follower, convert to one.
	// Otherwise, just update term to the leader's if it is out of date.
	if args.Term > rf.currentTerm || rf.status == Candidate {
		rf.BecomeFollowerOrUpdateTerm(args.Term)
	}

	// Reset election timer
	rf.lastHeartbeat = time.Now()

	// When send-ing an AppendEntries RPC, the leader includes the index and term of the entry in its log that
	// immediately precedes the new entries. If the follower does not find an entry in its log with the same index and
	// term, then it refuses the new entries (§5.3).
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%d found mismatched entry term on index %d, received %d expected %d\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)

		*reply = AppendEntriesReply{
			Success:      false,
			LastLogIndex: rf.lastLogIndex(),
			Term:         rf.currentTerm,
		}
		return
	}

	// The leader handles inconsistencies by forcing the followers’ logs to duplicate its own.
	// This means that conflicting entries in follower logs will be overwritten with entries from the
	// leader’s log (§5.3).
	rf.upsertEntries(args.Entries)

	// The leader keeps track of the highest index it knows to be committed, and it includes that index in
	// future AppendEntries RPCs (including heartbeats) so that the other servers eventually find out (§5.3).
	rf.advanceCommitIndex(args.LeaderCommitIndex)

	*reply = AppendEntriesReply{
		Success:      true,
		LastLogIndex: rf.lastLogIndex(),
	}
}

// Insert or update follower's log entries
func (rf *Raft) upsertEntries(entries []Entry) {
	if len(entries) == 0 {
		return
	}

	for _, v := range entries {
		DPrintf("%d Follower logging entry with index %d term %d command %v\n", rf.me, v.Index, v.Term, v.Command)
		rf.log[v.Index] = v
	}

	// If there are old entries with higher indices than the ones supplied by the leader, delete them.
	for i := entries[len(entries)-1].Index + 1; true; i++ {
		_, exists := rf.log[i]
		if exists {
			delete(rf.log, i)
			DPrintf("%d Follower deleted stale index %d, last index now is %d\n", rf.me, i, rf.lastLogIndex())
		} else {
			break
		}
	}

	// Changes to the log need to be persisted.
	rf.persist()
}

// Update follower's commitIndex to that of the leader
func (rf *Raft) advanceCommitIndex(leaderCommitIndex int) {
	// Note: Some committed entries may not be here yet, so in that case set the commit index to the last log entry.

	if leaderCommitIndex > rf.commitIndex {
		var newIndex int

		if leaderCommitIndex < rf.lastLogIndex() {
			newIndex = leaderCommitIndex
		} else {
			newIndex = rf.lastLogIndex()
		}

		rf.updateCommitIndex(newIndex)
	}
}
