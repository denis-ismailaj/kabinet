package raft

import (
	"time"
)

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

	if args.Term == rf.currentTerm && rf.status == Candidate {
		rf.status = Follower
	}

	ok := rf.checkTerm(args.Term)

	if !ok {
		// Ignore AppendEntries with old terms and send back the current term for the leader to update itself.
		DPrintf("%d ignoring AE from %d with old term %d. Current term is %d.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		*reply = AppendEntriesReply{
			Success: false,
			Term:    rf.currentTerm,
		}

		rf.mu.Unlock()
		return
	}

	// Reset election timer
	rf.lastHeartbeat = time.Now()

	// When send-ing an AppendEntries RPC, the leader includes the index and term of the entry in its log that
	// immediately precedes the new entries. If the follower does not find an entry in its log with the same index and
	// term, then it refuses the new entries (§5.3).
	if args.PrevLogIndex > 0 && rf.termForEntry(args.PrevLogIndex) != args.PrevLogTerm {
		if args.PrevLogIndex <= rf.lastLogIndex() {
			DPrintf("%d found mismatched entry term on index %d, received %d expected %d\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.termForEntry(args.PrevLogIndex))
		} else {
			DPrintf("%d ignoring AE with previous entry %d-%d, lli is %d.\n", rf.me, args.PrevLogTerm, args.PrevLogIndex, rf.lastLogIndex())
		}

		*reply = AppendEntriesReply{
			Success:      false,
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm(),
			Term:         rf.currentTerm,
		}

		rf.mu.Unlock()
		return
	}

	// The leader handles inconsistencies by forcing the followers’ logs to duplicate its own.
	// This means that conflicting entries in follower logs will be overwritten with entries from the
	// leader’s log (§5.3).
	rf.upsertEntries(args.Entries)

	*reply = AppendEntriesReply{
		Success:      true,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
		Term:         rf.currentTerm,
	}

	rf.mu.Unlock()

	// The leader keeps track of the highest index it knows to be committed, and it includes that index in
	// future AppendEntries RPCs (including heartbeats) so that the other servers eventually find out (§5.3).
	rf.advanceCommitIndex(args.LeaderCommitIndex)
}
