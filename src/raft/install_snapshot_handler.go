package raft

import "time"

//
// InstallSnapshot RPC handler.
// Invoked by leader to send a snapshot to a follower.
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("%d InstallSnapshot %d-%d\n", rf.me, args.LastIncludedTerm, args.LastIncludedIndex)

	rf.mu.Lock()

	ok := rf.checkTerm(args.Term)

	// Ignore InstallSnapshot with old terms and send back the current term for the leader to update itself.
	if !ok {
		DPrintf("%d ignoring entry from %d with old term %d. Current term is %d.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		*reply = InstallSnapshotReply{
			Term: rf.currentTerm,
		}

		rf.mu.Unlock()
		return
	}

	if args.Term == rf.currentTerm && rf.status == Candidate {
		rf.status = Follower
	}

	// Reset election timer
	rf.lastHeartbeat = time.Now()

	rf.mu.Unlock()

	rf.applyChan <- ApplyMsg{
		CommandValid:  false,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotValid: true,
	}

	rf.mu.Lock()
	*reply = InstallSnapshotReply{
		Term: rf.currentTerm,
	}
	rf.mu.Unlock()
}
