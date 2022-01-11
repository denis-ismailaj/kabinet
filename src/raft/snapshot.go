package raft

import "time"

// CondInstallSnapshot
// A service wants to switch to snapshot. Only do so if Raft doesn't
// have more recent info since it sent the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	DPrintf("%d CondInstall term %d index %d\n", rf.me, lastIncludedTerm, lastIncludedIndex)

	if lastIncludedTerm < rf.lastLogTerm() ||
		(lastIncludedTerm == rf.lastLogTerm() && lastIncludedIndex < rf.lastLogIndex()) {
		DPrintf("%d refusing CondInstall term %d index %d\n", rf.me, lastIncludedTerm, lastIncludedIndex)
		return false
	}

	rf.saveSnapshot(lastIncludedIndex, lastIncludedTerm, snapshot)

	return true
}

// Snapshot
// The service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("%d Service snapshotted index %d\n", rf.me, index)

	rf.mu.Lock()
	snapshotTerm := rf.termForEntry(index)
	rf.mu.Unlock()

	rf.saveSnapshot(index, snapshotTerm, snapshot)
}

func (rf *Raft) saveSnapshot(index int, term int, snapshot []byte) {
	DPrintf("%d Saving snapshot with index %d term %d\n", rf.me, index, term)

	rf.mu.Lock()

	// Delete log entries up to the snapshot index
	for i := rf.lastSnapshotIndex; i <= index; i++ {
		delete(rf.log, i)
		DPrintf("%d deleted unneeded index %d after snapshot.\n", rf.me, i)
	}

	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = term

	rf.lastApplied = index

	// Persist changes to lastSnapshotIndex and lastSnapshotTerm.
	rf.persist()

	rf.persister.SaveSnapshot(snapshot)

	rf.mu.Unlock()
}

// InstallSnapshotArgs
// InstallSnapshot RPC arguments structure.
//
type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

// InstallSnapshotReply
// InstallSnapshot RPC reply structure.
//
type InstallSnapshotReply struct {
	Term         int // currentTerm, for leader to update itself
	LastLogIndex int
}

//
// InstallSnapshot RPC handler.
// Invoked by leader to send a snapshot to a follower.
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("%d InstallSnapshot %d-%d\n", rf.me, args.LastIncludedTerm, args.LastIncludedIndex)

	rf.mu.Lock()

	// Ignore InstallSnapshot with old terms and send back the current term for the leader to update itself.
	if args.Term < rf.currentTerm {
		DPrintf("%d ignoring entry from %d with old term %d. Current term is %d.\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		*reply = InstallSnapshotReply{
			Term: rf.currentTerm,
		}

		rf.mu.Unlock()
		return
	}

	// If leader is legitimate and the server is not currently a follower, convert to one.
	// Otherwise, just update term to the leader's if it is out of date.
	if args.Term > rf.currentTerm || rf.status == Candidate {
		rf.BecomeFollowerOrUpdateTerm(args.Term)
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
		Term:         rf.currentTerm,
		LastLogIndex: rf.lastLogIndex(),
	}
	rf.mu.Unlock()
}
