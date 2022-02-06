package raft

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
