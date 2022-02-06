package raft

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
	LastLogTerm  int  // follower's term for last log entry
}
