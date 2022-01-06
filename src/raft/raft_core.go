package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    *int
	log         []int64
	status      Status

	lastHeartbeatTimestamp time.Time
	electionTimeout        int64
	votesReceived          int
}
