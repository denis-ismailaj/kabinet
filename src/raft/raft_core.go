package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyChan chan ApplyMsg

	// Persistent state
	currentTerm int
	votedFor    *int
	log         map[int]Entry

	// Volatile state
	status          Status
	commitIndex     int
	nextIndex       map[int]int
	matchIndex      map[int]int
	lastHeartbeat   time.Time
	electionTimeout int64

	// Condition variable for when Follower->Leader or Leader->Follower conversions happen.
	// This is used by the dispatcher and ticker loops to stop when they shouldn't be active.
	statusCond *sync.Cond
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}
