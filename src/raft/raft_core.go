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
// rf.Start(command interface{}) (index, Term, isLeader)
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
	applyLock uint32

	// Persistent state
	currentTerm int           // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    *int          // candidateId that received vote in current term (or null if none)
	log         map[int]Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	status Status

	commitIndex int // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)

	matchIndex map[int]int // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	nextIndex  map[int]int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)

	lastHeartbeat   time.Time
	electionTimeout int64

	lastSnapshotIndex int
	lastSnapshotTerm  int

	// Condition variable for when Follower->Leader or Leader->Follower conversions happen.
	// This is used by the dispatcher and ticker loops to stop when they shouldn't be active.
	statusCond *sync.Cond
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}
