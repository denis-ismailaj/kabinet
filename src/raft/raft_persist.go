package raft

import (
	"6.824/labgob"
	"bytes"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	DPrintf("%d Persisting state at term %d\n", rf.me, rf.currentTerm)

	rf.safeEncode(e, rf.currentTerm)
	rf.safeEncode(e, rf.log)

	if rf.votedFor != nil {
		rf.safeEncode(e, rf.votedFor)
	} else {
		rf.safeEncode(e, -1)
	}

	rf.safeEncode(e, rf.lastSnapshotIndex)
	rf.safeEncode(e, rf.lastSnapshotTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) safeEncode(e *labgob.LabEncoder, obj interface{}) {
	err := e.Encode(obj)
	if err != nil {
		DPrintf("ATTENTION: %d could not persist state.", rf.me)
		return
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log map[int]Entry

	var lastSnapshotIndex int
	var lastSnapshotTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil {
		panic("Could not decode Raft state.")
	} else {
		rf.currentTerm = currentTerm
		rf.log = log
		if votedFor != -1 {
			rf.votedFor = &votedFor
		}

		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm

		rf.commitIndex = lastSnapshotIndex
		rf.lastApplied = lastSnapshotIndex

		DPrintf("%d Restoring state with term %d\n", rf.me, rf.currentTerm)
	}
}
