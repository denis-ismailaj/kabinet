package kvraft

import (
	"6.824/labgob"
	"bytes"
)

//
// save KVServer's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (kv *KVServer) getStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	//DPrintf("%d Persisting state\n", kv.me)

	kv.safeEncode(e, kv.data)
	kv.safeEncode(e, kv.latestRaftIndex)
	kv.safeEncode(e, kv.appliedReqs)
	kv.safeEncode(e, kv.latestTrimRequest)

	return w.Bytes()
}

func (kv *KVServer) safeEncode(e *labgob.LabEncoder, obj interface{}) {
	err := e.Encode(obj)
	if err != nil {
		DPrintf("ATTENTION: %d could not persist state.", kv.me)
		return
	}
}

//
// restore previously persisted state.
//
func (kv *KVServer) restoreFromSnapshot() {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.restoreFromSnapshotNoCheck(data)
}

func (kv *KVServer) restoreFromSnapshotNoCheck(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData map[string]string
	var latestRaftIndex int
	var appliedReqs map[int64]int
	var latestTrimRequest int

	if d.Decode(&kvData) != nil ||
		d.Decode(&latestRaftIndex) != nil ||
		d.Decode(&appliedReqs) != nil ||
		d.Decode(&latestTrimRequest) != nil {
		panic("Could not decode KVServer state.")
	} else {
		kv.data = kvData
		kv.latestRaftIndex = latestRaftIndex
		kv.appliedReqs = appliedReqs
		kv.latestTrimRequest = latestTrimRequest

		DPrintf("%d Restoring state\n", kv.me)
	}
}
