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

	DPrintf("%d Persisting state\n", kv.me)

	kv.safeEncode(e, kv.data)

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData map[string]string

	if d.Decode(&kvData) != nil {
		panic("Could not decode KVServer state.")
	} else {
		kv.data = kvData

		DPrintf("%d Restoring state\n", kv.me)
	}
}
