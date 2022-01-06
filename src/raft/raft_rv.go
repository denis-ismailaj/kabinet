package raft

// RequestVoteArgs
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
// Votes against the candidate if:
// 	- the candidate's Term is older than the current Term
//  - it has already voted for another candidate in this Term
//  - the candidate's log is not at least as up to date as its log
// or otherwise grants vote.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		DPrintf("%d refusing %d on term %d because it's old, current %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)

	} else if args.Term == rf.currentTerm && rf.votedFor != nil {

		DPrintf("%d refusing %d on term %d because it has already voted\n", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

	} else {
		rf.currentTerm = args.Term
		rf.votedFor = &args.CandidateId

		rf.ResetElectionTimer()

		reply.VoteGranted = true
		reply.Term = rf.currentTerm

		DPrintf("%d voting for %d on term %d\n", rf.me, args.CandidateId, args.Term)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int) *RequestVoteReply {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.currentTerm,
	}
	rf.mu.Unlock()

	reply := &RequestVoteReply{}

	ok := rf.peers[server].Call("Raft.RequestVote", &args, reply)

	if ok {
		return reply
	} else {
		return nil
	}
}
