package raft

//
// RequestVote RPC handler.
//
// Invoked by candidates to gather votes (§5.2).
//
// Vote against the candidate if:
// 	- the candidate's term is older than the follower's term
//  - the follower has already voted for another candidate in this term
//  - the candidate's log is not at least as up to date as the follower's log
// or otherwise grant vote.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ok := rf.checkTerm(args.Term)

	// Ignore RequestVote RPCs with old terms and send back the current term for the candidate to update itself.
	if !ok {
		DPrintf("%d refusing %d on term %d because it's old, current term %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		*reply = RequestVoteReply{
			VoteGranted: false,
			Term:        rf.currentTerm,
		}
		return
	}

	// Each server will vote for at most one candidate in a given term, on a first-come-first-served basis (§5.2).
	if args.Term == rf.currentTerm && !rf.canVoteFor(args.CandidateId) {
		DPrintf("%d refusing %d on term %d because it has already voted for %d\n", rf.me, args.CandidateId, args.Term, *rf.votedFor)
		*reply = RequestVoteReply{
			VoteGranted: false,
		}
		return
	}

	// The RPC includes information about the candidate’s log, and the voter denies its vote if its own log
	// is more up-to-date than that of the candidate.
	// If the logs have last entries with different terms, then the log with the later term is more
	// up-to-date. If the logs end with the same term, then whichever log is longer is more up-to-date (§5.4.1).
	if args.LastLogTerm < rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex < rf.lastLogIndex()) {
		DPrintf(
			"%d refusing %d on term %d because it has a stale log %d-%d, mine %d-%d\n",
			rf.me, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, rf.lastLogTerm(), rf.lastLogIndex(),
		)
		*reply = RequestVoteReply{
			VoteGranted: false,
		}
		return
	}

	DPrintf("%d voting for %d on term %d\n", rf.me, args.CandidateId, args.Term)

	// Vote for candidate
	rf.votedFor = &args.CandidateId

	rf.ResetElectionTimer()

	// Changes to votedFor need to be persisted.
	rf.persist()

	*reply = RequestVoteReply{
		VoteGranted: true,
		Term:        rf.currentTerm,
	}
}
