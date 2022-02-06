package raft

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.ConvertToCandidate()

	DPrintf("%d starting election on term %d\n", rf.me, rf.currentTerm)

	// Creating the RequestVote structure only once and while under the same lock as the conversion to candidate.
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	rf.mu.Unlock()

	// A channel to count votes
	voteChannel := make(chan *RequestVoteReply)

	go rf.countVotes(voteChannel, args.Term)
	go rf.askForVotes(voteChannel, args)
}

func (rf *Raft) askForVotes(voteChannel chan *RequestVoteReply, args RequestVoteArgs) {
	// The candidate issues RequestVote RPCs in parallel to each of the other servers in the cluster (§5.2).
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			DPrintf("%d asking for vote from %d on term %d\n", rf.me, i, args.Term)

			// Not bothering retrying failed vote requests, because if the election is not won quickly
			// it will soon be restarted anyway.
			reply := &RequestVoteReply{}
			ok := rf.peers[i].Call("Raft.RequestVote", &args, reply)

			if ok {
				voteChannel <- reply
			}
		}(i)
	}
}

func (rf *Raft) countVotes(voteChannel chan *RequestVoteReply, term int) {
	// Start with the vote from the server itself.
	votesReceived := 1

	for reply := range voteChannel {
		rf.mu.Lock()

		// Stop the count if the server has moved on.
		if rf.currentTerm > term {
			DPrintf("%d stopping count for old election %d, now on term %d", rf.me, term, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		ok := rf.checkTerm(reply.Term)

		if rf.status != Candidate {
			DPrintf("%d stopping count for election %d, not a candidate anymore", rf.me, term)
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		if reply.VoteGranted && ok {
			votesReceived++
			DPrintf("%d vote count %d/%d\n", rf.me, votesReceived, len(rf.peers))
		}

		// A candidate wins an election if it receives votes from a majority of the servers in the
		// full cluster for the same term (§5.2).
		if votesReceived > len(rf.peers)/2 {
			rf.BecomeLeader()
			return
		}
	}
}
