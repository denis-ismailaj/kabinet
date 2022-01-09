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
		if i != rf.me {
			go func(i int) {
				DPrintf("%d asking for vote from %d on term %d\n", rf.me, i, rf.currentTerm)

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
}

func (rf *Raft) countVotes(voteChannel chan *RequestVoteReply, term int) {
	// Start with the vote from the server itself.
	votesReceived := 1

	for reply := range voteChannel {
		rf.mu.Lock()

		// Stop the count if the server has moved on.
		if rf.currentTerm > term {
			rf.mu.Unlock()
			return
		}

		// Convert to follower if term is out of date.
		if reply.Term > rf.currentTerm {
			rf.BecomeFollowerOrUpdateTerm(reply.Term)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if reply.VoteGranted {
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
