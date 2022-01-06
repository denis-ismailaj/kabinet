package raft

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

import (
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(10) * time.Millisecond)

		rf.mu.Lock()
		if rf.status == Leader || !rf.NoHeartbeatIn(rf.electionTimeout) {
			rf.mu.Unlock()
			continue
		}
		DPrintf("%d received no heartbeat in %d milliseconds\n", rf.me, rf.electionTimeout)
		rf.mu.Unlock()

		rf.StartElection()
	}
}

// The dispatcher go routine sends heartbeats
func (rf *Raft) dispatcher() {
	for rf.killed() == false {
		time.Sleep(time.Duration(120) * time.Millisecond)

		rf.mu.Lock()
		if rf.status != Leader {
			rf.mu.Unlock()
			continue
		}
		DPrintf("%d dispatching heartbeats on term %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			rf.mu.Lock()
			DPrintf("%d sending heartbeat to %d on term %d\n", rf.me, i, rf.currentTerm)
			rf.mu.Unlock()

			go func(i int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastLogIndex(),
					LastLogTerm:  rf.currentTerm,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}

				rf.peers[i].Call("Raft.AppendEntries", &args, reply)
			}(i)
		}
	}
}

func (rf *Raft) StartElection() {
	rf.ConvertToCandidate()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		DPrintf("%d asking for vote from %d on term %d\n", rf.me, i, rf.currentTerm)
		rf.mu.Unlock()

		go func(i int) {
			reply := rf.sendRequestVote(i)

			if reply == nil {
				// ignoring errors for now
				return
			}

			rf.mu.Lock()
			if reply.VoteGranted {
				rf.votesReceived++

				DPrintf("%d got vote from %d on term %d\n", rf.me, i, rf.currentTerm)

				if rf.status != Leader && rf.votesReceived > len(rf.peers)/2 {
					DPrintf("%d self-declared leader on term %d with %d votes\n", rf.me, rf.currentTerm, rf.votesReceived)
					rf.BecomeLeader()
				}
			} else if reply.Term > rf.currentTerm {
				rf.RevertToFollower(reply.Term)
			}
			rf.mu.Unlock()
		}(i)
	}
}
