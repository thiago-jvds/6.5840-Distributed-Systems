package raft

import "sync/atomic"

func (rf *Raft) beginElection() {
	rf.mu.Lock()
	DebugPrint(dVote, "S%d BeginElection, Term: %d", rf.me, rf.currentTerm)

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// (b) starting elections,
	rf.sendAck()

	rf.mu.Unlock()

	votes := int32(1)

	for s := range len(rf.peers) {

		if s == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower (§5.1)
				if reply.Term > rf.currentTerm {
					rf.toFollower(reply.Term)
					return
				}

				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}

				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
				}

				// If candidate won the election
				if atomic.LoadInt32(&votes) > rf.majority {
					rf.toLeader()

					rf.sendHeartbeatsAndNewEntries()
				}
			}
		}(s)

	}

}
