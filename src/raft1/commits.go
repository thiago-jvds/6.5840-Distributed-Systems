package raft

func (rf *Raft) sendHeartbeatsAndNewEntries() {

	for s := range len(rf.peers) {

		if s == rf.me {
			continue
		}

		go func(server int) {
			// retry indefinetely
			for {
				// create args
				rf.mu.Lock()

				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				entries := append(make([]LogEntry, 0), rf.log[rf.nextIndex[server]:]...)

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)

				if ok {
					rf.mu.Lock()

					// If RPC request or response contains term T > currentTerm:
					// set currentTerm = T, convert to follower (§5.1)
					if reply.Term > rf.currentTerm {
						rf.toFollower(reply.Term)
						rf.mu.Unlock()
						return

						// if server changed its state after args was created, do nothing
					} else if rf.state != Leader || rf.currentTerm != args.Term {
						rf.mu.Unlock()
						return

						// If successful: update nextIndex and matchIndex for
						// follower (§5.3)
					} else if reply.Success {
						rf.matchIndex[server] = args.PrevLogIndex + len(entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						rf.mu.Unlock()
						return

						// If AppendEntries fails because of log inconsistency:
						// decrement nextIndex and retry (§5.3)
					} else if !reply.Success {
						rf.nextIndex[server] = rf.nextIndex[server] - 1
						rf.mu.Unlock()

					}

				}
			}
		}(s)

	}
}

func (rf *Raft) isMajority(N int) bool {
	tot := int32(1) // count yourself
	for i := range len(rf.peers) {
		if i == rf.me {
			continue
		}

		if rf.matchIndex[i] >= N {
			tot++
		}

		if tot > rf.majority {
			return true
		}
	}
	return false
}
