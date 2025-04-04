package raft

func (rf *Raft) sendHeartbeatsAndNewEntries() {

	for s := range len(rf.peers) {

		if s == rf.me {
			continue
		}

		go func(server int) {

			// retry indefinetely
			for !rf.killed() {

				// create args
				rf.mu.Lock()

				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[server]-rf.lastIncludedIndex < 1 {
					rf.mu.Unlock()
					rf.sendSnapshot(server)
					return
				}

				prevLogIndex := rf.nextIndex[server] - 1

				var prevLogTerm int
				// not in the snapshot
				if prevLogIndex < rf.lastIncludedIndex {
					prevLogTerm = -1
				} else {
					prevLogTerm = rf.getAtIndex(prevLogIndex).Term
				}

				entries := make([]LogEntry, 0)
				if rf.nextIndex[server]-rf.lastIncludedIndex >= 0 {
					entries = append(make([]LogEntry, 0), rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:]...)
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				DPrintf("[SHANE] S%v sending S%d AppendEntries, with following args term: %v, prevLogIndex: %v, prevLogTerm: %v, leaderCommit: %v, entries len: %v",
					rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
				DebugPrint(dLeader, "S%v sent heartbeat S%v, with following args args term: %v, prevLogIndex: %v, prevLogTerm: %v, leaderCommit: %v, entries len: %v",
					rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)

				if ok {
					rf.mu.Lock()

					// If RPC request or response contains term T > currentTerm:
					// set currentTerm = T, convert to follower (§5.1)
					if rf.state != Leader || rf.currentTerm != args.Term {
						rf.mu.Unlock()
						return

						// if server changed its state after args was created, do nothing
					} else if reply.Term > rf.currentTerm {
						rf.toFollower(reply.Term)
						rf.mu.Unlock()
						return

						// If successful: update nextIndex and matchIndex for
						// follower (§5.3)
					} else if reply.Success {
						if args.PrevLogIndex+len(entries) > rf.matchIndex[server] {
							rf.matchIndex[server] = args.PrevLogIndex + len(entries)
						}
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						rf.mu.Unlock()
						return

						// If AppendEntries fails because of log inconsistency:
						// decrement nextIndex and retry (§5.3)
					} else if !reply.Success {

						// Case 3: follower's log is too short:
						if reply.XTerm == -1 {
							rf.nextIndex[server] = reply.XLen

							// if rf.lastIncludedIndex-reply.XLen > 1 {
							// 	go rf.sendSnapshot(server)
							// }
						} else {

							// try to find the conflictTerm in log
							finalIdx := rf.getTotalLogLen() - 1
							for ; finalIdx >= rf.lastIncludedIndex; finalIdx-- {
								if rf.getAtIndex(finalIdx).Term == reply.XTerm {
									break
								}
							}

							// Case 1: leader doesn't have XTerm:
							if finalIdx == -1 {
								rf.nextIndex[server] = reply.XIndex

								// Case 2: leader has XTerm:
							} else {
								rf.nextIndex[server] = finalIdx + 1
							}

						}
						rf.mu.Unlock()
					}

				}
			}
		}(s)

	}
}

func (rf *Raft) isMajority(N int) bool {
	tot := int32(1)
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
