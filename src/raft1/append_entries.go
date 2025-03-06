package raft

type AppendEntriesArgs struct {
	//leader’s term
	Term int

	// so follower can redirect clients
	LeaderId int

	// index of log entry immediately preceding
	PrevLogIndex int

	// term of prevLogIndex entry
	PrevLogTerm int

	// log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	Entries []LogEntry

	// leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int

	// true if follower contained entry matching
	// prevLogIndex and prevLogTerm
	Success bool
}

// Invoked by leader to replicate log entries (§5.3); also used as
// heartbeat (§5.2).
//
// 1. Reply false if term < currentTerm (§5.1)
//
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
//
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
//
// 4. Append any new entries not already in the log
//
// 5. If leaderCommit > commitIndex, set commitIndex =
// min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugPrint(dLog2, "S%v received AppendEntries from S%v, with following args term: %v, prevLogIndex: %v, prevLogTerm: %v, leaderCommit: %v, entries len: %v", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	// (a) receiving AppendEntries RPCs,
	rf.sendAck()

	reply.Success = false
	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return

	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	checkPrevLogIndexTerm := -1
	if 0 <= args.PrevLogIndex && args.PrevLogIndex < len(rf.log) {
		checkPrevLogIndexTerm = rf.log[args.PrevLogIndex].Term
	}

	if checkPrevLogIndexTerm != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true

	DebugPrint(dInfo, "S%v append entries from entries: %v. Current log: %v", rf.me, args.Entries, rf.log)

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	existingIndex := args.PrevLogIndex
	newIndex := 0

loop:
	if newIndex >= len(args.Entries) {
		goto end
	}

	existingIndex++

	// valid log index and terms for new entry match -> go to next entry
	if existingIndex < len(rf.log) && rf.log[existingIndex].Term == args.Entries[newIndex].Term {
		newIndex++
		goto loop
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if existingIndex < len(rf.log) && rf.log[existingIndex].Term != args.Entries[newIndex].Term {
		rf.log = rf.log[:existingIndex]
	}

	// 4. Append any new entries not already in the log
	// if it got here, existingIndex is out-of-bounds or terms don't match starting on newIndex
	rf.log = append(rf.log, args.Entries[newIndex:]...)

end:

	DebugPrint(dInfo, "S%v appended new entries. Final log: %v", rf.me, rf.log)

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DebugPrint(dVote, "S%d AppendEntries -> S%d (T%d) nextIndex: %v, matchIndex: %v, len log: %v", rf.me, server, args.Term, rf.nextIndex, rf.matchIndex, len(rf.log))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
