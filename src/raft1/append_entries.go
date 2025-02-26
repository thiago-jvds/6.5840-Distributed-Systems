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

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	// (a) receiving AppendEntries RPCs,
	rf.sendAck()

	reply.Success = false
	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false

		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
	} else if 0 <= args.PrevLogTerm && args.PrevLogTerm < len(rf.log) && // inbounds
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

		reply.Success = false

		// steps 3-5, ommitted for now
	} else {
		reply.Success = true

	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DebugPrint(dVote, "S%d AppendEntries -> S%d (T%d)", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
