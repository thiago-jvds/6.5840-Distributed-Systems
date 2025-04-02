package raft

type InstallSnapshotArgs struct {
	// candidate's term
	Term int

	// so follower can redirect clients
	LeaderId int

	// the snapshot replaces all entries up through
	// and including this index
	LastIncludedIndex int

	// term of lastIncludedIndex
	LastIncludedTerm int

	// raw bytes of the snapshot chunk
	Data []byte
}

type InstallSnapshotReply struct {
	// currentTerm, for candidate to update itself
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	// DebugPrint(dVote, "S%d RequestVote -> S%d (T%d)", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	return ok
}
