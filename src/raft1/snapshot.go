package raft

import (
	"6.5840/raftapi"
)

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

	DebugPrint(dSnap, "S%v received Snapshot from S%v, with following args term: %v, leaderId: %v, LastIncludedIndex: %v, lastIncludedTerm: %v, data len: %v",
		rf.me, args.LeaderId, args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply

	// this means there exists a log entry with the same index and term
	// and its known args.lastIncludedindex > rf.lastIncludedindex
	if args.LastIncludedIndex < rf.getTotalLogLen()-1 {
		// retain log entries following it and reply
		rf.log = append(make([]LogEntry, 0), rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
	} else {

		// Discard the entire log
		rf.log = []LogEntry{{args.LastIncludedTerm, nil}}
	}

	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.commitIndex, rf.lastApplied = args.LastIncludedIndex, args.LastIncludedIndex

	rf.snapshot = args.Data
	rf.persist()

	msg := raftapi.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	// DebugPrint(dVote, "S%d RequestVote -> S%d (T%d)", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	return ok
}

func (rf *Raft) sendSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	reply := InstallSnapshotReply{}

	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	rf.mu.Lock()

	if ok {

		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.toFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1

	}

	rf.mu.Unlock()
}
