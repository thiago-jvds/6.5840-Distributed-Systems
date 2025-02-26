package raft

type RequestVoteArgs struct {
	// candidate's term
	Term int

	//candidate requesting vote
	CandidateId int

	// index of candidate’s last log entry (§5.4)
	LastLogIndex int

	// term of candidate’s last log entry (§5.4)
	LastLogTerm int
}

type RequestVoteReply struct {
	// currentTerm, for candidate to update itself
	Term int

	// true means candidate received vote
	VoteGranted bool
}

// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date.
//
// Returns true if candidate's log is at least up-to-date, else false
func (rf *Raft) IsAtLeastUpToDate(args *RequestVoteArgs) bool {
	receiverLastIndex := len(rf.log) - 1

	var receiverLastLogTerm int = -1

	if receiverLastIndex >= 0 {
		receiverLastLogTerm = rf.log[receiverLastIndex].Term
	}

	candidateLastIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm

	if receiverLastLogTerm != candidateLastLogTerm {
		return candidateLastLogTerm >= receiverLastLogTerm
	}

	return candidateLastIndex >= receiverLastIndex

}

// Invoked by candidates to gather votes (§5.2).
//
// 1. Reply false if term < currentTerm (§5.1)
//
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if rf.currentTerm < args.Term {
		rf.toFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	if args.Term >= rf.currentTerm && // candidate has larger term
		(rf.votedFor == NULL || rf.votedFor == args.CandidateId) && // receiver hasn't voted to anyone else
		rf.IsAtLeastUpToDate(args) { // candidate is at least up-to-date

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = Follower

	} else {
		reply.VoteGranted = false
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DebugPrint(dVote, "S%d RequestVote -> S%d (T%d)", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// (c) receiving valid (success replies) responses from sendRequestVote
	if ok {
		rf.sendAck()
	}

	return ok
}
