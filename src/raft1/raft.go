package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	// the term in which the entry was created
	Term int

	Command interface{}
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const NULL int = -1

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// ------ Persistent State on all servers

	//latest term server has seen (initialized to 0
	//on first boot, increases monotonically)
	currentTerm int

	//candidateId that received vote in current
	//term (or null if none)
	votedFor int

	// log entries; each entry contains command
	// for state machine, and term when entry
	//was received by leader (first index is 1)
	log []LogEntry

	// ------ Volatile State on all servers

	// commitIndex index of highest log entry known to be
	// committed (initialized to 0, increases monotonically)
	commitIndex int

	// index of highest log entry applied to state
	// machine (initialized to 0, increases monotonically)
	lastApplied int

	// ------ Volatile State on leaders (Reinitialized after election)

	//for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	nextIndex []int

	// for each server, index of highest log entry
	// known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int

	// ------- Additional attributes

	// Current state for the server. It can be Follower,
	// Candidate, or Leader
	state State

	// channels to send notifications
	receivedCh chan bool

	// Majority number
	majority int32

	// Apply channel to send commit messages
	applyCh chan raftapi.ApplyMsg

	// index of the
	// last entry in the log that the snapshot replaces
	lastIncludedIndex int

	// Snapshot last included term
	lastIncludedTerm int

	// Snapshot
	snapshot []byte
}

func (rf *Raft) sendAck() {
	// Wait for the channel to be emptied
	select {
	case <-rf.receivedCh:
	default:
	}

	// Send something
	rf.receivedCh <- true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DebugPrint(dError, "S%v Error in readPersist", rf.me)

	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		// rf.commitIndex, rf.lastApplied = rf.lastIncludedIndex, rf.lastIncludedIndex
		rf.mu.Unlock()
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if snapshot is lagging
	if index <= rf.lastIncludedIndex {
		return
	}

	DPrintf("SNAPSHOT: at index: %d, S%d: State: %d, VotedFor: %d, Term: %d, LastIncludedIndex: %d", index, rf.me, rf.state, rf.votedFor, rf.currentTerm, rf.lastIncludedIndex)

	// Once a
	// server completes writing a snapshot, it may delete all log
	// entries up through the last included index, as well as any
	// prior snapshot
	rf.log = append(make([]LogEntry, 0), rf.log[index-rf.lastIncludedIndex:]...)

	rf.lastIncludedIndex = index
	lastIncludedEntry := rf.getAtIndex(index)
	rf.lastIncludedTerm = lastIncludedEntry.Term

	rf.snapshot = snapshot
	rf.persist()

}

// Become Functions -- Helpers to transform a server into different states
// They are NOT locked, so it needs to be used inside locked functions.

// Transforms server into the follower state
//
// `term` - term the server becomes follower. Gets from leader.
func (rf *Raft) toFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = NULL
	rf.persist()
}

// Transform server into candidate state
// To begin an election, a follower increments its current
// term and transitions to candidate state
func (rf *Raft) toCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
}

// Transform server into Leader state
//
// nextIndex - (initialized to leader
// last log index + 1)
//
// matchIndex - (initialized to 0, increases monotonically)
func (rf *Raft) toLeader() {
	if rf.state != Candidate {
		fmt.Printf("raft server %v tried to become leader as %v", rf.me, rf.state)
		return
	}

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range len(rf.peers) {
		rf.nextIndex[i] = rf.getTotalLogLen()
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) CheckElection() {

	// loop indefinetely for a new election
	for !rf.killed() {

		rf.mu.Lock()
		DebugPrint(dInfo, "S%d, State: %d, VotedFor: %d, Term: %d, LastIncludedIndex: %d", rf.me, rf.state, rf.votedFor, rf.currentTerm, rf.lastIncludedIndex)
		DPrintf("[CHECKELECTION] S%d, State: %d, VotedFor: %d, Term: %d, LastIncludedIndex: %d", rf.me, rf.state, rf.votedFor, rf.currentTerm, rf.lastIncludedIndex)
		state := rf.state
		rf.mu.Unlock()

		// Randomly select the election time
		electionTimeout := time.Duration(400+rand.Intn(200)) * time.Millisecond

		// Heartbeat interval
		// Hint: < 100ms (https://piazza.com/class/m6hpwkz1o806u2/post/109)
		heartbeatInterval := time.Duration(100) * time.Millisecond

		// Implementing according to the hint by
		// https://piazza.com/class/m6hpwkz1o806u2/post/102

		switch state {
		case Follower:
			select {
			case <-rf.receivedCh:
				// (a) receiving AppendEntries RPCs,
				// (b) starting elections,
				// (c) receiving valid (success replies) responses from sendRequestVote

			case <-time.After(electionTimeout):
				rf.mu.Lock()
				rf.toCandidate()
				go rf.beginElection()
				rf.mu.Unlock()

			}

		case Candidate:
			select {
			case <-rf.receivedCh:
				// (a) receiving AppendEntries RPCs,
				// (b) starting elections,
				// (c) receiving valid (success replies) responses from sendRequestVote

			case <-time.After(electionTimeout):
				rf.mu.Lock()
				rf.toCandidate()
				go rf.beginElection()
				rf.mu.Unlock()

			}

		case Leader:
			time.Sleep(heartbeatInterval)
			rf.sendHeartbeatsAndNewEntries()
		}
	}
}

func (rf *Raft) PushCommitIndex() {
	waitTime := 1 * time.Millisecond

	for !rf.killed() {

		rf.mu.Lock()

		if rf.state != Leader {
			rf.mu.Unlock()
			continue
		}

		// DebugPrint(dCommit, "S%v trying commit index. Info log: %v, matchIndex: %v, term: %v", rf.me, rf.log, rf.matchIndex, rf.currentTerm)
		for N := rf.commitIndex + 1; N < rf.getTotalLogLen(); N++ {
			if rf.isMajority(N) && rf.getAtIndex(N).Term == rf.currentTerm {
				rf.commitIndex = N
				DPrintf("[PUSHCOMMITINDEX] S%d, CommitIndex: %d", rf.me, rf.commitIndex)
			}
		}

		rf.mu.Unlock()
		time.Sleep(waitTime)

	}
}

func (rf *Raft) SendApplyCh() {
	waitTime := 1 * time.Millisecond

	for !rf.killed() {

		rf.mu.Lock()

		// DebugPrint(dCommit, "S%v is committing entries index [%v, %v)", rf.me, rf.lastApplied, rf.commitIndex)
		rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
		rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
		for rf.commitIndex > rf.lastApplied && rf.lastApplied < rf.getTotalLogLen() {
			rf.lastApplied++
			DPrintf("[SENDAPPLYCH] S%d, CommitIndex: %d, lastApplied: %d, lastIncludedIndex: %d, log len: %d", rf.me, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, rf.getTotalLogLen())
			entry := rf.getAtIndex(rf.lastApplied)
			rf.mu.Unlock()
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(waitTime)

	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	term := rf.currentTerm
	isLeader := rf.state == Leader
	if !isLeader {
		rf.mu.Unlock()

		return -1, term, isLeader
	}

	index := rf.getTotalLogLen()

	newEntry := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)

	rf.mu.Unlock()
	rf.persist()
	// Send AppendEntries to other servers
	rf.sendHeartbeatsAndNewEntries()

	//If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapshot = make([]byte, 0)

	rf.applyCh = applyCh

	rf.majority = int32(len(peers) / 2)
	rf.state = Follower
	rf.receivedCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.snapshot = persister.ReadSnapshot()

	go rf.CheckElection()

	go rf.PushCommitIndex()

	go rf.SendApplyCh()

	return rf
}
