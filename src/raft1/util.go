package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// Raft - this makes sure it matches lastIncludedIndex

// Returns entry at `index` if it is included
// in the Snapshot
func (rf *Raft) getAtIndex(index int) LogEntry {
	if index < rf.lastIncludedIndex {
		DPrintf("index %d is <= lastIncludedIndex: %d", index, rf.lastIncludedIndex)
	}
	return rf.log[index-rf.lastIncludedIndex]
}

func (rf *Raft) getTotalLogLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}
