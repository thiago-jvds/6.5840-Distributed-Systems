package kvraft

import (
	"log"

	rsm "6.5840/kvraft1/rsm"
)

// Debugging
// const Debug = false

func DPrintf(format string, a ...interface{}) {
	if rsm.Debug {
		log.Printf(format, a...)
	}
}
