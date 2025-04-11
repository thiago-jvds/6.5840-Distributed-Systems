package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	// keep track of which Ops have committed
	ops2Submit map[int][]any
	dead       int32
	opNum      int
}

func (rsm *RSM) Kill() {
	atomic.StoreInt32(&rsm.dead, 1)
	// Your code here, if desired.
}

func (rsm *RSM) killed() bool {
	z := atomic.LoadInt32(&rsm.dead)
	return z == 1
}

func (rsm *RSM) reader() {

	for {

		rsm.mu.Lock()

		select {
		case msg, ok := <-rsm.applyCh:

			if !ok {
				rsm.Kill()
				rsm.mu.Unlock()
				// fmt.Printf("closing channel\n")
				return
			}

			if !msg.CommandValid {
				rsm.mu.Unlock()
				continue
			}

			command := msg.Command.(Op)

			// fmt.Printf("received apply op.id: %v, o p.req: %v, op.me: %v\n", command.Id, command.Req, command.Me)

			res := rsm.sm.DoOp(command.Req)
			if _, ok := rsm.ops2Submit[command.Id]; !ok {
				rsm.ops2Submit[command.Id] = []any{res, msg.CommandIndex}
			}
		default:
			// nothing in the channel, do nothing
		}
		rsm.mu.Unlock()
		time.Sleep(10 * time.Millisecond)

	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	rsm.ops2Submit = make(map[int][]any)
	rsm.opNum = 0

	go rsm.reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	op := Op{Me: rsm.me, Id: rsm.opNum, Req: req}
	rsm.opNum++

	oldIndex, oldTerm, isLeader := rsm.rf.Start(op)
	rsm.mu.Unlock()

	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	for !rsm.killed() {

		// fmt.Printf("trying to submit op.id: %v, op.req: %v, op.me: %v\n", op.Id, op.Req, op.Me)
		rsm.mu.Lock()

		// if rsm.killed() {
		// 	// fmt.Printf("shutdown rsm.me: %v\n", rsm.me)
		// 	rsm.mu.Unlock()
		// 	return rpc.ErrWrongLeader, nil
		// }

		curTerm, isLeader := rsm.rf.GetState()
		if curTerm != oldTerm || !isLeader {
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}

		arr, ok := rsm.ops2Submit[op.Id]

		if ok {
			res, index := arr[0], arr[1]
			delete(rsm.ops2Submit, op.Id)

			if index == oldIndex {
				rsm.mu.Unlock()
				// fmt.Printf("submitting op.id: %v, op.req: %v, op.me: %v\n", op.Id, op.Req, op.Me)
				return rpc.OK, res
			}
		}

		rsm.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

	return rpc.ErrWrongLeader, nil
}
