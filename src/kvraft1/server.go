package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	mu   sync.Mutex
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	kvdbase map[any]KDBEntry
}

type KDBEntry struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req := req.(type) {

	case rpc.GetArgs:
		key := req.Key
		p, ok := kv.kvdbase[key]

		reply := rpc.GetReply{}

		if !ok {
			reply.Err = rpc.ErrNoKey
			return &reply
		}

		reply.Err = rpc.OK
		reply.Value = p.Value
		reply.Version = p.Version

		return &reply

	case rpc.PutArgs:
		key := req.Key
		val := req.Value
		proposedVersion := req.Version

		reply := rpc.PutReply{}

		p, ok := kv.kvdbase[key]

		var v rpc.Tversion
		if !ok {
			v = 0
		} else {
			v = p.Version
		}

		if proposedVersion != v {
			reply.Err = rpc.ErrVersion
			return &reply
		}

		kv.kvdbase[key] = KDBEntry{val, proposedVersion + 1}

		reply.Err = rpc.OK
		return &reply

	default:
		DPrintf("KVServer %v: unknown request type %T\n", kv.me, req)
		return nil
	}

}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	kv.mu.Lock()
	if _, isLeader := kv.rsm.Raft().GetState(); !isLeader {
		reply.Err = rpc.ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("KVServer %v: GET called with args %v\n", kv.me, args)

	err, val := kv.rsm.Submit(*args)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("val: %v\n", val)

	if err == rpc.OK {
		reply_ := val.(*rpc.GetReply)
		reply.Err = reply_.Err
		reply.Value = reply_.Value
		reply.Version = reply_.Version
		DPrintf("KVServer %v: GET replied OK with reply %v\n", kv.me, reply)
	} else {
		reply.Err = err
		DPrintf("KVServer %v: GET replied with an error with reply %v\n", kv.me, reply)
	}

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	if _, isLeader := kv.rsm.Raft().GetState(); !isLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	DPrintf("KVServer %v: PUT called with args %v\n", kv.me, args)

	err, val := kv.rsm.Submit(*args)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("val: %v\n", val)

	if err == rpc.OK {
		reply_ := val.(*rpc.PutReply)
		reply.Err = reply_.Err
	} else {
		reply.Err = err
	}

	DPrintf("KVServer %v: PUT called returned %v\n", kv.me, reply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.kvdbase = make(map[any]KDBEntry)
	return []tester.IService{kv, kv.rsm.Raft()}
}
