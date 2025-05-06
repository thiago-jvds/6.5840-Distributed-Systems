package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid
	mu   sync.Mutex

	// shardId -> config Num
	configNums map[shardcfg.Tshid]shardcfg.Tnum

	// req -> entry + version
	shard2kvdbase map[shardcfg.Tshid]map[any]KDBEntry

	// clientId -> command
	client2latestCmd map[int32]Cmd

	// controllerId -> command
	controller2latestCmd map[int32]Cmd

	// shards owned
	ownedShards [shardcfg.NShards]bool

	// shards that are frozen
	freeze map[shardcfg.Tshid]bool
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req := req.(type) {

	case rpc.GetArgs:

		return kv.handleGetRequest(&req)

	case rpc.PutArgs:

		return kv.handlePutRequest(&req)

	case shardrpc.FreezeShardArgs:

		return kv.handleFreezeRequest(&req)

	case shardrpc.InstallShardArgs:

		return kv.handleInstallRequest(&req)

	case shardrpc.DeleteShardArgs:

		return kv.handleDeleteRequest(&req)

	default:
		DPrintf("KVServer %v: unknown request type %T\n", kv.me, req)
		return nil
	}

}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("KVServer %v: Snapshotting\n", kv.me)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shard2kvdbase)
	e.Encode(kv.client2latestCmd)
	e.Encode(kv.controller2latestCmd)
	e.Encode(kv.configNums)
	e.Encode(kv.ownedShards)
	e.Encode(kv.freeze)

	if err := e.Encode(kv.shard2kvdbase); err != nil {
		DPrintf("Error in encoding kvdbase")
	}

	if err := e.Encode(kv.client2latestCmd); err != nil {
		DPrintf("Error in encoding client2latestCmd")
	}

	if err := e.Encode(kv.controller2latestCmd); err != nil {
		DPrintf("Error in encoding controller2latestCmd")
	}
	if err := e.Encode(kv.configNums); err != nil {
		DPrintf("Error in encoding configNums")
	}
	if err := e.Encode(kv.ownedShards); err != nil {
		DPrintf("Error in encoding ownedShards")
	}
	if err := e.Encode(kv.freeze); err != nil {
		DPrintf("Error in encoding freeze")
	}

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	DPrintf("KVServer %v: Restoring\n", kv.me)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shard2kvdbase map[shardcfg.Tshid]map[any]KDBEntry
	var client2latestCmd map[int32]Cmd
	var controller2latestCmd map[int32]Cmd
	var configNums map[shardcfg.Tshid]shardcfg.Tnum
	var ownedShards [shardcfg.NShards]bool
	var freeze map[shardcfg.Tshid]bool

	if d.Decode(&shard2kvdbase) != nil ||
		d.Decode(&client2latestCmd) != nil ||
		d.Decode(&controller2latestCmd) != nil ||
		d.Decode(&configNums) != nil ||
		d.Decode(&ownedShards) != nil ||
		d.Decode(&freeze) != nil {
		DPrintf("Error in decoding kvdbase")
	} else {
		kv.shard2kvdbase = shard2kvdbase
		kv.client2latestCmd = client2latestCmd
		kv.controller2latestCmd = controller2latestCmd
		kv.configNums = configNums
		kv.ownedShards = ownedShards
		kv.freeze = freeze
	}

	DPrintf("KVServer %v: Done Restoring\n", kv.me)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	DPrintf("KVServer %v: [GET] Starting\n", kv.me)

	// kv.mu.Lock()
	// shardId := shardcfg.Key2Shard(args.Key)

	// if kv.isFrozen(shardId) {
	// 	reply.Err = rpc.ErrWrongGroup
	// 	reply.Value = ""
	// 	reply.Version = 0
	// 	DPrintf("KVServer %v: [GET] called with args %v, but shard %v is frozen\n", kv.me, args, shardId)
	// 	kv.mu.Unlock()
	// 	return

	// }
	// kv.mu.Unlock()

	// kv.mu.Lock()
	// if lastestCmd, ok := kv.client2latestCmd[args.CId]; ok {
	// 	if lastestCmd.RId >= args.RId {

	// 		reply.Value = kv.shard2kvdbase[shardId][args.Key].Value
	// 		reply.Version = kv.shard2kvdbase[shardId][args.Key].Version
	// 		reply.Err = rpc.OK
	// 		DPrintf("KVServer %v: [GET DUP] called with args %v, but already processed\n", kv.me, args)
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// }
	// kv.mu.Unlock()

	kv.mu.Lock()
	if _, isLeader := kv.rsm.Raft().GetState(); !isLeader {
		reply.Err = rpc.ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("KVServer %v: GET called with args %v\n", kv.me, args)

	err, res := kv.rsm.Submit(*args)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err == rpc.OK {
		reply_ := res.(*rpc.GetReply)
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

	DPrintf("KVServer %v: [PUT] Starting\n", kv.me)
	// shardId := shardcfg.Key2Shard(args.Key)

	// kv.mu.Lock()
	// if kv.isFrozen(shardId) {
	// 	reply.Err = rpc.ErrWrongGroup
	// 	DPrintf("KVServer %v: [PUT] called with args %v, but shard %v is frozen\n", kv.me, args, shardId)
	// 	kv.mu.Unlock()
	// 	return

	// }
	// kv.mu.Unlock()

	// kv.mu.Lock()
	// if lastestCmd, ok := kv.client2latestCmd[args.CId]; ok {
	// 	if lastestCmd.RId >= args.RId {
	// 		reply.Err = rpc.OK
	// 		DPrintf("KVServer %v: [PUT DUP] called with args %v, but already processed\n", kv.me, args)
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// }
	// kv.mu.Unlock()

	kv.mu.Lock()
	if _, isLeader := kv.rsm.Raft().GetState(); !isLeader {
		reply.Err = rpc.ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	DPrintf("KVServer %v: PUT called with args %v\n", kv.me, args)

	err, res := kv.rsm.Submit(*args)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err == rpc.OK {
		reply_ := res.(*rpc.PutReply)
		reply.Err = reply_.Err
	} else {
		reply.Err = err
	}

	DPrintf("KVServer %v: PUT called returned %v\n", kv.me, reply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	DPrintf("KVServer %v: [FREEZE] Starting\n", kv.me)

	// kv.mu.Lock()
	// if lastestCmd, ok := kv.client2latestCmd[args.CId]; ok {
	// 	if lastestCmd.RId >= args.RId {

	// 		reply.Err = rpc.OK
	// 		DPrintf("KVServer %v: [FREEZE DUP] called with args %v, but already processed\n", kv.me, args)
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// }
	// kv.mu.Unlock()

	kv.mu.Lock()
	if _, isLeader := kv.rsm.Raft().GetState(); !isLeader {
		reply.Err = rpc.ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("KVServer %v: FREEZE called with args %v\n", kv.me, args)

	err, res := kv.rsm.Submit(*args)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err == rpc.OK {
		reply_ := res.(*shardrpc.FreezeShardReply)
		reply.Err = reply_.Err
		reply.Num = reply_.Num
		reply.State = reply_.State
		DPrintf("KVServer %v: FREEZE replied OK\n", kv.me)
	} else {
		reply.Err = err
		DPrintf("KVServer %v: FREEZE replied with an error: %v\n", kv.me, reply.Err)
	}

}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	DPrintf("[KVServer %v INSTALL] Starting\n", kv.me)

	// kv.mu.Lock()
	// if lastestCmd, ok := kv.client2latestCmd[args.CId]; ok {
	// 	if lastestCmd.RId >= args.RId {

	// 		reply.Err = rpc.OK
	// 		DPrintf("[KVServer %v INSTALL DUP] called but already processed\n", kv.me)
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// }
	// kv.mu.Unlock()

	kv.mu.Lock()
	if _, isLeader := kv.rsm.Raft().GetState(); !isLeader {
		reply.Err = rpc.ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("[KVServer %v INSTALL] called \n", kv.me)

	err, res := kv.rsm.Submit(*args)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err == rpc.OK {
		reply_ := res.(*shardrpc.InstallShardReply)
		reply.Err = reply_.Err
		DPrintf("[KVServer %v INSTALL] replied OK with reply %v\n", kv.me, reply)
	} else {
		reply.Err = err
		DPrintf("[KVServer %v INSTALL] replied with an error with reply %v\n", kv.me, reply)
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// kv.mu.Lock()
	// if lastestCmd, ok := kv.client2latestCmd[args.CId]; ok {
	// 	if lastestCmd.RId >= args.RId {

	// 		reply.Err = rpc.OK
	// 		DPrintf("[KVServer %v DELETE DUP] called with args %v, but already processed\n", kv.me, args)
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// }
	// kv.mu.Unlock()

	kv.mu.Lock()
	if _, isLeader := kv.rsm.Raft().GetState(); !isLeader {
		reply.Err = rpc.ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("[KVServer %v DELETE] called for shard %v\n", kv.me, args.Shard)

	err, res := kv.rsm.Submit(*args)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err == rpc.OK {
		reply_ := res.(*shardrpc.DeleteShardReply)
		reply.Err = reply_.Err
		DPrintf("[KVServer %v DELETE] replied OK \n", kv.me)
	} else {
		reply.Err = err
		DPrintf("[KVServer %v DELETE] replied with an error: %v \n", kv.me, reply.Err)
	}
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}

	kv.shard2kvdbase = make(map[shardcfg.Tshid]map[any]KDBEntry)

	kv.client2latestCmd = make(map[int32]Cmd)
	kv.controller2latestCmd = make(map[int32]Cmd)

	kv.configNums = make(map[shardcfg.Tshid]shardcfg.Tnum)

	kv.freeze = make(map[shardcfg.Tshid]bool)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// For the initial configuration (i.e., configuration number = 0),
	// you can hard-code it in the initialization method of shard
	// (i.e., StartServerShardGrp) group: if gid is equal to
	// shardcfg.Gid1, then assign all shards to this group; otherwise, assign none.
	for i := 0; i < shardcfg.NShards; i++ {

		kv.configNums[shardcfg.Tshid(i)] = 0

		if gid == shardcfg.Gid1 {
			kv.ownedShards[i] = true
			kv.shard2kvdbase[shardcfg.Tshid(i)] = make(map[any]KDBEntry)
		} else {
			kv.ownedShards[i] = false
		}
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
