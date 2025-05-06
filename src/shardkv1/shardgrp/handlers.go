package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

// ALL HANDLERS ARE ASSUMED TO BE LOCKED ALREADY

func (kv *KVServer) handleGetRequest(req *rpc.GetArgs) *rpc.GetReply {
	reply := rpc.GetReply{}

	key := req.Key
	shardId := shardcfg.Key2Shard(key)

	ok, trueVal := kv.CheckDuplicates(req.CId, req.RId)
	if ok {
		reply.Value = trueVal.Value
		reply.Err = rpc.OK

		cmd := Cmd{
			Key:     req.Key,
			Value:   trueVal.Value,
			CId:     req.CId,
			RId:     req.RId,
			Version: trueVal.Version,
		}

		kv.client2latestCmd[req.CId] = cmd
		return &reply
	}

	if !kv.ownedShards[shardId] || kv.isFrozen(shardId) {
		reply.Err = rpc.ErrWrongGroup
		return &reply
	}

	db, ok := kv.shard2kvdbase[shardId]

	if !ok {
		panic("shard does not exist")
	}

	p, ok := db[key]

	if !ok {
		reply.Err = rpc.ErrNoKey
		return &reply
	}

	reply.Err = rpc.OK
	reply.Value = p.Value
	reply.Version = p.Version

	return &reply
}

func (kv *KVServer) handlePutRequest(req *rpc.PutArgs) *rpc.PutReply {
	key := req.Key
	val := req.Value
	proposedVersion := req.Version
	shardId := shardcfg.Key2Shard(key)

	reply := rpc.PutReply{}

	ok, trueVal := kv.CheckDuplicates(req.CId, req.RId)
	if ok {
		reply.Err = rpc.OK
		cmd := Cmd{
			Key:     key,
			Value:   trueVal.Value,
			CId:     req.CId,
			RId:     req.RId,
			Version: trueVal.Version,
		}
		kv.client2latestCmd[req.CId] = cmd
		DPrintf("[PUT] GOT FROM DUP\n")
		return &reply
	}

	if !kv.ownedShards[shardId] || kv.isFrozen(shardId) {
		reply.Err = rpc.ErrWrongGroup
		return &reply
	}

	db, ok := kv.shard2kvdbase[shardId]

	if !ok {
		panic("shard does not exist")
	}

	p, ok := db[key]
	var v rpc.Tversion
	if !ok {
		v = 0
	} else {
		v = p.Version
	}

	if proposedVersion != v {
		DPrintf("Wrong version. Proposed: %v, current %v\n", proposedVersion, v)
		reply.Err = rpc.ErrVersion
		return &reply
	}

	kv.shard2kvdbase[shardId][key] = KDBEntry{val, proposedVersion + 1}

	cmd := Cmd{
		Key:     key,
		Value:   val,
		CId:     req.CId,
		RId:     req.RId,
		Version: proposedVersion + 1,
	}
	kv.client2latestCmd[req.CId] = cmd

	reply.Err = rpc.OK
	return &reply

}

func (kv *KVServer) handleFreezeRequest(req *shardrpc.FreezeShardArgs) *shardrpc.FreezeShardReply {
	reply := shardrpc.FreezeShardReply{}

	// invalid configuration, ignore
	// if it's the same config num, try to freeze again
	if kv.configNums[req.Shard] > req.Num {
		DPrintf("[FREEZE] already processed \n")
		reply.Err = rpc.OK
		reply.Num = kv.configNums[req.Shard]
		reply.State = nil
		return &reply
	}

	state, err := kv.encodeState(req.Shard)

	if err != nil {
		reply.Err = rpc.ErrWrongGroup
		return &reply
	}

	// freezes it
	kv.freeze[req.Shard] = true
	DPrintf("Freeze arr at %v for shard %v: %v\n", kv.me, req.Shard, kv.freeze)

	// update config num for freeze
	kv.configNums[req.Shard] = req.Num

	reply.Err = rpc.OK
	reply.Num = req.Num // not sure
	reply.State = state

	cmd := Cmd{
		CId:   req.CId,
		RId:   req.RId,
		Shard: req.Shard,
		Num:   req.Num,
	}

	kv.controller2latestCmd[req.CId] = cmd

	return &reply
}

func (kv *KVServer) handleInstallRequest(req *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {

	reply := shardrpc.InstallShardReply{}

	// invalid configuration, ignore
	if kv.configNums[req.Shard] >= req.Num {
		DPrintf("[INSTALL] Incorrect config num\n")
		reply.Err = rpc.OK
		return &reply
	}

	if req.State != nil {
		err := kv.decodeState(req.Shard, req.State)

		if err != nil {
			reply.Err = rpc.ErrWrongGroup
			DPrintf("Error during decoding state: %v\n", err)
			return &reply
		}
	} else {
		DPrintf("NOTHING IN INSTALL STATE\n")
		// kv.shard2kvdbase[req.Shard] = make(map[any]KDBEntry)
	}

	// install succeeded
	reply.Err = rpc.OK

	// gains ownership
	kv.ownedShards[req.Shard] = true

	// make sure shard is not frozen
	kv.freeze[req.Shard] = false

	// update config num for install
	kv.configNums[req.Shard] = req.Num

	cmd := Cmd{
		CId:   req.CId,
		RId:   req.RId,
		Shard: req.Shard,
		Num:   req.Num,
	}

	kv.client2latestCmd[req.CId] = cmd

	return &reply
}

func (kv *KVServer) handleDeleteRequest(req *shardrpc.DeleteShardArgs) *shardrpc.DeleteShardReply {
	reply := shardrpc.DeleteShardReply{}

	if kv.configNums[req.Shard] > req.Num {
		DPrintf("[DELETE] Incorrect config num\n")
		reply.Err = rpc.OK
		return &reply
	}

	if kv.configNums[req.Shard] == req.Num && !kv.ownedShards[req.Shard] {
		DPrintf("[DELETE] Delete already processed \n")
		reply.Err = rpc.OK
		return &reply
	}

	// if shard is not frozen -- probably optional
	if !kv.isFrozen(req.Shard) {
		DPrintf("[DELETE] Not frozen shard %v\n", req.Shard)
		reply.Err = rpc.ErrWrongGroup
		return &reply
	}

	// finally update gidOld Num
	kv.configNums[req.Shard] = req.Num

	// delete was complete
	kv.shard2kvdbase[req.Shard] = make(map[any]KDBEntry)
	reply.Err = rpc.OK

	kv.freeze[req.Shard] = false
	kv.ownedShards[req.Shard] = false

	cmd := Cmd{
		CId:   req.CId,
		RId:   req.RId,
		Shard: req.Shard,
		Num:   req.Num,
	}

	kv.controller2latestCmd[req.CId] = cmd

	return &reply
}
