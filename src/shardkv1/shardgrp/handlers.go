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

	if !kv.ownedShards[shardId] {
		reply.Err = rpc.ErrWrongGroup
		return &reply
	}

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

	reply := rpc.PutReply{}

	shardId := shardcfg.Key2Shard(key)

	if !kv.ownedShards[shardId] {
		reply.Err = rpc.ErrWrongGroup
		return &reply
	}

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
		DPrintf("[GET] GOT FROM DUP\n")
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

	// if shard is not owned by shardgrp
	if !kv.ownedShards[req.Shard] {
		DPrintf("[FREEZE] Not owner for shard %v at %v\n", req.Shard, kv.gid)
		reply.Err = rpc.ErrWrongGroup
		reply.Num = kv.configNums[req.Shard]
		return &reply
	}
	// invalid configuration
	if kv.configNums[req.Shard] > req.Num {
		DPrintf("[FREEZE] Incorrect config num\n")
		reply.Err = rpc.ErrWrongGroup
		reply.Num = kv.configNums[req.Shard]
		return &reply
	}

	// check for duplicates req
	ok, stateEntry := kv.CheckDuplicatesShard(req.CId, req.RId)
	if ok {
		DPrintf("[FREEZE] Duplicate request\n")
		reply.Err = rpc.OK
		reply.Num = stateEntry.Num

		state, err := kv.encodeState(req.Shard)

		if err != nil {
			reply.Err = rpc.ErrWrongGroup
			return &reply
		}

		reply.State = state
		return &reply
	}

	state, err := kv.encodeState(req.Shard)

	if err != nil {
		reply.Err = rpc.ErrWrongGroup
		return &reply
	}

	// freezes it
	kv.freeze = append(kv.freeze, req.Shard)
	DPrintf("Freeze arr at %v: %v\n", kv.me, kv.freeze)

	// loses ownership once freeze happens
	kv.ownedShards[req.Shard] = false
	kv.configNums[req.Shard] = req.Num

	reply.Err = rpc.OK
	reply.Num = req.Num // not sure
	reply.State = state

	cmd := Cmd{
		CId: req.CId,
		RId: req.RId,
	}

	kv.client2latestCmd[req.CId] = cmd

	return &reply
}

func (kv *KVServer) handleInstallRequest(req *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {

	reply := shardrpc.InstallShardReply{}

	ok, _ := kv.CheckDuplicatesShard(req.CId, req.RId)
	if ok {
		DPrintf("[INSTALL] Duplicate request\n")
		reply.Err = rpc.OK
		return &reply
	}

	// invalid configuration
	if kv.configNums[req.Shard] > req.Num {
		DPrintf("[INSTALL] Incorrect config num\n")
		reply.Err = rpc.ErrWrongGroup
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
		kv.shard2kvdbase[req.Shard] = make(map[any]KDBEntry)
	}

	// gains ownership
	kv.ownedShards[req.Shard] = true
	kv.configNums[req.Shard] = req.Num

	reply.Err = rpc.OK

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

	ok, _ := kv.CheckDuplicatesShard(req.CId, req.RId)
	if ok {
		DPrintf("[DELETE] Duplicate request\n")
		reply.Err = rpc.OK
		return &reply
	}

	if kv.configNums[req.Shard] > req.Num {
		DPrintf("[DELETE] Incorrect config num\n")
		reply.Err = rpc.ErrWrongGroup
		return &reply
	}

	// if shard is not frozen
	if !kv.isFrozen(req.Shard) {
		DPrintf("[DELETE] Not frozen shard %v\n", req.Shard)
		reply.Err = rpc.ErrWrongGroup
		return &reply
	}

	kv.shard2kvdbase[req.Shard] = make(map[any]KDBEntry)
	kv.configNums[req.Shard] = req.Num
	kv.freeze = removeShardFromFreeze(kv.freeze, req.Shard)
	reply.Err = rpc.OK

	cmd := Cmd{
		CId:   req.CId,
		RId:   req.RId,
		Shard: req.Shard,
		Num:   req.Num,
	}

	kv.client2latestCmd[req.CId] = cmd

	return &reply
}
