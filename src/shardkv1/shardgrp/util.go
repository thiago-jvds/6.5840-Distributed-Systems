package shardgrp

import (
	"bytes"
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/shardkv1/shardcfg"
)

// Debugging
const Debug = false
const special = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func DBPrintf(format string, a ...interface{}) {
	if special {
		log.Printf(format, a...)
	}
}

type Cmd struct {
	// GET and PUT requests

	Key     string
	Value   string
	Version rpc.Tversion

	// FREEZE, INSTALL, DELETE requests

	Shard shardcfg.Tshid
	Num   shardcfg.Tnum

	// common

	CId int32
	RId int
}

type KDBEntry struct {
	Value   string
	Version rpc.Tversion
}

type ShardEntry struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

func (kv *KVServer) CheckDuplicates(clientId int32, requestId int) (bool, KDBEntry) {
	op, ok := kv.client2latestCmd[clientId]

	if !ok || op.RId < requestId {
		return false, KDBEntry{"", 0}
	}
	return true, KDBEntry{op.Value, op.Version}
}

func (kv *KVServer) CheckDuplicatesShard(clientId int32, requestId int) (bool, ShardEntry) {

	op, ok := kv.controller2latestCmd[clientId]

	if !ok || op.RId < requestId {
		return false, ShardEntry{}
	}

	return true, ShardEntry{Shard: op.Shard, Num: op.Num}
}

func (kv *KVServer) isFrozen(shardId shardcfg.Tshid) bool {

	isIn, ok := kv.freeze[shardId]

	return ok && isIn
}

func (kv *KVServer) encodeState(shardId shardcfg.Tshid) ([]byte, error) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shard2kvdbase[shardId])

	if err := e.Encode(kv.shard2kvdbase[shardId]); err != nil {
		DPrintf("Error in encoding kvdbase")
		return nil, err
	}

	return w.Bytes(), nil

}

func (kv *KVServer) decodeState(shardId shardcfg.Tshid, state []byte) error {

	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var kvdbase map[any]KDBEntry

	if d.Decode(&kvdbase) != nil {
		DPrintf("Error in decoding kvdbase")
	} else {
		kv.shard2kvdbase[shardId] = kvdbase
	}

	return nil
}

func removeShardFromFreeze(freeze []shardcfg.Tshid, shard shardcfg.Tshid) []shardcfg.Tshid {
	for i, frozenShard := range freeze {
		if frozenShard == shard {
			return append(freeze[:i], freeze[i+1:]...)
		}
	}
	return freeze // Shard not found, return original slice
}
