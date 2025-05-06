package shardgrp

import (
	"time"

	"math/rand"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const waitTime = 100 * time.Microsecond
const maxAttempts = 1000

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	lastLeader int
	clientId   int32 // Unique ID for the client
	reqId      int   // Unique ID for the request
}

// generates a unique client ID
func MakeClerkId() int32 {
	return rand.Int31()
}

func (ck *Clerk) GetRequestId() int {
	ck.reqId++
	return ck.reqId
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.lastLeader = 0
	ck.reqId = 0
	ck.clientId = MakeClerkId()
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// Keep trying indefintely
	args := rpc.GetArgs{
		Key: key,
		CId: ck.clientId,
		RId: ck.GetRequestId(),
	}

	chosenIdx := ck.lastLeader
	i := 0
	for {

		DPrintf("[GET] at %s\n", ck.servers[chosenIdx%len(ck.servers)])
		reply := rpc.GetReply{}

		ok := ck.clnt.Call(ck.servers[chosenIdx%len(ck.servers)], "KVServer.Get", &args, &reply)

		if ok && reply.Err == rpc.ErrWrongGroup {
			DPrintf("[GET] returned WrongGroup at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			return "", 0, rpc.ErrWrongGroup
		}

		if ok && reply.Err == rpc.ErrNoKey {
			DPrintf("[GET] returned NoKey at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			return "", 0, rpc.ErrNoKey
		}

		if ok && reply.Err == rpc.OK {
			DPrintf("[GET] returned OK at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			ck.lastLeader = chosenIdx % len(ck.servers)
			return reply.Value, reply.Version, reply.Err
		}

		chosenIdx++
		if chosenIdx%len(ck.servers) == 0 {

			time.Sleep(waitTime)
		}

		if ok {
			i = 0
			continue
		}

		if !ok {
			DPrintf("[shardgrp GET] Reply not ok\n")
			i++
			if i == maxAttempts {
				break
			}
		}
	}

	DPrintf("[GET] reached max num of attempts; returned ErrWrongGroup\n")
	return "", 0, rpc.ErrWrongGroup
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {

	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
		CId:     ck.clientId,
		RId:     ck.GetRequestId(),
	}

	hasGoneTru := make([]bool, len(ck.servers))
	chosenIdx := ck.lastLeader
	i := 0
	for {

		reply := rpc.PutReply{}
		DPrintf("[PUT] at %s\n", ck.servers[chosenIdx%len(ck.servers)])

		ok := ck.clnt.Call(ck.servers[chosenIdx%len(ck.servers)], "KVServer.Put", &args, &reply)

		// First time trying
		if ok && !hasGoneTru[chosenIdx%len(ck.servers)] && reply.Err == rpc.ErrVersion {
			DPrintf("[PUT] returned ErrVersion at %s\n", ck.servers[chosenIdx%len(ck.servers)])

			return rpc.ErrVersion
		}

		hasGoneTru[chosenIdx%len(ck.servers)] = true

		if ok && reply.Err == rpc.ErrNoKey {
			DPrintf("[PUT] returned noKey at %s\n", ck.servers[chosenIdx%len(ck.servers)])

			return rpc.ErrNoKey
		}

		if ok && reply.Err == rpc.OK {
			DPrintf("[PUT] returned OK at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			ck.lastLeader = chosenIdx % len(ck.servers)

			return rpc.OK
		}

		if ok && reply.Err == rpc.ErrVersion && hasGoneTru[chosenIdx%len(ck.servers)] {
			DPrintf("[PUT] returned Maybe at %s\n", ck.servers[chosenIdx%len(ck.servers)])

			return rpc.ErrMaybe
		}

		if ok && reply.Err == rpc.ErrWrongGroup {
			DPrintf("[PUT] returned ErrWrongGroup at %s\n", ck.servers[chosenIdx%len(ck.servers)])

			return rpc.ErrWrongGroup
		}

		chosenIdx++
		if chosenIdx%len(ck.servers) == 0 {

			time.Sleep(waitTime)
		}

		if ok {
			i = 0
			continue
		}

		if !ok {
			DPrintf("[shardgrp PUT] Reply not ok\n")
			i++
			if i == maxAttempts {
				break
			}
		}

	}

	DPrintf("[PUT] reached max num of attempts; returned ErrWrongGroup\n")
	return rpc.ErrWrongGroup
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {

	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
		CId:   ck.clientId,
		RId:   ck.GetRequestId(),
	}

	chosenIdx := ck.lastLeader
	i := 0
	for {

		DPrintf("[FREEZE] at %s\n", ck.servers[chosenIdx%len(ck.servers)])
		reply := shardrpc.FreezeShardReply{}

		ok := ck.clnt.Call(ck.servers[chosenIdx%len(ck.servers)], "KVServer.FreezeShard", &args, &reply)

		if ok && reply.Err == rpc.ErrNoKey {
			DPrintf("[FREEZE] returned NoKey at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			return []byte{}, rpc.ErrNoKey
		}

		if ok && reply.Err == rpc.ErrWrongGroup {
			DPrintf("[FREEZE] returned WrongGroup at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			return []byte{}, rpc.ErrWrongGroup
		}

		if ok && reply.Err == rpc.OK {
			DPrintf("[FREEZE] returned OK at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			ck.lastLeader = chosenIdx % len(ck.servers)
			return reply.State, reply.Err
		}

		chosenIdx++
		if chosenIdx%len(ck.servers) == 0 {
			time.Sleep(waitTime)
		}

		if ok {
			i = 0
			continue
		}

		if !ok {
			DPrintf("[shardgrp FREEZE] Reply not ok\n")
			i++
			if i == maxAttempts {
				break
			}
		}

	}

	DPrintf("[FREEZE] reached max num of attempts; returned ErrWrongGroup\n")
	return []byte{}, rpc.ErrWrongGroup
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
		CId:   ck.clientId,
		RId:   ck.GetRequestId(),
	}

	chosenIdx := ck.lastLeader
	i := 0
	for {

		DPrintf("[SHARDGRP INSTALL] at %s\n", ck.servers[chosenIdx%len(ck.servers)])
		reply := shardrpc.InstallShardReply{}

		ok := ck.clnt.Call(ck.servers[chosenIdx%len(ck.servers)], "KVServer.InstallShard", &args, &reply)

		if ok && reply.Err == rpc.ErrNoKey {
			DPrintf("[SHARDGRP INSTALL] returned NoKey at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			return rpc.ErrNoKey
		}

		if ok && reply.Err == rpc.ErrWrongGroup {
			DPrintf("[SHARDGRP INSTALL] returned WrongGroup at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			return rpc.ErrWrongGroup
		}

		if ok && reply.Err == rpc.OK {
			DPrintf("[SHARDGRP INSTALL] returned OK at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			ck.lastLeader = chosenIdx % len(ck.servers)
			return reply.Err
		}

		chosenIdx++
		if chosenIdx%len(ck.servers) == 0 {
			time.Sleep(waitTime)
		}

		if ok {
			i = 0
			continue
		}

		if !ok {
			DPrintf("[shardgrp INSTALL] Reply not ok\n")
			i++
			if i == maxAttempts {
				break
			}
		}

	}

	DPrintf("[INSTALL] reached max num of attempts; returned ErrWrongGroup\n")
	return rpc.ErrWrongGroup
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
		CId:   ck.clientId,
		RId:   ck.GetRequestId(),
	}

	chosenIdx := ck.lastLeader
	i := 0
	for {

		DPrintf("[DELETE] at %s\n", ck.servers[chosenIdx%len(ck.servers)])
		reply := shardrpc.DeleteShardReply{}

		ok := ck.clnt.Call(ck.servers[chosenIdx%len(ck.servers)], "KVServer.DeleteShard", &args, &reply)

		if ok && reply.Err == rpc.ErrWrongGroup {
			DPrintf("[DELETE] returned WrongGroup at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			return reply.Err
		}

		if ok && reply.Err == rpc.OK {
			DPrintf("[DELETE] returned OK at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			ck.lastLeader = chosenIdx % len(ck.servers)
			return reply.Err
		}

		chosenIdx++
		if chosenIdx%len(ck.servers) == 0 {
			time.Sleep(waitTime)
		}

		if ok {
			i = 0
			continue
		}

		if !ok {
			DPrintf("[shardgrp DELETE] Reply not ok\n")
			i++
			if i == maxAttempts {
				break
			}
		}

	}

	DPrintf("[DELETE] reached max num of attempts; returned ErrWrongGroup\n")
	return rpc.ErrWrongGroup
}
