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
const waitTimeCtrler = 25 * time.Millisecond
const timeoutTime = 800 * time.Millisecond

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
	args := rpc.GetArgs{
		Key: key,
		CId: ck.clientId,
		RId: ck.GetRequestId(),
	}

	timeout := time.After(timeoutTime)
	chosenIdx := ck.lastLeader
	for {
		select {
		case <-timeout:
			DPrintf("[GET] reached timeout; returned ErrWrongGroup\n")
			return "", 0, rpc.ErrWrongGroup

		default:

			DPrintf("[GET] at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			reply := rpc.GetReply{}

			ok := ck.clnt.Call(ck.servers[chosenIdx%len(ck.servers)], "KVServer.Get", &args, &reply)

			// not a leader, try next server
			if ok && reply.Err == rpc.ErrWrongLeader {
				DPrintf("[GET] returned WrongLeader at %s\n", ck.servers[chosenIdx%len(ck.servers)])
				chosenIdx++
				if chosenIdx%len(ck.servers) == 0 {
					time.Sleep(waitTime)
				}
				continue
			}

			// rpc call was successful and it is a leader
			if ok {
				ck.lastLeader = chosenIdx % len(ck.servers)
				if reply.Err == rpc.ErrWrongGroup {
					DPrintf("[GET] returned WrongGroup at %s\n", ck.servers[chosenIdx%len(ck.servers)])
					return "", 0, rpc.ErrWrongGroup
				}

				if reply.Err == rpc.OK {
					DPrintf("[GET] returned OK at %s\n", ck.servers[chosenIdx%len(ck.servers)])
					return reply.Value, reply.Version, reply.Err
				}

				if reply.Err == rpc.ErrNoKey {
					DPrintf("[GET] returned NoKey at %s\n", ck.servers[chosenIdx%len(ck.servers)])

					return "", 0, rpc.ErrNoKey
				}
				panic("Unexpected error in get")

			}

			// call failed, try next server
			chosenIdx++
			if chosenIdx%len(ck.servers) == 0 {
				time.Sleep(waitTime)
			}
		}
	}

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
	timeout := time.After(timeoutTime)
	for {
		select {
		case <-timeout:
			DPrintf("[PUT] reached timeout; returned ErrWrongGroup\n")
			return rpc.ErrWrongGroup
		default:
			reply := rpc.PutReply{}
			DPrintf("[PUT] at %s\n", ck.servers[chosenIdx%len(ck.servers)])

			ok := ck.clnt.Call(ck.servers[chosenIdx%len(ck.servers)], "KVServer.Put", &args, &reply)

			// not a leader, try next server
			if ok && reply.Err == rpc.ErrWrongLeader {
				chosenIdx++
				if chosenIdx%len(ck.servers) == 0 {
					time.Sleep(waitTime)
				}
				continue
			}

			// rpc call was successful and it is a leader
			if ok {
				ck.lastLeader = chosenIdx % len(ck.servers)
				// First time trying
				if !hasGoneTru[chosenIdx%len(ck.servers)] && reply.Err == rpc.ErrVersion {
					DPrintf("[PUT] returned ErrVersion at %s\n", ck.servers[chosenIdx%len(ck.servers)])

					return rpc.ErrVersion
				}

				hasGoneTru[chosenIdx%len(ck.servers)] = true

				if reply.Err == rpc.ErrNoKey {
					DPrintf("[PUT] returned noKey at %s\n", ck.servers[chosenIdx%len(ck.servers)])

					return rpc.ErrNoKey
				}

				if reply.Err == rpc.OK {
					DPrintf("[PUT] returned OK at %s\n", ck.servers[chosenIdx%len(ck.servers)])

					return rpc.OK
				}

				if reply.Err == rpc.ErrVersion {
					DPrintf("[PUT] returned Maybe at %s\n", ck.servers[chosenIdx%len(ck.servers)])

					return rpc.ErrMaybe
				}

				if reply.Err == rpc.ErrWrongGroup {
					DPrintf("[PUT] returned ErrWrongGroup at %s\n", ck.servers[chosenIdx%len(ck.servers)])

					return rpc.ErrWrongGroup
				}

				panic("Unexpected error in put")

			}

			// call failed, try next server
			chosenIdx++
			if chosenIdx%len(ck.servers) == 0 {

				time.Sleep(waitTime)
			}
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum, sckId int32) ([]byte, rpc.Err) {

	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
		CId:   ck.clientId,
		RId:   ck.GetRequestId(),
	}

	for {

		DPrintf("[FREEZE] at %s\n", ck.servers[ck.lastLeader%len(ck.servers)])
		reply := shardrpc.FreezeShardReply{}

		ok := ck.clnt.Call(ck.servers[ck.lastLeader%len(ck.servers)], "KVServer.FreezeShard", &args, &reply)

		// rpc call was successful and it is a leader
		if ok && reply.Err != rpc.ErrWrongLeader {

			if reply.Err == rpc.OK {
				DPrintf("[FREEZE] returned OK at %s\n", ck.servers[ck.lastLeader%len(ck.servers)])

				return reply.State, reply.Err
			}

			if reply.Err == rpc.ErrVersion && reply.Num >= num {
				// freeze + delete has already run successfully
				DPrintf("[FREEZE] returned ErrVersion at %s, sck Id: %v, reply.Num: %v, num: %v\n", ck.servers[ck.lastLeader%len(ck.servers)], sckId, reply.Num, num)
				return nil, rpc.OK
			}

		}

		// call failed, try next server
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(waitTimeCtrler)
		DPrintf("[FREEZE] call failed, trying next server\n")
	}

}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
		CId:   ck.clientId,
		RId:   ck.GetRequestId(),
	}

	for {

		DPrintf("[SHARDGRP INSTALL] at %s\n", ck.servers[ck.lastLeader%len(ck.servers)])
		reply := shardrpc.InstallShardReply{}

		ok := ck.clnt.Call(ck.servers[ck.lastLeader%len(ck.servers)], "KVServer.InstallShard", &args, &reply)

		// rpc call was successful and it is a leader
		if ok && reply.Err != rpc.ErrWrongLeader {

			if reply.Err == rpc.OK {
				DPrintf("[SHARDGRP INSTALL] returned OK at %s\n", ck.servers[ck.lastLeader%len(ck.servers)])
				return reply.Err
			}

			// time.Sleep(waitTimeCtrler)
			continue

		}

		// call failed, try next server
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(waitTimeCtrler)
		DPrintf("[SHARDGRP INSTALL] call failed, trying next server\n")

	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
		CId:   ck.clientId,
		RId:   ck.GetRequestId(),
	}

	for {

		DPrintf("[DELETE] at %s\n", ck.servers[ck.lastLeader%len(ck.servers)])
		reply := shardrpc.DeleteShardReply{}

		ok := ck.clnt.Call(ck.servers[ck.lastLeader%len(ck.servers)], "KVServer.DeleteShard", &args, &reply)

		// rpc call was successful and it is a leader
		if ok && reply.Err != rpc.ErrWrongLeader {

			if reply.Err == rpc.OK {
				DPrintf("[DELETE] returned OK at %s\n", ck.servers[ck.lastLeader%len(ck.servers)])
				return reply.Err
			}
			// time.Sleep(waitTimeCtrler)
			continue
		}

		// call failed, try next server
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(waitTimeCtrler)
		DPrintf("[DELETE] call failed, trying next server\n")
	}

}
