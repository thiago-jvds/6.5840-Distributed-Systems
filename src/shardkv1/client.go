package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	sck    *shardctrler.ShardCtrler
	mu     sync.Mutex
	clerks map[tester.Tgid]*shardgrp.Clerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:   clnt,
		sck:    sck,
		clerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}

	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	oldcfg := ck.sck.Query()
	for {
		shardgrp.DPrintf("[CLERK CLIENT GET] Init Get\n")

		shard := shardcfg.Key2Shard(key)

		gid, _, _ := oldcfg.GidServers(shard)

		clerk := ck.getClerks(gid, oldcfg)

		val, version, err := clerk.Get(key)

		if err == rpc.OK {
			return val, version, err
		}

		if err == rpc.ErrNoKey {
			newCfg := ck.sck.Query()

			if oldcfg.String() == newCfg.String() {
				return "", 0, rpc.ErrNoKey
			}

			oldcfg = newCfg
			continue
		}

		if err == rpc.ErrWrongGroup {
			oldcfg = ck.sck.Query()
			continue
		}
	}

}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	hasTried := false
	oldcfg := ck.sck.Query()

	for {
		shardgrp.DPrintf("[CLERK CLIENT PUT] Init Put\n")

		shard := shardcfg.Key2Shard(key)

		gid, _, _ := oldcfg.GidServers(shard)

		clerk := ck.getClerks(gid, oldcfg)

		err := clerk.Put(key, value, version)

		if !hasTried && err == rpc.ErrVersion {
			return rpc.ErrVersion
		}

		hasTried = true

		if err == rpc.OK {
			return err
		}

		if err == rpc.ErrNoKey {
			newCfg := ck.sck.Query()

			if oldcfg.String() == newCfg.String() {
				return rpc.ErrNoKey
			}

			oldcfg = newCfg
			continue
		}

		if err == rpc.ErrVersion && hasTried {
			return rpc.ErrMaybe
		}

		if err == rpc.ErrWrongGroup {
			oldcfg = ck.sck.Query()
			continue
		}

	}
}

func (ck *Clerk) getClerks(gid tester.Tgid, cfg *shardcfg.ShardConfig) *shardgrp.Clerk {
	ckG, ok := ck.clerks[gid]

	if ok {
		return ckG
	}

	servers := cfg.Groups[gid]
	ckG = shardgrp.MakeClerk(ck.clnt, servers)
	ck.clerks[gid] = ckG
	return ckG
}
