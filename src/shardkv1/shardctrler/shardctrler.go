package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"math/rand"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	killed int32 // set by Kill()

	// Your data here.
	curConfig  *shardcfg.ShardConfig
	nextConfig *shardcfg.ShardConfig
	Id         int32
	clerks     map[tester.Tgid]*shardgrp.Clerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.curConfig = nil
	sck.clerks = make(map[tester.Tgid]*shardgrp.Clerk)
	sck.Id = rand.Int31()
	// Your code here.
	return sck
}

func (sck *ShardCtrler) getClerk(gid tester.Tgid, new *shardcfg.ShardConfig) *shardgrp.Clerk {
	ck, ok := sck.clerks[gid]

	if !ok {
		servers := sck.curConfig.Groups[gid]

		if len(servers) == 0 {
			servers = new.Groups[gid]
		}
		sck.clerks[gid] = shardgrp.MakeClerk(sck.clnt, servers)
		ck = sck.clerks[gid]
	}

	return ck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	shardgrp.DPrintf("ShardCtrler: InitController\n")
	for {
		time.Sleep(1 * time.Millisecond)

		cfgId, _, err := sck.IKVClerk.Get("config")

		if err == rpc.ErrNoKey {
			return
		}

		if err == rpc.OK {
			sck.curConfig = shardcfg.FromString(cfgId)
			break
		}
	}

	shardgrp.DPrintf("COmpleted getting curConfig\n")
	for {
		time.Sleep(1 * time.Millisecond)
		// shardgrp.DBPrintf("ShardCtrler: InitController: curConfig num is %v\n", sck.curConfig.Num)
		cfgId, _, err := sck.IKVClerk.Get("nextConfig")

		if err == rpc.ErrNoKey {
			return
		}

		if err == rpc.OK {
			sck.nextConfig = shardcfg.FromString(cfgId)
			break
		}
	}
	// shardgrp.DPrintf("ShardCtrler: InitController: curConfig num is %v\n", sck.curConfig.Num)
	// shardgrp.DPrintf("ShardCtrler: InitController: nextConfig is num %v\n", sck.nextConfig.Num)
	if sck.nextConfig.Num > sck.curConfig.Num {
		shardgrp.DPrintf("ShardCtrler: InitController: nextConfig is %v\n", sck.nextConfig)
		sck.ChangeConfigTo(sck.nextConfig)
	}

}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	configId := cfg.String()

	for {
		time.Sleep(1 * time.Millisecond)

		err := sck.IKVClerk.Put("config", configId, 0)

		if err == rpc.OK {

			sck.curConfig = cfg.Copy()
			break
		}
	}

	new := &shardcfg.ShardConfig{
		Num: -1,
	}

	mockConfigId := new.String()
	for {
		time.Sleep(1 * time.Millisecond)
		// shardgrp.DBPrintf("ShardCtrler: InitConfig: mockConfigId is %v\n", mockConfigId)

		err := sck.IKVClerk.Put("nextConfig", mockConfigId, 0)

		if err == rpc.OK {
			return
		}
	}

}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	shardgrp.DPrintf("Beginning ChnageConfigTo\n")

	sck.curConfig = sck.Query()
	if new.Num <= sck.curConfig.Num {
		shardgrp.DPrintf("Wrong num at ChangeConfig\n")
		return
	}

	if ok := sck.postNewConfig(new); !ok {
		shardgrp.DBPrintf("Cntrler %v failed to change config to %v\n", sck.Id, new.Num)
		return
	}

	oldStates := make(map[shardcfg.Tshid][]byte)
	for shardId := 0; shardId < shardcfg.NShards; shardId++ {
		gidOld := sck.curConfig.Shards[shardId]
		gidNew := new.Shards[shardId]

		shId := shardcfg.Tshid(shardId)

		if gidOld == gidNew {
			continue
		}

		ck := sck.getClerk(gidOld, new)

		shardgrp.DPrintf("Freezing shard %v in group %v sck %v\n", shId, gidOld, sck.Id)
		oldShardState, err := ck.FreezeShard(shId, new.Num, sck.Id)

		if err != rpc.OK {
			panic("Err in freeze ctrl shard\n")

		}

		oldStates[shId] = oldShardState

	}

	for shardId := 0; shardId < shardcfg.NShards; shardId++ {
		gidOld := sck.curConfig.Shards[shardId]
		gidNew := new.Shards[shardId]

		shId := shardcfg.Tshid(shardId)

		if gidOld == gidNew {
			continue
		}

		ck := sck.getClerk(gidNew, new)
		err := ck.InstallShard(shId, oldStates[shId], new.Num)

		if err != rpc.OK {
			panic("Err in install ctrl shard\n")

		}

	}

	for shardId := 0; shardId < shardcfg.NShards; shardId++ {
		gidOld := sck.curConfig.Shards[shardId]
		gidNew := new.Shards[shardId]

		shId := shardcfg.Tshid(shardId)

		if gidOld == gidNew {
			continue
		}

		ck := sck.getClerk(gidOld, new)
		err := ck.DeleteShard(shId, new.Num)

		if err != rpc.OK {
			shardgrp.DPrintf("err: %v\n", err)
			panic("Err in delete ctrl shard\n")

		}

	}

	sck.updateConfig(new)

	sck.curConfig = new.Copy()

	shardgrp.DBPrintf("cntrler %v: Done ChangeConfig to %v\n", sck.Id, new.Num)

}

// Replace config with new one, return true if succeeded
// and false if the config has been changed by another controller
func (sck *ShardCtrler) postNewConfig(new *shardcfg.ShardConfig) bool {
	shardgrp.DPrintf("Beginning posting new config\n")

	// sck.printConfigs(new)/

	for {

		time.Sleep(10 * time.Millisecond)
		_, version, err := sck.IKVClerk.Get("nextConfig")
		// shardgrp.DBPrintf("ShardCtrler: postNewConfig: version is %v\n", version)
		if err != rpc.OK {
			continue
		}

		// if version is not the previous version,
		// return false
		if shardcfg.Tnum(version) > new.Num-1 {
			shardgrp.DBPrintf("Current version is: %v, new version is %v\n", version, new.Num)
			return false
		}

		newConfigId := new.String()

		err = sck.IKVClerk.Put("nextConfig", newConfigId, version)

		// Succeeded changing the config
		if err == rpc.OK {
			shardgrp.DBPrintf("ShardCtrler %v: new config posted", sck.Id)
			return true
		}

		if err == rpc.ErrVersion {

		}

		shardgrp.DBPrintf("ShardCtrler %v: failed to post new config, trying it again\n", sck.Id)

	}

}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {

	for {
		// time.Sleep(1 * time.Millisecond)
		cfgId, _, err := sck.IKVClerk.Get("config")

		if err == rpc.OK {
			return shardcfg.FromString(cfgId)
		}
	}

}

func (sck *ShardCtrler) printConfigs(new *shardcfg.ShardConfig) {

	if !shardgrp.Debug {

		shardgrp.DBPrintf("====== OLD CONFIG (num: %v) =====\n", sck.curConfig.Num)
		for s, gid := range sck.curConfig.Shards {
			shardgrp.DBPrintf("shard %v -> group %v\n", s, gid)
		}
		for g, s := range sck.curConfig.Groups {
			shardgrp.DBPrintf("group %v : %v", g, s)
		}

		shardgrp.DBPrintf("===== NEW CONFIG (num: %v)=====\n", new.Num)
		for s, gid := range new.Shards {
			shardgrp.DBPrintf("shard %v -> group %v\n", s, gid)
		}
		for g, s := range new.Groups {
			shardgrp.DBPrintf("group %v : %v", g, s)
		}
	}

}

func (sck *ShardCtrler) updateConfig(new *shardcfg.ShardConfig) {
	sck.nextConfig = new.Copy()
	newConfigId := new.String()

	shardgrp.DPrintf("Beginning updating config\n")

	// sck.printConfigs(new)

	for {
		time.Sleep(1 * time.Millisecond)

		_, version, err := sck.IKVClerk.Get("config")

		if err != rpc.OK {
			continue
		}

		err = sck.IKVClerk.Put("config", newConfigId, version)

		if err == rpc.OK {
			shardgrp.DPrintf("ShardCtrler: updated config")
			return
		}

	}
}
