package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"

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
	curConfig *shardcfg.ShardConfig
	mu        sync.Mutex
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.curConfig = nil
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	configId := cfg.String()

	for {
		err := sck.IKVClerk.Put("config", configId, 0)

		if err == rpc.OK {
			sck.mu.Lock()
			sck.curConfig = cfg.Copy()
			sck.mu.Unlock()
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
	if new.Num <= sck.curConfig.Num {
		shardgrp.DPrintf("Wrong num at ChangeConfig\n")
		return
	}

	for shardId := 0; shardId < len(sck.curConfig.Shards); shardId++ {
		gidOld := sck.curConfig.Shards[shardId]
		gidNew := new.Shards[shardId]

		shId := shardcfg.Tshid(shardId)

		if gidOld == gidNew {
			continue
		}

		oldShardState, err := sck.freezeShard(shId, new.Num, gidOld)

		if err != rpc.OK {
			panic("Err in freeze ctrl shard\n")

		}

		err = sck.installShard(shId, oldShardState, new.Num, gidNew, new)

		if err != rpc.OK {
			panic("Err in install ctrl shard\n")

		}

		err = sck.deleteShard(shId, new.Num, gidOld)

		if err != rpc.OK {
			shardgrp.DPrintf("err: %v\n", err)
			panic("Err in delete ctrl shard\n")

		}

	}

	// Any old shards that need to be deleted?
	if len(new.Shards) < len(sck.curConfig.Shards) {
		shardgrp.DPrintf("[CTRL] Starting deleting old shards\n")
		for shardOld := len(new.Shards); shardOld < len(sck.curConfig.Shards); shardOld++ {
			gidOld := sck.curConfig.Shards[shardOld]
			shId := shardcfg.Tshid(shardOld)

			_, err := sck.freezeShard(shId, new.Num, gidOld)

			if err != rpc.OK {
				panic("Err in freeze ctrl shard\n")

			}

			err = sck.deleteShard(shId, new.Num, gidOld)

			if err != rpc.OK {
				panic("Err in delete ctrl shard\n")

			}

		}

		shardgrp.DPrintf("[CTRL] Done deleting old shards\n")

		// Any new shards that need to be added?
	} else if len(new.Shards) > len(sck.curConfig.Shards) {
		shardgrp.DPrintf("[CTRL] Starting adding new shards\n")
		for shardNew := len(sck.curConfig.Shards); shardNew < len(new.Shards); shardNew++ {
			gidNew := new.Shards[shardNew]
			shId := shardcfg.Tshid(shardNew)

			err := sck.installShard(shId, nil, new.Num, gidNew, new)

			if err != rpc.OK {
				panic("Err in delete ctrl shard\n")

			}

		}
		shardgrp.DPrintf("[CTRL] Done adding new shards\n")

	}

	sck.postNewConfig(new)

	sck.mu.Lock()
	sck.curConfig = new.Copy()
	sck.mu.Unlock()

	shardgrp.DPrintf("Done ChangeConfig\n")

}

// Freeze the shard in the current config and return its state
func (sck *ShardCtrler) freezeShard(shardId shardcfg.Tshid, num shardcfg.Tnum, gidOld tester.Tgid) ([]byte, rpc.Err) {
	ck := shardgrp.MakeClerk(sck.clnt, sck.curConfig.Groups[gidOld])

	for {
		state, err := ck.FreezeShard(shardId, num)

		if err == rpc.OK {
			return state, err
		}
	}
}

// Install the shard state in the new config
func (sck *ShardCtrler) installShard(shardId shardcfg.Tshid, state []byte, num shardcfg.Tnum, gidNew tester.Tgid, new *shardcfg.ShardConfig) rpc.Err {
	ck := shardgrp.MakeClerk(sck.clnt, new.Groups[gidNew])

	for {
		err := ck.InstallShard(shardId, state, num)

		if err == rpc.OK {
			return err
		}
	}
}

// Delete the shard in the old config
func (sck *ShardCtrler) deleteShard(shardId shardcfg.Tshid, num shardcfg.Tnum, gidOld tester.Tgid) rpc.Err {
	ck := shardgrp.MakeClerk(sck.clnt, sck.curConfig.Groups[gidOld])

	for {
		err := ck.DeleteShard(shardId, num)

		if err == rpc.OK {
			return err
		}
	}
}

// Replace config with new one
func (sck *ShardCtrler) postNewConfig(new *shardcfg.ShardConfig) {
	shardgrp.DPrintf("Beginning posting new config\n")

	sck.printConfigs(new)

	for {

		_, version, err := sck.IKVClerk.Get("config")

		if err != rpc.OK {
			continue
		}

		newConfigId := new.String()

		err = sck.IKVClerk.Put("config", newConfigId, version)

		// Succeeded changing the config
		if err == rpc.OK {
			shardgrp.DPrintf("ShardCtrler: ChangeConfigTo: changed")
			return
		}

	}

}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {

	// if sck.curConfig == nil {
	// sck.mu.Lock()
	// defer sck.mu.Unlock()
	for {

		cfgId, _, err := sck.IKVClerk.Get("config")

		if err == rpc.OK {
			return shardcfg.FromString(cfgId)
		}
	}
	// }

	// return sck.curConfig.Copy()

}

func (sck *ShardCtrler) printConfigs(new *shardcfg.ShardConfig) {

	shardgrp.DPrintf("====== OLD CONFIG (num: %v) =====\n", sck.curConfig.Num)
	for s, gid := range sck.curConfig.Shards {
		shardgrp.DPrintf("shard %v -> group %v\n", s, gid)
	}
	for g, s := range sck.curConfig.Groups {
		shardgrp.DPrintf("group %v : %v", g, s)
	}

	shardgrp.DPrintf("===== NEW CONFIG (num: %v)=====\n", new.Num)
	for s, gid := range new.Shards {
		shardgrp.DPrintf("shard %v -> group %v\n", s, gid)
	}
	for g, s := range new.Groups {
		shardgrp.DPrintf("group %v : %v", g, s)
	}

}
