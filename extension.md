# 5D - Enabling Raft for Controller ShardKV 


## Main Modification
```
// Make a ShardCltler, which stores its state in a kvraft srv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
    sck := &ShardCtrler{clnt: clnt}

    // ------ BEGIN OF MOD

    // For now, test raft with 3 peers
    srv1 := tester.ServerName(tester.Tgid(0), 1) // group 0 for a kvraft group
    srv2 := tester.ServerName(tester.Tgid(0), 2) 
    srv3 := tester.ServerName(tester.Tgid(0), 3)

    // changed here to raft for 5D
    sck.IKVClerk = kvraft.MakeClerk(clnt, []string{srv1, srv2, srv3})
    // ------ END OF MOD

    sck.curConfig = nil
    sck.clerks = make(map[tester.Tgid]*shardgrp.Clerk)
    sck.Id = rand.Int31()

    return sck
}
```

## Test case

```
// checks that the controller can query and update the
// configuration while one of the kvraft peers is down.
func TestRaftPeerDown5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): Config with one peer down ...", /*reliable=*/true)
	defer ts.Cleanup()

	// The tester's setupKVService() sets up a kvsrv for the
	// controller to store configurations and calls the controller's
	// Init() method to create the first configuration with 1
	// shardgrp.
	ts.setupKVService()

	ck := ts.MakeClerk()               // make a shardkv clerk
	ka, va := ts.SpreadPuts(ck, NKEYS) // do some puts
	sck := ts.ShardCtrler()

	// take a peer down
	ts.Group(GID0).ShutdownServer(0)

	// change config
	gid2 := ts.newGid()
	if ok := ts.joinGroups(sck, []tester.Tgid{gid2}); !ok {
		ts.t.Fatalf("TestRaftPeerDown5D: joinGroups failed")
	}

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	ts.leave(sck, shardcfg.Gid1)
	if ok := ts.checkMember(sck, shardcfg.Gid1); ok {
		ts.t.Fatalf("%d is a member after leave", shardcfg.Gid1)
	}

	// check if can query
	ch := make(chan *shardcfg.ShardConfig)
	go func() {
		ch <- sck.Query()
	}()

	select {
	case <-ch:
		// ts.Fatalf("Query didn't finished %v", err)
	case <-time.After(2 * time.Second):
		ts.Fatalf("Querying didn't complete on time")
	}

}
```