# 5D - Enabling Raft for Controller ShardKV 

In this extension, I tackle the problem of converting KV system for the controler from `kvsrv` system to a `kvraft` system. This allows for increased replication and availability of the system to retrieve the configurations at the cost of lower performance and throughput, since Raft spends some time electing leaders.

## Main Modification

The main design was to replace the controler KV server to a KVRaft server in `MakeShardCtrler`. In this case, the choice for server was `Tgid(0)` and it was allocated a variable number of peers: 3 and 5. I manually changed them for each test but that could've been easily automated.

```go
// Make a ShardCltler, which stores its state in a kvraft srv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
    sck := &ShardCtrler{clnt: clnt}

    // ------ BEGIN OF MOD

    // Example test raft with 3 peers
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

The design of the test case was similar to `TestJoinLeaveBasic5A` where the semantics are similar, but an additional configuration querying was done – all of that while one of the peers was down. The query had a time limit of 2 seconds, a par with other test cases.

```go
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
		// query returned on time
	case <-time.After(2 * time.Second):
		ts.Fatalf("Querying didn't complete on time")
	}

}
```

## Results

The results worked as expected. KVRaft works but it is considerably more slow than the original KVServer. The more peers were added, the longer time it took to complete the task due to elections. However, tests regarding availability are needed and performance comparison between both KV systems would bring better data points to compare them.

#### 3 peers
```zsh
Test (5D): Config with one peer down ... (reliable network)...
  ... Passed --  time 32.4s #peers 3 #RPCs  9007 #Ops  120
PASS
ok      6.5840/shardkv1 32.609s
```

#### 5 peers
```zsh
Test (5D): Config with one peer down ... (reliable network)...
  ... Passed --  time 51.2s #peers 5 #RPCs  9580 #Ops  120
PASS
ok      6.5840/shardkv1 51.458s
```
