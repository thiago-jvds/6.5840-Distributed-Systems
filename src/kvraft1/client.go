package kvraft

import (
	"time"

	"math/rand"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

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

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.reqId = 0
	ck.clientId = MakeClerkId()
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// Keep trying indefintely
	args := rpc.GetArgs{
		Key: key,
		CId: ck.clientId,
		RId: ck.GetRequestId(),
	}

	chosenIdx := ck.lastLeader
	for {

		DPrintf("Clerk: calling GET at %s\n", ck.servers[chosenIdx%len(ck.servers)])
		reply := rpc.GetReply{}

		ok := ck.clnt.Call(ck.servers[chosenIdx%len(ck.servers)], "KVServer.Get", &args, &reply)

		if ok && reply.Err == rpc.ErrNoKey {
			DPrintf("Clerk: GET returned NoKey at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			return "", 0, rpc.ErrNoKey
		}

		if ok && reply.Err == rpc.OK {
			DPrintf("Clerk: GET returned OK at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			ck.lastLeader = chosenIdx % len(ck.servers)
			return reply.Value, reply.Version, reply.Err
		}

		chosenIdx++
		time.Sleep(10 * time.Millisecond)

	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
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
	for {

		reply := rpc.PutReply{}
		DPrintf("Clerk: calling PUT at %s\n", ck.servers[chosenIdx%len(ck.servers)])

		ok := ck.clnt.Call(ck.servers[chosenIdx%len(ck.servers)], "KVServer.Put", &args, &reply)

		// First time trying
		if ok && !hasGoneTru[chosenIdx%len(ck.servers)] && reply.Err == rpc.ErrVersion {
			DPrintf("Clerk: PUT returned ErrVersion at %s\n", ck.servers[chosenIdx%len(ck.servers)])

			return rpc.ErrVersion
		}

		hasGoneTru[chosenIdx%len(ck.servers)] = true

		if ok && reply.Err == rpc.ErrNoKey {
			DPrintf("Clerk: PUT returned noKey at %s\n", ck.servers[chosenIdx%len(ck.servers)])

			return rpc.ErrNoKey
		}

		if ok && reply.Err == rpc.OK {
			DPrintf("Clerk: PUT returned OK at %s\n", ck.servers[chosenIdx%len(ck.servers)])
			ck.lastLeader = chosenIdx % len(ck.servers)

			return rpc.OK
		}

		if ok && reply.Err == rpc.ErrVersion {
			DPrintf("Clerk: PUT returned Maybe at %s\n", ck.servers[chosenIdx%len(ck.servers)])

			return rpc.ErrMaybe
		}

		chosenIdx++
		time.Sleep(10 * time.Millisecond)

	}
}
