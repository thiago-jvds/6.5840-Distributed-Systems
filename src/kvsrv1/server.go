package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Pair struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu  sync.Mutex
	Map map[string]*Pair
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.Map = make(map[string]*Pair)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key

	p, ok := kv.Map[key]

	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = p.Value
		reply.Version = p.Version
		reply.Err = rpc.OK
	}

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	proposedVersion := args.Version
	key := args.Key

	p, ok := kv.Map[key]

	// Key doesn't exist and Version is not 0
	if !ok && proposedVersion != 0 {
		reply.Err = rpc.ErrNoKey
		return

		// Installs the key if doesn't exist and version is 0
	} else if !ok && proposedVersion == 0 {
		newP := Pair{Value: args.Value, Version: 1}
		kv.Map[key] = &newP
		reply.Err = rpc.OK
		return

		// Key exists and Version doesn't match
	} else if ok && proposedVersion != p.Version {
		reply.Err = rpc.ErrVersion
		return

		// Key exists and Version matches
	} else if ok && proposedVersion == p.Version {
		p.Version = p.Version + 1
		p.Value = args.Value
		reply.Err = rpc.OK
		return
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
