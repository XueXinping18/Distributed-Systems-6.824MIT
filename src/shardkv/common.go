package shardkv

import (
	"6.824/shardctrler"
	"encoding/base64"
	"strconv"
	"sync/atomic"
)

// used to control whether or not print debugging info
const ShardKVDebug = false

// used to shorten the uid of a client for logging
const PrefixLength = 5

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type OperationType int

const (
	GET OperationType = iota
	PUT
	APPEND
	INSTALLSHARD
	INCREMENTCONFIG
)

type Err string

// Sharded key/value server.
// Lots of replica groups, each running Raft.
// ShardCtrler decides which group serves each shard.
// ShardCtrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
const (
	OK                = "OK"                // applied
	ErrNoKey          = "ErrNoKey"          // for GET, applied but no key
	ErrWrongLeader    = "ErrWrongLeader"    // the server the clerk talked to is not leader
	ErrWrongGroup     = "ErrWrongGroup"     // the group does not serve the request either because the shard configuration has changed
	ErrShardNotReady  = "ErrShardNotReady"  // the group should serve the request but still waiting for the shard to arrive
	ErrLogEntryErased = "ErrLogEntryErased" // previous index is detected with a new log entry, definitely not applied
	ErrTermChanged    = "ErrTermChanged"    // Periodic term detector found that the term of the previous leader has changed, no guarantee if the command will be applied or erased.
)

type KVOperationArgs struct {
	Key     string
	Value   string        // not used in Get
	Type    OperationType // Put, Append or Get
	ClerkId int64         // to identify the client
	SeqNum  int           // to make operation idempotent and eliminate ABA issue
}

type KVOperationReply struct {
	Err   Err
	Value string // only used in Get
}

type InstallShardArgs struct {
	Version        int // the new configID i.e. all the changes on the shards before this configID have been reflected to the shards
	Shard          int // shardId
	Data           map[string]string
	SourceGid      int
	DuplicateTable map[int64]*RpcResponse // the duplication table so that one operation will not be executed in both group when configuration changes
}
type InstallShardReply struct {
	Err Err // similar error logic as in client request, except that the error result can't be ErrWrongGroup or ErrShardNotReady
}

// int64ToBase64 converts an int64 number to a base64 encoded string.
// used for logging purpose
func int64ToBase64(number int64) string {
	bytes := []byte(strconv.FormatInt(number, 10))
	return base64.StdEncoding.EncodeToString(bytes)
}
func base64Prefix(number int64) string {
	return int64ToBase64(number)[0:PrefixLength]
}

// used to increment an atomic number
func atomicIncrementAndSwap(val *int32) int32 {
	var oldValue int32
	for {
		oldValue = atomic.LoadInt32(val)
		// Try to update the value with oldValue + 1
		if atomic.CompareAndSwapInt32(val, oldValue, oldValue+1) {
			// If successful, break out of the loop
			break
		}
		// Otherwise, the loop will continue and try again
	}
	return oldValue
}
