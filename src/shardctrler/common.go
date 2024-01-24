package shardctrler

import (
	"encoding/base64"
	"strconv"
	"sync/atomic"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// used to control whether or not print debugging info
const ControllerDebug = false

// The number of shards.
const NShards = 10

// used to shorten the uid of a client for logging
const PrefixLength = 5

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid (many-to-one mapping)
	Groups map[int][]string // gid -> servers[] (one-to-many mapping)
}

const (
	OK                = "OK"                // applied
	ErrWrongLeader    = "ErrWrongLeader"    // the server the clerk talked to is not leader
	ErrLogEntryErased = "ErrLogEntryErased" // previous index is detected with a new log entry, definitely not applied
	ErrTermChanged    = "ErrTermChanged"    // Periodic term detector found that the term of the previous leader has changed, no guarantee if the command will be applied or erased.
)

type ControllerOperationType int

const (
	JOIN ControllerOperationType = iota
	LEAVE
	MOVE
	QUERY
)

type Err string

type ControllerOperationArgs struct {
	Servers  map[int][]string // used for Join
	GIDs     []int            // used for Leave
	Shard    int              // used for Move
	GID      int              // used for Move
	ConfigID int              // used for Query
	Type     ControllerOperationType
	ClerkId  int64
	SeqNum   int
}
type ControllerOperationReply struct {
	Err    Err
	Config *Config // used by query operation
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
