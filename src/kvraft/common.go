package kvraft

import (
	"encoding/base64"
	"strconv"
	"sync/atomic"
)

const (
	OK                    = "OK"
	ErrNoKey              = "ErrNoKey"
	ErrWrongLeader        = "ErrWrongLeader"
	ErrOutOfOrderDelivery = "ErrOutOfOrderDelivery"
	ErrLogEntryErased     = "ErrLogEntryErased"
)

type OperationType int

const (
	GET OperationType = iota
	PUT
	APPEND
)

type Err string

type OperationArgs struct {
	Key   string
	Value string        // not used in "Get"
	Type  OperationType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	SeqNum  int
}

type OperationReply struct {
	Err   Err
	Value string // only used by Get
	// used for the client to redirect message to leader
	LeaderId int
	TermId   int
}

// int64ToBase64 converts an int64 number to a base64 encoded string.
// used for logging purpose
func int64ToBase64(number int64) string {
	bytes := []byte(strconv.FormatInt(number, 10))
	return base64.StdEncoding.EncodeToString(bytes)
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
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
