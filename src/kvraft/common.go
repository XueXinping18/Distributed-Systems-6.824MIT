package kvraft

import (
	"encoding/base64"
	"strconv"
	"sync/atomic"
)

// used to control whether or not print debugging info
const KVDebug = false

type Err string

const (
	OK                    = "OK"                    // applied
	ErrNoKey              = "ErrNoKey"              // for GET, applied but no key
	ErrWrongLeader        = "ErrWrongLeader"        // the server the clerk talked to is not leader
	ErrOutOfOrderDelivery = "ErrOutOfOrderDelivery" // outdated message received, ignored
	ErrLogEntryErased     = "ErrLogEntryErased"     // previous index is detected with a new log entry, definitely not applied
	ErrTermChanged        = "ErrTermChanged"        // Periodic term detector found that the term of the previous leader has changed, no guarantee if the command will be applied or erased.
)

// used to shorten the uid of a client for logging
const PrefixLength = 5

type KVOperationType int

const (
	GET KVOperationType = iota
	PUT
	APPEND
)

type KVOperationArgs struct {
	Key     string
	Value   string          // not used in "Get"
	Type    KVOperationType // "Put", "Append" or "Get"
	ClerkId int64           // identify the client
	SeqNum  int             // to make operation idempotent and eliminate ABA issue
}

type KVOperationReply struct {
	Err   Err
	Value string // only used by Get
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
