package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"log"
	mrand "math/rand"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers        []*labrpc.ClientEnd
	uid            int64
	base64IdPrefix string // for logging purpose
	// Clerk prefers to talk to a known leader, otherwise try randomly
	preferredServer int   // Record the previous server that the clerk successfully communicated with
	nextSeqNum      int32 // atomically increase for SeqNum
}

// generate random int64
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
func (ck *Clerk) chooseRandomServer() int {
	return mrand.Intn(len(ck.servers))
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// assign a unique ID for the clerk
	ck.uid = nrand()
	ck.base64IdPrefix = base64Prefix(ck.uid)
	ck.nextSeqNum = 0
	ck.preferredServer = -1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := &OperationArgs{
		Key:     key,
		Value:   "",
		Type:    GET,
		ClerkId: ck.uid,
		SeqNum:  int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Client issues GET operation with key "+key+".")
	return ck.Operation(args)
}
func (ck *Clerk) Put(key string, value string) {
	args := &OperationArgs{
		Key:     key,
		Value:   value,
		Type:    PUT,
		ClerkId: ck.uid,
		SeqNum:  int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Client issues PUT operation with key "+key+" and value "+value+".")
	ck.Operation(args)
}
func (ck *Clerk) Append(key string, value string) {
	args := &OperationArgs{
		Key:     key,
		Value:   value,
		Type:    APPEND,
		ClerkId: ck.uid,
		SeqNum:  int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Client issues APPEND operation with key "+key+" and value "+value+".")
	ck.Operation(args)
}

// Handling the RPC for the operation given the OperationArgs
// return the result string if it is a Get, return empty string if not
// the function won't return until the successful finish of the operation
// Note that the client will issue requests one at a time, there is no burden on handling concurrency on client side!
func (ck *Clerk) Operation(args *OperationArgs) string {
	var reply *OperationReply
	seqNum := args.SeqNum
	count := 0
	for done := false; !done; {
		ck.logClerk(false, "The number of attempt to send the operation for the SeqNum %d is %d. Do the next attempt!",
			seqNum, count)
		// reset reply struct for every retry
		reply = new(OperationReply)
		var serverId int
		// decide which leader to choose
		if ck.preferredServer == -1 {
			serverId = ck.chooseRandomServer()
			ck.logRPC(false, seqNum, serverId, "Decide to select server handle %d in random", serverId)
		} else {
			serverId = ck.preferredServer
			ck.logRPC(false, seqNum, serverId, "Decide to select server handle %d as previously did", serverId)
		}
		// Doing RPC to the server.
		// Note that serverId in client side and server side is different for the same server
		ok := ck.servers[serverId].Call("KVServer.HandleOperation", args, reply)

		// handle the RPC reply and potentially retry, update the preferred server to send
		if !ok {
			// message lost, retry with a random server
			ck.logRPC(false, seqNum, serverId, "Message Lost in traffic (either request or response)")
			ck.preferredServer = -1
		} else {
			// decide what action to do according to error term, validate invariance
			switch reply.Err {
			case ErrOutOfOrderDelivery:
				// simply ignore, network reordering
				ck.logRPC(true, seqNum, serverId, "ERROR: In RPC api, out of order delivery in synchronous scenario is not possible")
			case ErrLogEntryErased:
				ck.logRPC(false, seqNum, serverId, "Client notified that log entry has been erased, retry!")
				ck.preferredServer = -1
			case ErrWrongLeader:
				ck.logRPC(false, seqNum, serverId, "Client notified that the server is not leader, retry another server!")
				ck.preferredServer = -1
			case ErrNoKey:
				ck.preferredServer = serverId
				// success
				if reply.Value != "" || args.Type != GET {
					ck.logRPC(true, seqNum, serverId, "ERROR: ErrNoKey replied for non-GET operation!")
				}
				ck.logRPC(false, seqNum, serverId, "The Get request with key not existed in the state machine!")
				done = true // break the loop
			case OK:
				ck.preferredServer = serverId
				// success
				ck.logRPC(false, seqNum, serverId, "The request returns successfully!")
				done = true // break the loop
			}
		}
		count++
	}
	return reply.Value
}

func (ck *Clerk) logClerk(fatal bool, format string, args ...interface{}) {
	if !KVDebug {
		return
	}
	prefix := "Clerk-" + ck.base64IdPrefix + ": "
	message := fmt.Sprintf(format, args...)
	if fatal {
		log.Fatalln(prefix + message)
	} else {
		log.Println(prefix + message)
	}
}
func (ck *Clerk) logRPC(fatal bool, seqNum int, serverId int, format string, args ...interface{}) {
	if !KVDebug {
		return
	}
	prefixClerk := "Clerk-" + ck.base64IdPrefix + " "
	prefixSeq := fmt.Sprintf("Seq-%d ", seqNum)
	prefixService := fmt.Sprintf("ServiceHandle-%d: ", serverId)
	message := fmt.Sprintf(format, args...)
	if fatal {
		log.Fatalln(prefixClerk + prefixSeq + prefixService + message)
	} else {
		log.Println(prefixClerk + prefixSeq + prefixService + message)
	}
}
