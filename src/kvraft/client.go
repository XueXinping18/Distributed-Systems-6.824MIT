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
	servers  []*labrpc.ClientEnd
	uid      int64
	base64Id string // for logging purpose
	// Clerk prefers to talk to a known leader, otherwise try randomly
	suggestedLeader int   // Record the suggestion of which server to send
	suggestedTerm   int   // Record the term of that leader for comparison purpose
	nextSeqNum      int32 // atomically increase for SeqNum
}

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
	ck.base64Id = int64ToBase64(ck.uid)
	ck.nextSeqNum = 0
	ck.suggestedTerm = -1
	ck.suggestedLeader = -1
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
	ck.Operation(args)
}

// Handling the RPC for the operation given the OperationArgs
// return the result string if it is a Get, return empty string if not
// the function won't return until the successful finish of the operation
// Note that the client will issue requests one at a time, there is no burden on handling concurrency on client side!
func (ck *Clerk) Operation(args *OperationArgs) string {
	var reply *OperationReply
	seqNum := args.SeqNum
	for {
		// reset reply struct
		reply = new(OperationReply)
		var serverId int
		// decide which leader to choose
		if ck.suggestedLeader == -1 {
			serverId = ck.chooseRandomServer()
		} else {
			serverId = ck.suggestedLeader
		}
		ok := ck.servers[serverId].Call("KVServer.HandleOperation", args, reply)
		if !ok {
			// message lost, retry with a random server
			ck.logRPC(false, seqNum, serverId, "Message Lost in traffic (either request or response)")
			ck.suggestedLeader, ck.suggestedTerm = -1, -1
		} else {
			// check invariance: if one is invalid, both are invalid
			if (reply.LeaderId == -1 || reply.TermId == -1) && reply.LeaderId != reply.TermId {
				ck.logRPC(true, seqNum, serverId, "ERROR: Invariant break w.r.t. LeaderId and TermId!")
			}
			// received reply: update the suggested leader if
			if reply.TermId > ck.suggestedTerm || reply.LeaderId == -1 {
				ck.suggestedLeader, ck.suggestedTerm = reply.LeaderId, reply.TermId
			}
			// decide what action to do according to error term, validate invariance
			switch reply.Err {
			case ErrOutOfOrderDelivery:
				// simply ignore, network reordering
				ck.logRPC(true, seqNum, serverId, "ERROR: In RPC api, out of order delivery in synchronous scenario is not possible")
			case ErrLogEntryErased:
				ck.logRPC(false, seqNum, serverId, "Client notified that log entry has been erased, retry!")
			case ErrWrongLeader:
				ck.logRPC(false, seqNum, serverId, "Client notified that the server is not leader, retry another server!")
			case ErrNoKey:
				// success
				if reply.Value != "" || args.Type != GET {
					ck.logRPC(true, seqNum, serverId, "ERROR: Inconsistency found with ErrNoKey!")
				}
				ck.logRPC(false, seqNum, serverId, "The Get request with key not existed in the state machine!")
				break
			case OK:
				// success
				ck.logRPC(false, seqNum, serverId, "The request returns successfully!")
				break
			}
		}
	}
	return reply.Value
}

func (ck *Clerk) logClerk(fatal bool, format string, args ...interface{}) {
	prefix := "Clerk-" + ck.base64Id + ": "
	message := fmt.Sprintf(format, args...)
	if fatal {
		log.Println(prefix + message)
	} else {
		log.Fatalln(prefix + message)
	}
}
func (ck *Clerk) logRPC(fatal bool, seqNum int, serverId int, format string, args ...interface{}) {
	prefixClerk := "Clerk-" + ck.base64Id + " "
	prefixSeq := fmt.Sprintf("Seq-%d ", seqNum)
	prefixService := fmt.Sprintf("Service-%d: ", serverId)
	message := fmt.Sprintf(format, args...)
	if fatal {
		log.Fatalln(prefixClerk + prefixSeq + prefixService + message)
	} else {
		log.Println(prefixClerk + prefixSeq + prefixService + message)
	}
}
