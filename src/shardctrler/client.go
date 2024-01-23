package shardctrler

//
// Shardctrler clerk.
//

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
	// assign a unique ID for the clerk
	ck.uid = nrand()
	ck.base64IdPrefix = base64Prefix(ck.uid)
	ck.nextSeqNum = 0
	ck.preferredServer = -1
	return ck
}

func (ck *Clerk) Query(index int) Config {
	args := &ControllerOperationArgs{
		ConfigID: index,
		Type:     QUERY,
		ClerkId:  ck.uid,
		SeqNum:   int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Admin issues QUERY operation with id %d.", index)
	return *ck.ControllerOperation(args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &ControllerOperationArgs{
		Servers: servers,
		Type:    JOIN,
		ClerkId: ck.uid,
		SeqNum:  int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Admin issues a JOIN operation.")
	ck.ControllerOperation(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &ControllerOperationArgs{
		GIDs:    gids,
		Type:    LEAVE,
		ClerkId: ck.uid,
		SeqNum:  int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Admin issues a LEAVE operation with gids %v.", gids)
	ck.ControllerOperation(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &ControllerOperationArgs{
		Shard:   shard,
		GID:     gid,
		Type:    MOVE,
		ClerkId: ck.uid,
		SeqNum:  int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Admin issues a MOVE operation with shard %d to gid %d.", shard, gid)
	ck.ControllerOperation(args)
}

// Handling the RPC for the operation given the ControllerOperationArgs
// return the result string if it is a Query, return nil if not
// the function won't return until the successful finish of the operation, encapsulating retry logic
// Note that the client will issue requests one at a time, there is no burden on handling concurrency on client side!
func (ck *Clerk) ControllerOperation(args *ControllerOperationArgs) *Config {
	var reply *ControllerOperationReply
	seqNum := args.SeqNum
	count := 0
	for done := false; !done; {
		ck.logClerk(false, "The number of attempt to send the operation for the SeqNum %d is %d. Do the next attempt!",
			seqNum, count)
		// reset reply struct for every retry
		reply = new(ControllerOperationReply)
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
		ok := ck.servers[serverId].Call("KVServer.HandleControllerOperation", args, reply)

		// handle the RPC reply and potentially retry, update the preferred server to send
		if !ok {
			// message lost, retry with a random server
			ck.logRPC(false, seqNum, serverId, "Message Lost in traffic (either request or response)")
			ck.preferredServer = -1
		} else {
			// decide what action to do according to error term, validate invariance
			switch reply.Err {
			case ErrLogEntryErased:
				ck.logRPC(false, seqNum, serverId, "Client notified that log entry has been erased, retry!")
				ck.preferredServer = -1
			case ErrWrongLeader:
				ck.logRPC(false, seqNum, serverId, "Client notified that the server is not leader, retry another server!")
				ck.preferredServer = -1
			case OK:
				ck.preferredServer = serverId
				// success
				ck.logRPC(false, seqNum, serverId, "The request returns successfully!")
				done = true // break the loop
			}
		}
		count++
	}
	return &reply.Config
}

func (ck *Clerk) logClerk(fatal bool, format string, args ...interface{}) {
	if !ControllerDebug {
		return
	}
	prefix := "Admin-" + ck.base64IdPrefix + ": "
	message := fmt.Sprintf(format, args...)
	if fatal {
		log.Fatalln(prefix + message)
	} else {
		log.Println(prefix + message)
	}
}
func (ck *Clerk) logRPC(fatal bool, seqNum int, serverId int, format string, args ...interface{}) {
	if !ControllerDebug {
		return
	}
	prefixClerk := "Admin-" + ck.base64IdPrefix + " "
	prefixSeq := fmt.Sprintf("Seq-%d ", seqNum)
	prefixService := fmt.Sprintf("ControllerHandle-%d: ", serverId)
	message := fmt.Sprintf(format, args...)
	if fatal {
		log.Fatalln(prefixClerk + prefixSeq + prefixService + message)
	} else {
		log.Println(prefixClerk + prefixSeq + prefixService + message)
	}
}
