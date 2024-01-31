package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"fmt"
	"log"
	mrand "math/rand"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm              *shardctrler.Clerk // the clerk for calling controller RPCs
	config          shardctrler.Config
	makeEnd         func(string) *labrpc.ClientEnd
	uid             int64
	base64IdPrefix  string // for logging purpose
	preferredServer int    // Record the previous server that the clerk successfully communicated with under the current configuration
	nextSeqNum      int32  // atomically increase for SeqNum
}

// the tester calls MakeClerk.
//
// controllers[] is needed to call shardctrler.MakeClerk().
//
// makeEnd(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(controllers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(controllers)
	ck.makeEnd = makeEnd // a mapping from string to *labrpc.ClientEnd
	ck.uid = nrand()
	ck.base64IdPrefix = base64Prefix(ck.uid)
	ck.nextSeqNum = 0
	ck.preferredServer = -1
	return ck
}

// encapsulate the logic to select a server of the correct group given current config and preferredServer field
//
//	return the clientEnd, gid, and the index (in the group) of the selected server
//	It only guarantees that it will find a server who is in the group serving the key according to the config the client has
//
// This function might block and fetch config if the configuration no server available for the key
// The function does not guarantee that the server selected is the leader.
func (ck *Clerk) selectServer(args *KVOperationArgs, mustRefreshConfig bool) (*labrpc.ClientEnd, int, int) {
	shard := key2shard(args.Key)
	gid := ck.config.Shards[shard]
	servers := ck.config.Groups[gid]
	for gid == 0 || len(servers) == 0 || mustRefreshConfig {
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1) // fetch the newest config
		gid = ck.config.Shards[shard]
		servers = ck.config.Groups[gid]
		ck.preferredServer = -1 // reset the server
	}
	var serverId int
	var serverName string
	// decide which leader to choose in the group
	if ck.preferredServer == -1 {
		serverId = mrand.Intn(len(servers))
		serverName = servers[serverId]
		ck.logRPC(false, args.SeqNum, gid, serverId, "Decide to select server handle %d in random", serverId)
	} else {
		serverId = ck.preferredServer
		serverName = servers[serverId]
		ck.logRPC(false, args.SeqNum, gid, serverId, "Decide to select server handle %d as previously did", serverId)
	}
	return ck.makeEnd(serverName), gid, serverId
}

// Handling the RPC for the operation given the KVOperationArgs
// return the result string if it is a Get, return empty string if not
// the function won't return until the successful finish of the operation
// Note that the client will issue requests one at a time, there is no burden on handling concurrency on client side!
func (ck *Clerk) KVOperation(args *KVOperationArgs) string {
	var reply *KVOperationReply
	seqNum := args.SeqNum
	count := 0
	refreshConfig := false
	for done := false; !done; {
		ck.logClerk(false, "The number of attempt to send the operation for the SeqNum %d is %d. Do the next attempt!",
			seqNum, count)
		// reset reply struct for every retry
		reply = new(KVOperationReply)
		// select server according to config
		serverEnd, gid, serverId := ck.selectServer(args, refreshConfig)
		// Doing RPC to the server.
		// Note that serverId in client side and server side is different for the same server
		ok := serverEnd.Call("ShardKV.HandleKVOperation", args, reply)

		// handle the RPC reply and potentially retry, update the preferred server to send
		if !ok {
			// message lost, retry with a random server
			ck.logRPC(false, seqNum, gid, serverId, "Message Lost in traffic (either request or response)")
			ck.preferredServer = -1
			refreshConfig = true
		} else {
			// decide what action to do according to error term, validate invariance
			switch reply.Err {
			case ErrWrongGroup:
				ck.logRPC(false, seqNum, gid, serverId, "Client notified that the server no longer manages the shard, refresh the config and retry!")
				refreshConfig = true
			case ErrLogEntryErased:
				ck.logRPC(false, seqNum, gid, serverId, "Client notified that log entry has been erased, retry!")
				refreshConfig = false
				ck.preferredServer = -1
			case ErrWrongLeader:
				ck.logRPC(false, seqNum, gid, serverId, "Client notified that the server is not leader, retry another server!")
				refreshConfig = false
				ck.preferredServer = -1
			case ErrNoKey:
				refreshConfig = false
				ck.preferredServer = serverId
				// success
				if reply.Value != "" || args.Type != GET {
					ck.logRPC(true, seqNum, gid, serverId, "ERROR: ErrNoKey replied for non-GET operation!")
				}
				ck.logRPC(false, seqNum, gid, serverId, "The Get request with key not existed in the state machine!")
				done = true // break the loop
			case OK:
				refreshConfig = false
				ck.preferredServer = serverId
				// success
				ck.logRPC(false, seqNum, gid, serverId, "The request returns successfully!")
				done = true // break the loop
			}
		}
		count++
	}
	return reply.Value
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := &KVOperationArgs{
		Key:     key,
		Value:   "",
		Type:    GET,
		ClerkId: ck.uid,
		SeqNum:  int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Client issues GET operation with key "+key+".")
	return ck.KVOperation(args)
}
func (ck *Clerk) Put(key string, value string) {
	args := &KVOperationArgs{
		Key:     key,
		Value:   value,
		Type:    PUT,
		ClerkId: ck.uid,
		SeqNum:  int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Client issues PUT operation with key "+key+" and value "+value+".")
	ck.KVOperation(args)
}
func (ck *Clerk) Append(key string, value string) {
	args := &KVOperationArgs{
		Key:     key,
		Value:   value,
		Type:    APPEND,
		ClerkId: ck.uid,
		SeqNum:  int(atomicIncrementAndSwap(&ck.nextSeqNum)),
	}
	ck.logClerk(false, "Client issues APPEND operation with key "+key+" and value "+value+".")
	ck.KVOperation(args)
}

// for logging purpose
func (ck *Clerk) logClerk(fatal bool, format string, args ...interface{}) {
	if !ShardKVDebug {
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
func (ck *Clerk) logRPC(fatal bool, seqNum int, gid int, serverId int, format string, args ...interface{}) {
	if !ShardKVDebug {
		return
	}
	prefixClerk := "Clerk-" + ck.base64IdPrefix + " "
	prefixSeq := fmt.Sprintf("Seq-%d ", seqNum)
	prefixService := fmt.Sprintf("ServiceHandle-%d-%d: ", gid, serverId)
	message := fmt.Sprintf(format, args...)
	if fatal {
		log.Fatalln(prefixClerk + prefixSeq + prefixService + message)
	} else {
		log.Println(prefixClerk + prefixSeq + prefixService + message)
	}
}
