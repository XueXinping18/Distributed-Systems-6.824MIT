package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

// CommandResponse is used to cache the result of the most recent command from a specific clerk
// Because the design is synchronous, duplication with uncertainty only happens for the most recent command
// (i.e. the client always retry the previous unsuccessful command before it issues the new command)
type CommandResponse struct {
	ClerkId int64
	SeqNum  int
	Err     Err
	Value   string // only useful for GET command
}

// RpcContext is used to record the information related to an RPC thread handling an operation
type RpcContext struct {
	ClerkId      int64
	SeqNum       int
	ReplyChannel chan *CommandResponse // used to reply the RPC thread and close the resource, avoiding memory leak
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// ClerkId and SeqNum uniquely identify the operation
	ClerkId int64
	SeqNum  int
	Type    OperationType
	Key     string
	Value   string // not used if the type is GET
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxRaftState int   // snapshot if log grows this big
	// why map of channels not conditional variable? because it can achieve that if there is a handler, then the handler
	// will definitely receive the response immediately. If no such handler, do not block.
	// If using conditional variable, hard to pass idiosyncratic message for each RPC handlers. (still need a map to pass the message)
	// It will be wrong to share one single response without map and using broadcast to wake up all threads to find match for that response.
	// Because it is not guaranteed that the shared response will ever be matched before updated (the producer acquires lock again
	// to update the response before the consumer can consume that response).
	rpcContexts       map[int]*RpcContext // for each RPC there will be a channel for it to pass in the response
	lastExecutedIndex int

	stateMachine   map[string]string          // the in-memory key-value stateMachine
	duplicateTable map[int64]*CommandResponse // used to detect duplicated commands to execute or duplicated client request
}

// The RPC handler for all GET, PUT and APPEND operation
func (kv *KVServer) HandleOperation(args *OperationArgs, reply *OperationReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cache, ok := kv.duplicateTable[args.ClerkId]
	// the command has just been executed, need to reply
	if ok && cache.SeqNum == args.SeqNum {
		reply.Err = cache.Err
		reply.Value = cache.Value
		// without entering raft.Start(), no leader info can be reported back to the client
		kv.logRPC(false, args.ClerkId, args.SeqNum, "Executed command found in RPC handler and must reply again")
		return
	}
	// outdated command, the response for which the client must have seen, irrelevant,
	// (in RPC, the client has marked the packet as lost and move on to retry, so the error is not visible by the client)
	if ok && cache.SeqNum > args.SeqNum {
		reply.Err = ErrOutOfOrderDelivery
		reply.Value = ""
		kv.logRPC(false, args.ClerkId, args.SeqNum, "Executed command found in RPC handler and the client"+
			" is guaranteed to have seen the response! Simply ignored!")
		return
	}
	// pruning: SeqNum to large showing that the current thread is not leader with the highest term
	// (although it might still believe it is the leader)
	if ok && args.SeqNum > cache.SeqNum+1 {
		kv.logRPC(false, args.ClerkId, args.SeqNum, "Jump in sequence number"+
			" The service must not be the leader of current term")
		reply.Err = ErrWrongLeader
		return
	}
	// !ok || args.SeqNum == cache.SeqNum+1
	op := Op{
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
		Type:    args.Type,
		Key:     args.Key,
		Value:   args.Value,
	}
	indexOrLeader, term, success := kv.rf.Start(op)
	if !success {
		reply.Err = ErrWrongLeader
		kv.logRPC(false, args.ClerkId, args.SeqNum, "Service called by the clerk is not leader for term %d!", term)
		return
	}
	// scenario: successfully append the command to the log
	// check index collision
	prevContext, ok := kv.rpcContexts[indexOrLeader]
	if ok {
		// there is a rpcContext outstanding whose index colliding with the current index,
		// the old entry must have been erased so that the current entry can be appended at that index
		// give it the clerkId and seqNum for the current thread and set error so that it got resent
		// Here is a very interesting corner case: if the clerkId, seqNum, index and the leaderId are all the same
		// (which implies a different term), we can still suggest the handler to resend because next time it is
		// most likely to be detected by duplicateTable (either during initial check or check before execution)
		responseToStale := &CommandResponse{
			ClerkId: args.ClerkId,
			SeqNum:  args.SeqNum,
			Err:     ErrLogEntryErased,
		}
		kv.logRPC(false, args.ClerkId, args.SeqNum, "There is already an RpcContext at the index where the RpcContext can reside")
		// TODO: no need to release lock here, because if channel in map then it must be blocking
		prevContext.ReplyChannel <- responseToStale
	}
	currentContext := &RpcContext{
		SeqNum:       args.SeqNum,
		ClerkId:      args.ClerkId,
		ReplyChannel: make(chan *CommandResponse),
	}
	kv.rpcContexts[indexOrLeader] = currentContext
	kv.mu.Unlock()
	// blocking to receive response
	response := <-currentContext.ReplyChannel
	kv.mu.Lock()
	delete(kv.rpcContexts, indexOrLeader)
	if response.SeqNum != args.SeqNum || response.ClerkId != args.ClerkId || response.Err == ErrLogEntryErased {
		// mismatch found, the log entry must have been cleaned up
		reply.Err = ErrLogEntryErased
		kv.logRPC(false, args.ClerkId, args.SeqNum, "Log entry appended found removed! Reply error")
		return
	} else {
		// matched correctly
		reply.Value, reply.Err = response.Value, response.Err
		kv.logRPC(false, args.ClerkId, args.SeqNum, "Correctly reply the client the response generated by applyChannelObserver")
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// Observe the applyChannel to actually apply to the in-memory stateMachine
// if the server is the server that talks to the client, notify the client
// by waking up the RPC thread
func (kv *KVServer) applyChannelObserver() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		// must wait until the kv.replyEntry has been properly handled to apply the next command and update kv.replyEntry
		kv.mu.Lock()
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)          // type assertion into Op so that the operation can be applied
			response := kv.validateAndApply(&op) // check if the operation is up-to-date
			// if so, apply the operation and store the result (especially GET) to duplicateTable

			// update the executed index if executed
			if applyMsg.CommandIndex > kv.lastExecutedIndex {
				if applyMsg.CommandIndex != kv.lastExecutedIndex+1 {
					kv.logService(true, "ERROR: Jump found in the index of executed commands!")
				}
				kv.lastExecutedIndex = applyMsg.CommandIndex
			}
			rpcContext, ok := kv.rpcContexts[applyMsg.CommandIndex]
			// reply if there is a matched RPC handler
			kv.mu.Unlock()
			if ok {
				rpcContext.ReplyChannel <- response
			}
		} else if applyMsg.SnapshotValid {
			// (3B)
			kv.mu.Unlock()
		} else {
			kv.logService(true, "ERROR: Neither snapshot nor command for a applyMsg!")
		}
	}
}

// find if the operation is outdated and for outdated operation if the result needs to be correctly replied,
// update the replyEntry accordingly
// if the seqNum is smaller than the cached seqNum for the clerk, the clerk must have received the reply because
// all operations are synchronous in Raft, so we can safely do nothing
func (kv *KVServer) validateAndApply(operation *Op) *CommandResponse {
	if operation == nil {
		kv.logService(true, "ERROR: The operation to execute is a nil pointer!")
	}
	cachedEntry, ok := kv.duplicateTable[operation.ClerkId]
	// not outdated or duplicated
	var response *CommandResponse
	if !ok || cachedEntry.SeqNum < operation.SeqNum {
		response = kv.apply(operation)
		kv.duplicateTable[operation.ClerkId] = response // by pointer assign to duplicateTable
	} else if cachedEntry.SeqNum == operation.SeqNum {
		// duplicated, need to resend reply
		response = cachedEntry
		kv.logRPC(false, operation.ClerkId, operation.SeqNum, "Executed command found in apply channel and must reply again")
	} else {
		// lagged behind operation, the response will be ignored by clerk because the clerk has received reply
		kv.logRPC(false, operation.ClerkId, operation.SeqNum, "Executed command found in apply channel and the client"+
			" is guaranteed to have seen the response! Simply ignored!")
		// just return an empty response as the client is guaranteed to see the response already
		// it will be discarded by the clerk anyway
		response = &CommandResponse{
			ClerkId: operation.ClerkId,
			SeqNum:  operation.SeqNum,
			Err:     ErrOutOfOrderDelivery,
			Value:   "",
		}
	}
	return response
}

// Apply the operation to the in-memory kv stateMachine and put the result into kv.replyEntry
func (kv *KVServer) apply(operation *Op) *CommandResponse {
	if operation == nil {
		kv.logService(true, "ERROR: The operation to execute is a nil pointer!")
	}
	response := &CommandResponse{
		ClerkId: operation.ClerkId,
		SeqNum:  operation.SeqNum,
	}
	switch operation.Type {
	case GET:
		val, ok := kv.stateMachine[operation.Key]
		response.Value = val
		if ok {
			response.Err = OK
		} else {
			response.Err = ErrNoKey
		}
		kv.logRPC(false, operation.ClerkId, operation.SeqNum, "GET operation has been executed!")
	case PUT:
		kv.stateMachine[operation.Key] = operation.Value
		response.Err = OK
		response.Value = "" // no value needed for put
		kv.logRPC(false, operation.ClerkId, operation.SeqNum, "PUT operation has been executed!")
	case APPEND:
		kv.stateMachine[operation.Key] = kv.stateMachine[operation.Key] + operation.Value
		response.Err = OK
		response.Value = "" // no value needed for append
		kv.logRPC(false, operation.ClerkId, operation.SeqNum, "APPEND operation has been executed!")
	default:
		kv.logService(true, "ERROR: unknown type of operation to be applied!")
	}
	return response
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &KVServer{
		me:             me,
		maxRaftState:   maxraftstate,
		applyCh:        make(chan raft.ApplyMsg),
		dead:           0,
		stateMachine:   make(map[string]string),
		duplicateTable: make(map[int64]*CommandResponse),
		rpcContexts:    make(map[int]*RpcContext),
	}
	// You may need initialization code here.
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// (3B: load from persister)

	// You may need initialization code here.
	go kv.applyChannelObserver()
	return kv
}

// logging functions
// 1. logging info regarding the service itself
func (kv *KVServer) logService(fatal bool, format string, args ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("Service-%d: ", kv.me)
		if fatal {
			log.Printf(prefix+format+"\n", args...)
		} else {
			log.Fatalf(prefix+format+"\n", args...)
		}
	}
}

// 2. logging info regarding the communication between clerk and service
func (kv *KVServer) logRPC(fatal bool, clerkId int64, seqNum int, format string, args ...interface{}) {
	if Debug {
		// convert id to base64 and take a prefix for logging purpose
		clerkStr := base64Prefix(clerkId)
		prefixService := fmt.Sprintf("Service-%d ", kv.me)
		prefixClerk := "Clerk-" + clerkStr + " "
		prefixSeq := fmt.Sprintf("Seq-%d: ", seqNum)
		fmtString := fmt.Sprintf(format, args...)
		if fatal {
			log.Fatal(prefixService + prefixClerk + prefixSeq + fmtString + "\n")
		} else {
			log.Print(prefixService + prefixClerk + prefixSeq + fmtString + "\n")
		}
	}
}
