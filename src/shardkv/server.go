package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const RaftStateLengthRatio float64 = 0.8  // the threshold above which the raft state length (mainly log) will trigger taking a snapshot
const StaleDetectorSleepMillis int = 1500 // how long to check if the term deviated from the term a command is appended in log

// KVCommandResponse is used to cache the result of the most recent command from a specific clerk
// Because the design is synchronous, duplication with uncertainty only happens for the most recent command
// (i.e. the client always retry the previous unsuccessful command before it issues the new command)
type KVCommandResponse struct {
	ClerkId int64
	SeqNum  int
	Err     Err
	Value   string // only useful for GET command
}

// RpcContext is used to record the information related to an RPC thread handling an operation
type RpcContext struct {
	ClerkId      int64
	SeqNum       int
	TermAppended int                // record the term that the log is appended, invalidate the request if the change of term detected
	replyCond    *sync.Cond         // used to wake up the waiting RPC handler after response is assembled
	Response     *KVCommandResponse // used by the sender thread to send the response to the receiver handler
}

// The constructor for RpcContext, mainly for bind the conditional variable to the mutex of the ShardKV
func (kv *ShardKV) NewRpcContext(clerkId int64, seqNum int, term int) *RpcContext {
	return &RpcContext{
		ClerkId:      clerkId,
		SeqNum:       seqNum,
		TermAppended: term,
		replyCond:    sync.NewCond(&kv.mu),
		Response:     nil,
	}
}
func (rpcContext *RpcContext) deliverResponseAndNotify(response *KVCommandResponse) {
	rpcContext.Response = response
	rpcContext.replyCond.Broadcast()
}

// To be serialized and stored in log
type Op struct {
	// ClerkId and SeqNum uniquely identify the operation
	ClerkId int64
	SeqNum  int
	Type    KVOperationType
	Key     string
	Value   string // not used if the type is GET
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd // mapping from name to clientEnd
	mck          *shardctrler.Clerk
	gid          int
	controllers  []*labrpc.ClientEnd
	maxRaftState int // snapshot if log grows this big

	dead              int32               // set by Kill()
	snapshotThreshold int                 // calculated based on maxRaftState
	rpcContexts       map[int]*RpcContext // for each RPC there will be a rpcContext for it to pass in response and notify the finish of response
	lastExecutedIndex int
	stateMachine      map[string]string            // the in-memory key-value stateMachine
	duplicateTable    map[int64]*KVCommandResponse // used to detect duplicated commands to execute or duplicated client request
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// The RPC handler for all GET, PUT and APPEND operation
func (kv *ShardKV) HandleKVOperation(args *KVOperationArgs, reply *KVOperationReply) {
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
		reply.Err = OK
		kv.logRPC(false, args.ClerkId, args.SeqNum, "Executed command found in RPC handler and the client"+
			" is guaranteed to have seen the response! Simply ignored!")
		return
	}
	op := Op{
		ClerkId: args.ClerkId,
		SeqNum:  args.SeqNum,
		Type:    args.Type,
		Key:     args.Key,
		Value:   args.Value,
	}
	kv.logRPC(false, args.ClerkId, args.SeqNum, "Try to delegate the command to the Raft library")

	indexOrLeader, term, success := kv.rf.Start(op)
	kv.logRPC(false, args.ClerkId, args.SeqNum, "Finished delegation of command")
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

		// Here is a very interesting corner case: if the clerkId, seqNum, index and the leaderId are all the same
		// (which implies a different term), we can still suggest the handler to resend because next time it is
		// most likely to be detected by duplicateTable (either during initial check or check before execution)
		kv.logRPC(false, args.ClerkId, args.SeqNum, "There is already an RpcContext at the index where the command can reside")
		response := &KVCommandResponse{
			ClerkId: prevContext.ClerkId,
			SeqNum:  prevContext.SeqNum,
			Err:     ErrLogEntryErased,
		}
		prevContext.deliverResponseAndNotify(response)
	}
	currentContext := kv.NewRpcContext(args.ClerkId, args.SeqNum, term)
	kv.rpcContexts[indexOrLeader] = currentContext
	// If the leadership changed during waiting for reply, and the command is not erased with its index occupied by another command,
	// e.g. serving a single client, then it might wait forever. We need to detect the change of term
	kv.logRPC(false, args.ClerkId, args.SeqNum, "Started to wait for the apply of the command and response assembled")
	// blocking to receive response
	currentContext.replyCond.Wait()
	delete(kv.rpcContexts, indexOrLeader)
	response := currentContext.Response
	if response == nil {
		kv.logRPC(true, args.ClerkId, args.SeqNum, "ERROR: RPC handler woken up but the response is nil!")
	}
	kv.logRPC(false, args.ClerkId, args.SeqNum, "Received assembled response, wake up the RPC handler")
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

// Observe the applyChannel to actually apply to the in-memory stateMachine
// if the server is the server that talks to the client, notify the client
// by waking up the RPC thread
func (kv *ShardKV) applyChannelObserver() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		// must wait until the kv.replyEntry has been properly handled to apply the next command and update kv.replyEntry
		kv.mu.Lock()
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)          // type assertion into Op so that the operation can be applied
			response := kv.validateAndApply(&op) // check if the operation is up-to-date

			// update the executed index if executed
			if applyMsg.CommandIndex > kv.lastExecutedIndex {
				if applyMsg.CommandIndex != kv.lastExecutedIndex+1 {
					kv.logService(true, "ERROR: Jump found in the index of executed commands!")
				}
				kv.lastExecutedIndex = applyMsg.CommandIndex
			}
			// take snapshot if the new command makes the raft state too long
			if kv.shouldTakeSnapshot() {
				snapshot := kv.serializeSnapshot() // with lock, obstructing normal execution
				kv.logService(false, "Service decide to take snapshot under the threshold %d!", kv.snapshotThreshold)
				kv.rf.Snapshot(kv.lastExecutedIndex, snapshot)
			}
			rpcContext, ok := kv.rpcContexts[applyMsg.CommandIndex]
			// reply if there is a matched RPC handler (i.e. the leader that talked to the client is the current server)
			if ok {
				rpcContext.deliverResponseAndNotify(response)
				kv.logRPC(false, response.ClerkId, response.SeqNum, "Notify the RPC handler with the reply message!")
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			// The raft has arranged that the snapshot will be uploaded to the service only if it is more up to date
			if kv.lastExecutedIndex >= applyMsg.SnapshotIndex {
				kv.logService(true, "ERROR: Received from Raft a snapshot that is not more up-to-date than the current state machine")
			}
			kv.logService(false, "Received from Raft an entire up-to-date snapshot!")
			kv.deserializeSnapshot(applyMsg.Snapshot)
			if applyMsg.SnapshotIndex != kv.lastExecutedIndex {
				kv.logService(true, "ERROR: Index deserialized from snapshot not equal to the index in the apply message!")
			}
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
func (kv *ShardKV) validateAndApply(operation *Op) *KVCommandResponse {
	if operation == nil {
		kv.logService(true, "ERROR: The operation to execute is a nil pointer!")
	}
	cachedEntry, ok := kv.duplicateTable[operation.ClerkId]
	// not outdated or duplicated
	var response *KVCommandResponse
	if !ok || cachedEntry.SeqNum < operation.SeqNum {
		response = kv.apply(operation)
		// store the result (especially GET) to duplicateTable
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
		response = &KVCommandResponse{
			ClerkId: operation.ClerkId,
			SeqNum:  operation.SeqNum,
			Err:     OK,
		}
	}
	return response
}

// Apply the operation to the in-memory kv stateMachine and put the result into kv.replyEntry
func (kv *ShardKV) apply(operation *Op) *KVCommandResponse {
	if operation == nil {
		kv.logService(true, "ERROR: The operation to execute is a nil pointer!")
	}
	response := &KVCommandResponse{
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

// Periodically check if the term has changed for each ongoing RPC requests. If the term changed, different from its
// previous term, the detector will help to generate response to client, asking the client to retry
// Note that for safety, the command is still likely to be committed, which might be unsafe. But in the synchronous
// setting, the client will always retry. Therefore, there is no safety issue in this particular scenario.
func (kv *ShardKV) staleRpcContextDetector() {
	kv.logService(false, "Start the detector for pending client requests whose log entry is outdated in term...")
	for !kv.killed() {
		time.Sleep(time.Duration(StaleDetectorSleepMillis) * time.Millisecond)
		kv.mu.Lock()
		currentTerm, isLeader := kv.rf.GetState()
		kv.logService(false, "Current Term %d; Is server %d leader? "+strconv.FormatBool(isLeader), currentTerm, kv.me)
		for _, rpcContext := range kv.rpcContexts {
			if currentTerm != rpcContext.TermAppended {
				kv.logRPC(false, rpcContext.ClerkId, rpcContext.SeqNum, "Outdated waiting rpc handler detected!")
				reply := &KVCommandResponse{
					ClerkId: rpcContext.ClerkId,
					SeqNum:  rpcContext.SeqNum,
					Err:     ErrTermChanged,
				}
				rpcContext.deliverResponseAndNotify(reply)
				kv.logRPC(false, rpcContext.ClerkId, rpcContext.SeqNum, "Notify ErrTermChanged to the handler!")
			}
		}
		kv.mu.Unlock()
	}
}

// after each time the stateMachine executed a new command, check if the size of the log has grown too large and
// take snapshot if so.
func (kv *ShardKV) shouldTakeSnapshot() bool {
	// according to protocol, -1 represents never to take snapshot
	if kv.maxRaftState == -1 {
		return false
	}
	return kv.rf.IsStateSizeAbove(kv.snapshotThreshold)
}

// serialize the state machine and duplicate table
func (kv *ShardKV) serializeSnapshot() []byte {
	kv.logService(false, "Serialize the service states into a snapshot!")
	writeBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writeBuffer)
	if encoder.Encode(kv.stateMachine) != nil {
		kv.logService(true, "Fail to encode the state machine!")
	}
	if encoder.Encode(kv.duplicateTable) != nil {
		kv.logService(true, "Fail to encode duplicate table!")
	}
	if encoder.Encode(kv.lastExecutedIndex) != nil {
		kv.logService(true, "Fail to encode the last executed index!")
	}
	return writeBuffer.Bytes()
}

// deserialize the snapshot and write the field into the kv server
func (kv *ShardKV) deserializeSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state
		return
	}
	kv.logService(false, "Deserialize the snapshot into service states!")
	readBuffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(readBuffer)
	var stateMachine map[string]string
	var duplicateTable map[int64]*KVCommandResponse
	var lastExecutedIndex int
	if decoder.Decode(&stateMachine) != nil {
		kv.logService(false, "Failed to read state machine from snapshot bytes!")
	}
	if decoder.Decode(&duplicateTable) != nil {
		kv.logService(false, "Failed to read duplicate table from snapshot bytes!")
	}
	if decoder.Decode(&lastExecutedIndex) != nil {
		kv.logService(false, "Failed to read last executed index from snapshot bytes!")
	}
	kv.stateMachine = stateMachine
	kv.duplicateTable = duplicateTable
	kv.lastExecutedIndex = lastExecutedIndex
}

// servers[] contains the ports of the servers in this group.
// a server is always in the same group so the underlying raft interaction
// and servers[] never change
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxRaftState bytes, in order to allow Raft to garbage-collect its
// log. if maxRaftState is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass controllers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// makeEnd(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use controllers[]
// and makeEnd() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int,
	controllers []*labrpc.ClientEnd, makeEndFunc func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})                // as the command stored in logs
	labgob.Register(KVCommandResponse{}) // as the response cached in duplicate table
	kv := &ShardKV{
		me:                me,
		maxRaftState:      maxraftstate,
		makeEnd:           makeEndFunc,
		gid:               gid,
		controllers:       controllers,
		applyCh:           make(chan raft.ApplyMsg),
		dead:              0,
		stateMachine:      make(map[string]string),
		duplicateTable:    make(map[int64]*KVCommandResponse),
		rpcContexts:       make(map[int]*RpcContext),
		lastExecutedIndex: 0,
		snapshotThreshold: int(RaftStateLengthRatio * float64(maxraftstate)),
	}
	kv.mck = shardctrler.MakeClerk(controllers)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.logService(false, "Restart (or start) the service...")

	// load states from persisted snapshot
	kv.deserializeSnapshot(kv.rf.ReadSnapshot())
	// run background thread
	go kv.applyChannelObserver()
	go kv.staleRpcContextDetector()
	return kv
}

// logging functions
// 1. logging info regarding the service itself
func (kv *ShardKV) logService(fatal bool, format string, args ...interface{}) {
	if ShardKVDebug {
		prefix := fmt.Sprintf("Service-%d-%d: ", kv.gid, kv.me)
		if fatal {
			log.Fatalf(prefix+format+"\n", args...)
		} else {
			log.Printf(prefix+format+"\n", args...)
		}
	}
}

// 2. logging info regarding the communication between clerk and service
func (kv *ShardKV) logRPC(fatal bool, clerkId int64, seqNum int, format string, args ...interface{}) {
	if ShardKVDebug {
		// convert id to base64 and take a prefix for logging purpose
		clerkStr := base64Prefix(clerkId)
		prefixService := fmt.Sprintf("Service-%d-%d ", kv.gid, kv.me)
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
