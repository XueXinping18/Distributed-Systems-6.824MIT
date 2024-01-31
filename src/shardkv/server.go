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
const QueryConfigMillis int = 80          // periodically issue query to shard controller for latest config
const SendShardMillis int = 150           // periodically scan the sendJob list and scheduling to send shards if data available
const GarbageCollectMillis int = 200      // periodically trigger the garbage collector to delete buffered shards that have been installed to their new owner

// CommandUniqueIdentifier encapsulated how RPC context and RPC response is matched
type CommandUniqueIdentifier struct {
	Type    OperationType // used to identify that command for the index is matched as previous command
	ClerkId int64         // only used for client operation to identify that command for the index is matched as previous command
	SeqNum  int           // only used for client operation to identify that command for the index is matched as previous command
	ShardId int           // only used for installShard to identify if the command for the index is matched with previous command
	Version int           // only used for installShard to identify if the command for the index is matched with previous command
}

// RpcResponse is used to cache the result of the most recent command from a specific clerk
// It is also the encapsulated class for the applier to deliver assembled response to the corresponding RPC context, both ClientOperation and InstallShards
// Because the design is synchronous, duplication with uncertainty only happens for the most recent command
// (i.e. the client always retry the previous unsuccessful command before it issues the new command)
type RpcResponse struct {
	ClerkId   int64                   // only used in duplicateTable to detect client operation duplicates
	SeqNum    int                     // only used in duplicateTable to detect client operation duplicates
	CommandId CommandUniqueIdentifier // used to decide if waken-up RPC context is matched with the response delivered when index is the same
	Err       Err                     // used by all
	Value     string                  // only useful for GET command
}

// RpcContext is used to record the information related to an RPC thread handling an operation
type RpcContext struct {
	CommandId    CommandUniqueIdentifier // used to decide if waken-up RPC context is matched with the response delivered when index is the same
	TermAppended int                     // record the term that the log is appended, invalidate the request if the change of term detected
	replyCond    *sync.Cond              // used to wake up the waiting RPC handler after response is assembled
	Response     *RpcResponse            // used by the sender thread to send the response to the receiver handler
}

// used as the key for shards that has different version as the current config version
type ShardVersion struct {
	Shard   int
	Version int
}

type JobState int

const (
	NotStarted JobState = iota
	Ongoing
	Finished
)

// used for shard sender to store info about unfinished send jobs
type SendJobContext struct {
	Shard       int
	OldVersion  int // the version that the shard has on the shardBuffer.
	NewVersion  int // the version of the shard before which updates are reflected on the data, might be mismatched
	DestGID     int
	DestServers []string
	Status      JobState
}

// The constructor for RpcContext, mainly for bind the conditional variable to the mutex of the ShardKV
func (kv *ShardKV) NewRpcContextForClientOp(opType OperationType, clerkId int64, seqNum int, term int) *RpcContext {
	return &RpcContext{
		CommandId: CommandUniqueIdentifier{
			Type:    opType,
			ClerkId: clerkId,
			SeqNum:  seqNum,
		},
		TermAppended: term,
		replyCond:    sync.NewCond(&kv.mu),
		Response:     nil,
	}
}

// The constructor for RpcContext, mainly for bind the conditional variable to the mutex of the ShardKV
func (kv *ShardKV) NewRpcContextForShardInstall(opType OperationType, shardId int, version int, term int) *RpcContext {
	return &RpcContext{
		CommandId: CommandUniqueIdentifier{
			Type:    opType,
			ShardId: shardId,
			Version: version,
		},
		TermAppended: term,
		replyCond:    sync.NewCond(&kv.mu),
		Response:     nil,
	}
}
func (context *RpcContext) deliverResponseAndNotify(response *RpcResponse) {
	context.Response = response
	context.replyCond.Broadcast()
}

// To be serialized and stored in log as a client operation
type Op struct {
	// For all operations
	Type OperationType
	// For Client operations (get, put, append), ClerkId and SeqNum uniquely identify the operation
	ClerkId int64
	SeqNum  int
	Key     string
	Value   string // not used if the type is GET
	// For install shards
	Version        int // the new configID i.e. all the changes on the shards before this configID have been reflected to the shards
	ShardId        int
	ShardData      map[string]string
	SourceGid      int
	DuplicateTable map[int64]*RpcResponse // the duplication table so that one operation will not be executed in both group when configuration changes
	// For update Configuration
	Config shardctrler.Config
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

	duplicateTable       map[int64]*RpcResponse    // used to detect duplicated commands to execute or duplicated client request
	shardedStateMachines map[int]map[string]string // the in-memory key-value stateMachine, they are partitioned by shardID, done so as the unit of data deletion
	config               shardctrler.Config        // the config the shard is currently serving
	// once a configuration change observed in controller, the group stop serve some shards
	precludedShards map[int]bool                       // optional, used when a new incrementConfig is appended to log but yet to be applied. Prevent client operations on the to-be-removed shards from entering the log after the updateConfig log
	shardBuffer     map[ShardVersion]map[string]string // a layered buffer to store shards that are not for the current config
	// the first level is configID AND shardID, the last level is data for the shard that reflected all the changes before configID
	shardToSendJobs    map[ShardVersion]*SendJobContext // the set of unfinished send jobs, key to be the expected received version and shardId
	shardToInstallJobs map[int]int                      // the set of unfinished jobs to install shards to the state machine after receipt. key: shardId, value: version expected
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

// The RPC handler for InstallShard operation
func (kv *ShardKV) HandleInstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// duplication detection: a non-duplicated shard must either have higher version number or has the corresponding installation task or send task
	// similarly, double check is required before execution and here
	if kv.isShardDuplicated(args.Shard, args.Version, args.SourceGid) {
		reply.Err = OK
		kv.logShardReceiver(false, args.Shard, args.Version, args.SourceGid, "Duplicated Shard found by the receiver RPC handler, simply ignored!")
		return
	}
	op := Op{
		Type:           INSTALLSHARD,
		Version:        args.Version,
		ShardId:        args.Shard,
		ShardData:      args.Data,
		SourceGid:      args.SourceGid,
		DuplicateTable: args.DuplicateTable,
	}
	kv.logShardReceiver(false, args.Shard, args.Version, args.SourceGid, "Try to delegate a shard installation to the Raft library")
	indexOrLeader, term, success := kv.rf.Start(op)
	if !success {
		reply.Err = ErrWrongLeader
		kv.logShardReceiver(false, args.Shard, args.Version, args.SourceGid, "Service called by the clerk is not leader for term %d", term)
		return
	}
	// scenario: successfully append the command to the log
	// check index collision
	kv.checkAndFreeRpcContextWithCollidingIndex(indexOrLeader)
	kv.logShardReceiver(false, args.Shard, args.Version, args.SourceGid, "There is already an RpcContext at the index where the command can reside")

	// create the RpcContext and wait for reply
	currentContext := kv.NewRpcContextForShardInstall(INSTALLSHARD, args.Shard, args.Version, term)
	kv.rpcContexts[indexOrLeader] = currentContext
	kv.logShardReceiver(false, args.Shard, args.Version, args.SourceGid, "Started to wait for the installation of the shard and response assembled")
	currentContext.replyCond.Wait()
	// woke up after the response is generated
	delete(kv.rpcContexts, indexOrLeader)
	response := currentContext.Response

	if !currentContext.isMatchedWithResponse() {
		// same log index but not same command
		reply.Err = ErrLogEntryErased
		kv.logShardReceiver(false, args.Shard, args.Version, args.SourceGid, "RPC Handler woken up but the command is not as it previously sent! The LogEntry must have been erased")
	} else {
		// matched correctly
		reply.Err = response.Err
		kv.logShardReceiver(false, args.Shard, args.Version, args.SourceGid, "RPC Handler waken up with assembled response matched with the RPC context, send back reply...")
	}
	return
}

// used by both the handler for client command and shard installation
// return true if an index has already been associated with a RpcContext when a new RpcContext try to use the same index
func (kv *ShardKV) checkAndFreeRpcContextWithCollidingIndex(logIndex int) bool {
	prevContext, colliding := kv.rpcContexts[logIndex]
	if colliding {
		// there is a rpcContext outstanding whose index colliding with the current index,
		// the old entry must have been erased so that the current entry can be appended at that index

		// Here is a very interesting corner case: if the clerkId, seqNum, index and the leaderId are all the same
		// (which implies a different term), we can still suggest the handler to resend because next time it is
		// most likely to be detected by duplicateTable (either during initial check or check before execution)
		response := &RpcResponse{
			ClerkId:   prevContext.CommandId.ClerkId,
			SeqNum:    prevContext.CommandId.SeqNum,
			CommandId: prevContext.CommandId,
			Err:       ErrLogEntryErased,
		}
		prevContext.deliverResponseAndNotify(response)
	}
	return colliding
}

// given the version and id of the shard, decide if it has been installed
// sourceGid is just for logging purpose
func (kv *ShardKV) isShardDuplicated(shard int, version int, sourceGid int) bool {
	// scenario 1: arrived earlier than the config change, check if the data already in the buffer
	if version > kv.getCurrentVersion() {
		if _, exists := kv.shardBuffer[ShardVersion{shard, version}]; exists {
			return true
		} else {
			return false
		}
	}
	// scenario 2: arrived after the config change, check job lists
	expectedInstallVersion, installJobExists := kv.shardToInstallJobs[shard]
	sendJobContext, sendJobExists := kv.shardToSendJobs[ShardVersion{shard, version}]
	// check invariant: one oldVersion can't be simultaneously needed for send job and install job
	if sendJobExists && installJobExists && expectedInstallVersion == version {
		kv.logShardReceiver(true, shard, version, sourceGid, "ERROR: a shard & version is expected to be installed and resend simultaneously!")
	}
	// check send job
	if sendJobExists {
		// job exists, check if data already ready
		newVersion := sendJobContext.NewVersion
		if _, exists := kv.shardBuffer[ShardVersion{shard, newVersion}]; exists {
			// data already ready
			return true
		} else {
			// send job exists but data not ready, NOT duplicates
			return false
		}
	}
	// check installation job
	if !installJobExists || expectedInstallVersion > version {
		return true
	} else if expectedInstallVersion == version {
		return false
	} else {
		kv.logShardReceiver(true, shard, version, sourceGid, "ERROR: Protocol break: received shard version larger than expected Install Version but not larger than config version!")
		return false // unreachable
	}
}

// check if RPC context matched with RPC response, if response is nil then it might be IncrementConfig (because it has no response associated)
func (context *RpcContext) isMatchedWithResponse() bool {
	response := context.Response
	if response == nil {
		// there is no associated response, it could happen when a IncrementConfig has the same index with an operation with RPC context
		return false
	}
	if context.CommandId.Type != response.CommandId.Type {
		return false
	}
	if context.CommandId.Type == INSTALLSHARD {
		return context.CommandId.ShardId == response.CommandId.ShardId && context.CommandId.Version == response.CommandId.Version
	}
	// for client operation
	return context.CommandId.ClerkId == response.CommandId.ClerkId && context.CommandId.SeqNum == response.CommandId.SeqNum
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
		kv.logClientRPC(false, args.ClerkId, args.SeqNum, "Executed command found in RPC handler and must reply again")
		return
	}
	// outdated command, the response for which the client must have seen, irrelevant,
	// (in RPC, the client has marked the packet as lost and move on to retry, so the error is not visible by the client)
	if ok && cache.SeqNum > args.SeqNum {
		reply.Err = OK
		kv.logClientRPC(false, args.ClerkId, args.SeqNum, "Executed command found in RPC handler and the client"+
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
	kv.logClientRPC(false, args.ClerkId, args.SeqNum, "Try to delegate client command to the Raft library")

	indexOrLeader, term, success := kv.rf.Start(op)
	if !success {
		reply.Err = ErrWrongLeader
		kv.logClientRPC(false, args.ClerkId, args.SeqNum, "Service called by the clerk is not leader for term %d!", term)
		return
	}
	// scenario: successfully append the command to the log
	// check index collision
	kv.checkAndFreeRpcContextWithCollidingIndex(indexOrLeader)
	kv.logClientRPC(false, args.ClerkId, args.SeqNum, "There is already an RpcContext at the index where the command can reside")

	// create the new RpcContext and wait for commit or abandon
	currentContext := kv.NewRpcContextForClientOp(args.Type, args.ClerkId, args.SeqNum, term)
	kv.rpcContexts[indexOrLeader] = currentContext
	// If the leadership changed during waiting for reply, and the command is not erased with its index occupied by another command,
	// e.g. serving a single client, then it might wait forever. We need to detect the change of term
	kv.logClientRPC(false, args.ClerkId, args.SeqNum, "Started to wait for the applyClientOperation of the command and response assembled")
	// blocking to receive response
	currentContext.replyCond.Wait()
	delete(kv.rpcContexts, indexOrLeader)
	response := currentContext.Response
	if !currentContext.isMatchedWithResponse() {
		// mismatch found, the log entry must have been cleaned up, client must retry
		reply.Err = ErrLogEntryErased
		kv.logClientRPC(false, args.ClerkId, args.SeqNum, "RPC Handler waken up but the command is not as it previously sent! The LogEntry must have been erased")
		return
	} else {
		// matched correctly
		reply.Value, reply.Err = response.Value, response.Err
		kv.logClientRPC(false, args.ClerkId, args.SeqNum, "RPC Handler waken up with assembled response matched with the RPC context, send back reply...")
		return
	}
}

// Observe the applyChannel to actually applyClientOperation to the in-memory stateMachine and other fields
// if the server is the server that talks to the client, notify the client
// if the server is the server that talks to the shard sender, notify the shard sender
// by waking up the RPC thread
func (kv *ShardKV) applyChannelObserver() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		// must wait until the kv.replyEntry has been properly handled to applyClientOperation the next command and update kv.replyEntry
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
			// reply if there is a matched RPC handler (i.e. the leader that talked to the client/shard sender is the current server)
			if ok {
				rpcContext.deliverResponseAndNotify(response)
				kv.logClientRPC(false, response.ClerkId, response.SeqNum, "Notify the RPC handler with the reply message!")
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
				kv.logService(true, "ERROR: Index deserialized from snapshot not equal to the index in the applyClientOperation message!")
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
// For InstallShards and Client Operation, retry the RpcResponse
// For IncrementConfig, where there is no RPC context associated, return nil
func (kv *ShardKV) validateAndApply(operation *Op) *RpcResponse {
	if operation == nil {
		kv.logService(true, "ERROR: The operation to execute is a nil pointer!")
	}
	// Scenario 1: incrementConfig
	if operation.Type == INCREMENTCONFIG {
		kv.validateAndApplyIncrementConfig(operation)
		return nil // IncrementConfig does not have associated RPC handler
	}
	// Scenario 2: installShard
	if operation.Type == INSTALLSHARD {
		response := kv.validateAndApplyInstallShard(operation)
		return response
	}
	// Scenario 3: clientOperation
	cachedEntry, ok := kv.duplicateTable[operation.ClerkId]
	// not outdated or duplicated
	var response *RpcResponse
	if !ok || cachedEntry.SeqNum < operation.SeqNum {
		response = kv.applyClientOperation(operation)
		// store the result (especially GET) to duplicateTable
		kv.duplicateTable[operation.ClerkId] = response // by pointer assign to duplicateTable
	} else if cachedEntry.SeqNum == operation.SeqNum {
		// duplicated, need to resend reply
		response = cachedEntry
		kv.logClientRPC(false, operation.ClerkId, operation.SeqNum, "Executed command found in applyClientOperation channel and must reply again")
	} else {
		// lagged behind operation, the response will be ignored by clerk because the clerk has received reply
		kv.logClientRPC(false, operation.ClerkId, operation.SeqNum, "Executed command found in applyClientOperation channel and the client"+
			" is guaranteed to have seen the response! Simply ignored!")
		// just return an empty response as the client is guaranteed to see the response already
		// it will be discarded by the clerk anyway
		response = &RpcResponse{
			ClerkId: operation.ClerkId,
			SeqNum:  operation.SeqNum,
			CommandId: CommandUniqueIdentifier{
				Type:    operation.Type,
				ClerkId: operation.ClerkId,
				SeqNum:  operation.SeqNum,
			},
			Err: OK,
		}
	}
	return response
}

// check duplicates and Apply the operation to increment Config
func (kv *ShardKV) validateAndApplyIncrementConfig(operation *Op) {
	// it should be impossible to have jump or going backward in config updates when applying Increment Config
	newVersion := operation.Config.Num
	if operation.Config.Num > 1+kv.getCurrentVersion() || operation.Config.Num < kv.getCurrentVersion() {
		kv.logService(true, "ERROR: Attempted to update config from %d to %d.", kv.getCurrentVersion(), operation.Config.Num)
	} else if operation.Config.Num == kv.getCurrentVersion() {
		kv.logConfigChange(false, kv.getCurrentVersion(), "This update in configuration has happened before, no need to apply again!")
	} else {
		shardsToReceive, shardsToSend, _, shardsToInitialize := CompareConfigs(kv.gid, kv.config, operation.Config)
		// clear the preclusion set because no longer useful
		for k := range kv.precludedShards {
			delete(kv.precludedShards, k)
		}
		// process each types of shards operation
		// 1. schedule to send shards to other group
		for _, shardId := range shardsToSend {
			var sendJob SendJobContext
			// where to send
			destGid := operation.Config.Shards[shardId]
			// check if data to send already available
			shardData, exists := kv.shardedStateMachines[shardId]
			if !exists { // if the shard is not in the shardedStateMachines, the shard is not ready yet before it no longer owns it
				kv.logShardSender(false, shardId, newVersion, destGid, "Shard not received yet when required to be resent to another group")
				// remove the corresponding previous installation job and give the job a hint that the old version and the new version is the same version
				oldVersionExpected, installJobExists := kv.shardToInstallJobs[shardId]
				if !installJobExists {
					kv.logShardSender(true, shardId, newVersion, destGid, "ERROR: Shard required to be sent is neither in state machine nor in install jobs!")
				}
				delete(kv.shardToInstallJobs, shardId)
				// create a sendJob reflecting the equivalence of old version shard and new version shard
				sendJob = SendJobContext{
					Shard:       shardId,
					OldVersion:  oldVersionExpected,
					NewVersion:  newVersion,
					DestGID:     destGid,
					DestServers: kv.getServerNames(destGid),
				}
			} else {
				// shard already in the stateMachine
				// switch the data to buffer zone
				kv.logShardSender(false, shardId, newVersion, destGid, "Shard switched to buffer zone to be sent")
				delete(kv.shardedStateMachines, shardId)
				kv.shardBuffer[ShardVersion{shardId, newVersion}] = shardData
				// create the job to job-list for asynchronously sending
				sendJob = SendJobContext{
					Shard:       shardId,
					OldVersion:  newVersion,
					NewVersion:  newVersion,
					DestGID:     destGid,
					DestServers: kv.getServerNames(destGid),
				}
			}
			kv.logShardSender(false, shardId, newVersion, destGid, "Produced a job to send shard")
			kv.shardToSendJobs[ShardVersion{sendJob.Shard, sendJob.OldVersion}] = &sendJob
		}
		// 2. create empty shards that are previously not assigned, maintain the invariance
		for _, shardId := range shardsToInitialize {
			// make sure that previously it is not served
			_, exists := kv.shardedStateMachines[shardId]
			if exists {
				kv.logShardReceiver(true, shardId, newVersion, 0, "Try to initialize a shard that is already in state machine")
			}
			kv.shardedStateMachines[shardId] = make(map[string]string)
		}
		// 3. install received shards to the state machine and add yet-to-be-received shards to job list
		for _, shardId := range shardsToReceive {
			sourceGid := kv.config.Shards[shardId]
			// make sure that previously it is not served, maintain the invariance
			_, exists := kv.shardedStateMachines[shardId]
			if exists {
				kv.logShardReceiver(true, shardId, newVersion, sourceGid, "ERROR: Try to receive a shard that is already in the state machine")
			}
			// check if the shard has already been in the buffer
			bufferedShardData, exists := kv.shardBuffer[ShardVersion{shardId, newVersion}]
			if exists {
				kv.logShardReceiver(false, shardId, newVersion, sourceGid, "Install the shard to state machine on applying IncrementConfig")
				delete(kv.shardBuffer, ShardVersion{shardId, newVersion})
				kv.shardedStateMachines[shardId] = bufferedShardData
			} else {
				// data not ready, create the installation job
				kv.logShardReceiver(false, shardId, newVersion, sourceGid, "Create a install job, awaiting the receipt of the shard")
				// check invariance: there should be at most 1 shardToInstall job for each shard.
				// Because install job is created by delegation of a shard and deleted by either the shard getting installed or re-delegated to a new group before installation to the current group
				prevVersion, alreadyExists := kv.shardToInstallJobs[shardId]
				if alreadyExists {
					kv.logShardReceiver(true, shardId, newVersion, sourceGid, "ERROR: Try to create a installJob but the shardId has been associated with a installJob with version %d", prevVersion)
				}
				kv.shardToInstallJobs[shardId] = newVersion
			}
		}
		// replace the config safely
		kv.config = operation.Config
	}
}

// Check duplicates and apply the installation of a shard
func (kv *ShardKV) validateAndApplyInstallShard(operation *Op) *RpcResponse {
	if operation == nil {
		kv.logService(true, "ERROR: The operation to execute is a nil pointer!")
	}
	// for duplicated request, simply reply OK
	if kv.isShardDuplicated(operation.ShardId, operation.Version, operation.SourceGid) {
		kv.logShardReceiver(false, operation.ShardId, operation.Version, operation.SourceGid, "An installShard about to be executed is found duplicated and simply reply OK")
		return &RpcResponse{
			CommandId: CommandUniqueIdentifier{
				Type:    INSTALLSHARD,
				ShardId: operation.ShardId,
				Version: operation.Version,
			},
			Err: OK,
		}
	}
	return kv.applyInstallShard(operation)
}

// apply the installation of a shard
func (kv *ShardKV) applyInstallShard(operation *Op) *RpcResponse {
	// always updates the duplicated table
	for clerkId, response := range operation.DuplicateTable {
		seqNum := response.SeqNum
		prevDupResponse, ok := kv.duplicateTable[clerkId]
		if !ok || seqNum > prevDupResponse.SeqNum {
			kv.duplicateTable[clerkId] = response
			// for logging purpose, -1 represents no previous record in dup table for the clerk
			oldSeqNum := -1
			if ok {
				oldSeqNum = prevDupResponse.SeqNum
			}
			kv.logShardReceiver(false, operation.ShardId, operation.Version, operation.SourceGid, "Duplicated table with clerkID "+base64Prefix(clerkId)+" has been updated with SeqNum from %d to %d", oldSeqNum, seqNum)
		}
	}
	// scenario 1: arrived earlier than the config change, simply add into the buffer for later use
	if operation.Version > kv.getCurrentVersion() {
		kv.shardBuffer[ShardVersion{operation.ShardId, operation.Version}] = operation.ShardData
		kv.logShardReceiver(false, operation.ShardId, operation.Version, operation.SourceGid, "Install shard with version %d larger than current version %d", operation.Version, kv.getCurrentVersion())
		return &RpcResponse{
			CommandId: CommandUniqueIdentifier{
				Type:    INSTALLSHARD,
				ShardId: operation.ShardId,
				Version: operation.Version,
			},
			Err: OK,
		}
	}
	// scenario 2: arrived after the config change, check job lists
	expectedInstallVersion, installJobExists := kv.shardToInstallJobs[operation.ShardId]
	sendJobContext, sendJobExists := kv.shardToSendJobs[ShardVersion{operation.ShardId, operation.Version}]
	// check invariant: one oldVersion can't be simultaneously needed for send job and install job
	if sendJobExists && installJobExists && expectedInstallVersion == operation.Version {
		kv.logShardReceiver(true, operation.ShardId, operation.Version, operation.SourceGid, "ERROR: a shard & version is expected to be installed and resend simultaneously!")
	}
	// check send job
	if sendJobExists {
		// job exists, install the data to buffer with newVersion, note that the sending of data is done async and the send job will be removed after executed
		newVersion := sendJobContext.NewVersion
		kv.shardBuffer[ShardVersion{operation.ShardId, newVersion}] = operation.ShardData
		kv.logShardReceiver(false, operation.ShardId, operation.Version, operation.SourceGid, "Install shard that the group is waiting to resend with old version %d and new version %d", operation.Version, newVersion)
		return &RpcResponse{
			CommandId: CommandUniqueIdentifier{
				Type:    INSTALLSHARD,
				ShardId: operation.ShardId,
				Version: operation.Version,
			},
			Err: OK,
		}
	}
	// check installation job: remove the job and install to state machine
	if installJobExists && expectedInstallVersion == operation.Version {
		// check invariant: when installing shard, there is no existed shard
		if _, ok := kv.shardedStateMachines[operation.ShardId]; ok {
			kv.logShardReceiver(true, operation.ShardId, operation.Version, operation.SourceGid, "ERROR: On installing a shard, conflicting shard is being served in the state machine")
		}
		kv.shardedStateMachines[operation.ShardId] = operation.ShardData
		delete(kv.shardToInstallJobs, operation.ShardId)
		kv.logShardReceiver(false, operation.ShardId, operation.Version, operation.SourceGid, "Install shard to state machine on receipt, current config ID %d", kv.getCurrentVersion())
		return &RpcResponse{
			CommandId: CommandUniqueIdentifier{
				Type:    INSTALLSHARD,
				ShardId: operation.ShardId,
				Version: operation.Version,
			},
			Err: OK,
		}
	} else if !installJobExists {
		kv.logShardReceiver(true, operation.ShardId, operation.Version, operation.SourceGid, "ERROR: On installing a shard, no job exists when install job or send job is expected!")
		return nil
	} else {
		kv.logShardReceiver(true, operation.ShardId, operation.Version, operation.SourceGid, "ERROR: On installing a shard, the expected install version is %d while provided version is %d",
			expectedInstallVersion, operation.Version)
		return nil
	}
}

// Apply the operation to the in-memory kv stateMachine and put the result into kv.replyEntry
func (kv *ShardKV) applyClientOperation(operation *Op) *RpcResponse {
	if operation == nil {
		kv.logService(true, "ERROR: The operation to execute is a nil pointer!")
	}
	response := &RpcResponse{
		ClerkId: operation.ClerkId,
		SeqNum:  operation.SeqNum,
		CommandId: CommandUniqueIdentifier{
			Type:    operation.Type,
			ClerkId: operation.ClerkId,
			SeqNum:  operation.SeqNum,
		},
	}
	shardID := key2shard(operation.Key)
	switch operation.Type {
	case GET:
		val, ok := kv.shardedStateMachines[shardID][operation.Key]
		response.Value = val
		if ok {
			response.Err = OK
		} else {
			response.Err = ErrNoKey
		}
		kv.logClientRPC(false, operation.ClerkId, operation.SeqNum, "GET operation has been executed!")
	case PUT:
		kv.shardedStateMachines[shardID][operation.Key] = operation.Value
		response.Err = OK
		response.Value = "" // no value needed for put
		kv.logClientRPC(false, operation.ClerkId, operation.SeqNum, "PUT operation has been executed!")
	case APPEND:
		kv.shardedStateMachines[shardID][operation.Key] = kv.shardedStateMachines[shardID][operation.Key] + operation.Value
		response.Err = OK
		response.Value = "" // no value needed for append
		kv.logClientRPC(false, operation.ClerkId, operation.SeqNum, "APPEND operation has been executed!")
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
	kv.logService(false, "Start the detector for pending client requests whose log entry is outdated w.r.t. term...")
	for !kv.killed() {
		time.Sleep(time.Duration(StaleDetectorSleepMillis) * time.Millisecond)
		kv.mu.Lock()
		currentTerm, isLeader := kv.rf.GetState()
		kv.logService(false, "Current Term %d; Is server %d leader? "+strconv.FormatBool(isLeader), currentTerm, kv.me)
		for _, rpcContext := range kv.rpcContexts {
			if currentTerm != rpcContext.TermAppended {
				if rpcContext.CommandId.Type == INCREMENTCONFIG {
					kv.logShardReceiver(false, rpcContext.CommandId.ShardId, rpcContext.CommandId.Version, -1, "Outdated waiting rpc handler detected!")
				} else {
					kv.logClientRPC(false, rpcContext.CommandId.ClerkId, rpcContext.CommandId.SeqNum, "Outdated waiting rpc handler detected!")
				}
				reply := &RpcResponse{
					ClerkId:   rpcContext.CommandId.ClerkId,
					SeqNum:    rpcContext.CommandId.SeqNum,
					CommandId: rpcContext.CommandId,
					Err:       ErrTermChanged,
				}
				rpcContext.deliverResponseAndNotify(reply)
				if rpcContext.CommandId.Type == INCREMENTCONFIG {
					kv.logShardReceiver(false, rpcContext.CommandId.ShardId, rpcContext.CommandId.Version, -1, "Notify ErrTermChanged to the handler!")
				} else {
					kv.logClientRPC(false, rpcContext.CommandId.ClerkId, rpcContext.CommandId.SeqNum, "Notify ErrTermChanged to the handler!")
				}
			}
		}
		kv.mu.Unlock()
	}
}

// Periodically issue query for latest config to the shard controller
func (kv *ShardKV) configQueryIssuer() {
	kv.logService(false, "Start to periodically issue queries for latest config if it is leader...")
	for !kv.killed() {
		time.Sleep(time.Duration(QueryConfigMillis) * time.Millisecond)
		kv.mu.Lock()
		// non-leader just sleep
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}
		// increment the version by at most 1 each time
		nextVersion := kv.getCurrentVersion() + 1
		kv.mu.Unlock()
		latestConfig := kv.mck.Query(nextVersion) //blocking operation so no lock
		kv.mu.Lock()
		// TODO: the same config might enter the log multiple times before the config is applied
		if latestConfig.Num == kv.getCurrentVersion()+1 {
			// optionally, prevent client operations on the about-to-be-removed shards from entering the log
			// because these log entries are after the configUpdate entry. They will not be applied anyway
			_, shardsToSend, _, _ := CompareConfigs(kv.gid, kv.config, latestConfig)
			for _, shard := range shardsToSend {
				kv.precludedShards[shard] = true
			}
			op := Op{
				Type:   INCREMENTCONFIG,
				Config: latestConfig,
			}
			kv.logConfigChange(false, latestConfig.Num, "Try to delegate new config to the Raft library")
			indexOrLeader, term, success := kv.rf.Start(op)
			if !success {
				kv.logConfigChange(false, indexOrLeader, "Fail to add new config to log because the server is "+
					"not leader in term %d, with the leader %d", term, indexOrLeader)
			} else {
				kv.logConfigChange(false, latestConfig.Num, "New Config added to the log at index %d!", indexOrLeader)
			}
		} else if latestConfig.Num < kv.getCurrentVersion() || latestConfig.Num > kv.getCurrentVersion()+1 {
			// break the invariant. The linearizability of controller operations prevent it
			kv.logService(true, "ERROR: Latest Config polled from controller has impossible version number!")
		}
		kv.mu.Unlock()
	}
}

// Periodically send shards to other group and waiting for reply
// on receiving reply of OK, the corresponding sendJob will be removed in a replicated fashion (needs to reach consensus)
func (kv *ShardKV) sendShardScheduler() {
	for !kv.killed() {
		time.Sleep(time.Duration(SendShardMillis) * time.Millisecond)
		kv.mu.Lock()
		// Optional: non-leader just sleep.
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}
		// don't have to worry if the shard sender is leader or not.
		// As the sendJob is created on the executing of config change
		// and the commit of config change is replicated in all replica with total order
		// i.e. the same sendJob executed by either leader or follower will send exactly the same shard
		for _, sendJob := range kv.shardToSendJobs {
			if sendJob.Status == NotStarted {
				sendJob.Status = Ongoing
				go kv.sendShard(sendJob)
			}
		}
		kv.mu.Unlock()
	}
}

// attempt to send shard to each server in a group (so that we don't have to maintain the leader for each group)
// if OK is replied by any of the server in the group, set the sendJob status as finished through replication.
// if none of them replied OK, finished by set the status as NotStarted if not Finished.
// if it fails to commit the finish of the job, set the status as NotStarted and return (causing duplication but maintain correctness)
func (kv *ShardKV) sendShard(sendJob *SendJobContext) {
	kv.logShardSender(false, sendJob.Shard, sendJob.NewVersion, sendJob.DestGID, "Start to send a shard!")
	for _, serverName := range sendJob.DestServers {
		serverEnd := kv.makeEnd(serverName)
		args := &InstallShardArgs{}
		reply := &InstallShardReply{}
		ok := serverEnd.Call("ShardKV.HandleInstallShard", args, reply)
	}
}

// Periodically clean the garbage to free some memory
func (kv *ShardKV) garbageCollector() {
	// In my implementation, the versioned shard and the send job will be removed
	// after receiving the reply of successful installation from the other group
	kv.logService(false, "Start to periodically remove the versioned shards on buffer that has already been sent to the other group...")
	for !kv.killed() {
		time.Sleep(time.Duration(GarbageCollectMillis) * time.Millisecond)
		kv.mu.Lock()
		for sv, sendJob := range kv.shardToSendJobs {
			if sendJob.Status == Finished {
				kv.logService(false, "GCed the shard with id %d and version %d", sendJob.Shard, sendJob.NewVersion)
				key := ShardVersion{sendJob.Shard, sendJob.NewVersion}
				if _, ok := kv.shardBuffer[key]; ok {
					delete(kv.shardBuffer, key)
				}
				delete(kv.shardToSendJobs, sv)
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
	if encoder.Encode(kv.shardedStateMachines) != nil {
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
	var shardedStateMachines map[int]map[string]string
	var duplicateTable map[int64]*RpcResponse
	var lastExecutedIndex int
	if decoder.Decode(&shardedStateMachines) != nil {
		kv.logService(false, "Failed to read state machine from snapshot bytes!")
	}
	if decoder.Decode(&duplicateTable) != nil {
		kv.logService(false, "Failed to read duplicate table from snapshot bytes!")
	}
	if decoder.Decode(&lastExecutedIndex) != nil {
		kv.logService(false, "Failed to read last executed index from snapshot bytes!")
	}
	kv.shardedStateMachines = shardedStateMachines
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
	labgob.Register(Op{}) // as the command stored in logs
	labgob.Register(shardctrler.Config{})
	labgob.Register(RpcResponse{}) // as the response cached in duplicate table
	kv := &ShardKV{
		me:                   me,
		maxRaftState:         maxraftstate,
		makeEnd:              makeEndFunc,
		gid:                  gid,
		controllers:          controllers,
		applyCh:              make(chan raft.ApplyMsg),
		dead:                 0,
		shardedStateMachines: make(map[int]map[string]string),
		shardBuffer:          make(map[ShardVersion]map[string]string),
		duplicateTable:       make(map[int64]*RpcResponse),
		rpcContexts:          make(map[int]*RpcContext),
		lastExecutedIndex:    0,
		snapshotThreshold:    int(RaftStateLengthRatio * float64(maxraftstate)),
		config:               *shardctrler.MakeEmptyConfig(),
	}
	kv.mck = shardctrler.MakeClerk(controllers)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.logService(false, "Restart (or start) the service...")

	// load states from persisted snapshot
	kv.deserializeSnapshot(kv.rf.ReadSnapshot())
	kv.cleanStatesPostRecoverFromPersist()
	// run background thread
	go kv.applyChannelObserver()
	go kv.staleRpcContextDetector()
	return kv
}

// perform necessary field cleansing after restore states from persistent states
func (kv *ShardKV) cleanStatesPostRecoverFromPersist() {
	// switch all ongoing send job to not-started so that they can be started again
	defer kv.mu.Unlock()
	kv.mu.Lock()
	// the state change between NotStarted and Ongoing is not replicated while change to Finished is replicated
	// Ongoing is just a transient state to avoid repeatedly send same shards before reply received
	for _, sendJob := range kv.shardToSendJobs {
		if sendJob.Status == Ongoing {
			sendJob.Status = NotStarted
		}
	}
}

// utility functions
func (kv *ShardKV) getCurrentVersion() int {
	return kv.config.Num
}
func CompareConfigs(gid int, oldConfig shardctrler.Config, newConfig shardctrler.Config) ([]int, []int, []int, []int) {
	if gid == 0 {
		panic("Gid in compare Configs should never be 0!")
	}
	oldShards := oldConfig.GetShardsManagedBy(gid)
	newShards := newConfig.GetShardsManagedBy(gid)
	var shardsToReceive, shardsToSend, noChangeShards, shardsToInitialize []int
	for shard := range oldShards {
		_, exists := newShards[shard]
		if exists {
			noChangeShards = append(noChangeShards, shard)
		} else {
			shardsToSend = append(shardsToSend, shard)
		}
	}
	for shard := range newShards {
		_, exists := oldShards[shard]
		if !exists && oldConfig.Shards[shard] != 0 { // previously a shard assigned to another group
			shardsToReceive = append(shardsToReceive, shard)
		} else if !exists && oldConfig.Shards[shard] == 0 { // previously not assigned to any group, create an empty new shard
			shardsToInitialize = append(shardsToInitialize, shard)
		}
	}
	return shardsToReceive, shardsToSend, noChangeShards, shardsToInitialize
}
func (kv *ShardKV) getServerNames(gid int) []string {
	return kv.config.Groups[gid]
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
func (kv *ShardKV) logClientRPC(fatal bool, clerkId int64, seqNum int, format string, args ...interface{}) {
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

// 3. logging info regarding configuration change of the server
func (kv *ShardKV) logConfigChange(fatal bool, newVersion int, format string, args ...interface{}) {
	if ShardKVDebug {
		prefix := fmt.Sprintf("Service-%d-%d ConfigChange-%d->%d: ", kv.gid, kv.me, newVersion-1, newVersion)
		if fatal {
			log.Fatalf(prefix+format+"\n", args...)
		} else {
			log.Printf(prefix+format+"\n", args...)
		}
	}
}

// 4. logging info regarding sending each shard
func (kv *ShardKV) logShardSender(fatal bool, shardId int, shardVersion int, destGid int, format string, args ...interface{}) {
	if !ShardKVDebug {
		return
	}
	prefix := fmt.Sprintf("Service-%d-%d Shard-%d Version-%d", kv.gid, kv.me, shardId, shardVersion)
	if destGid < 0 {
		prefix = prefix + ": "
	} else {
		prefix = prefix + fmt.Sprintf(" DestGroup-%d: ", destGid)
	}
	if fatal {
		log.Fatalf(prefix+format+"\n", args...)
	} else {
		log.Printf(prefix+format+"\n", args...)
	}
}

// 4. logging info regarding receiving each shard
func (kv *ShardKV) logShardReceiver(fatal bool, shardId int, shardVersion int, sourceGid int, format string, args ...interface{}) {
	if !ShardKVDebug {
		return
	}
	prefix := fmt.Sprintf("Service-%d-%d Shard-%d Version-%d", kv.gid, kv.me, shardId, shardVersion)
	if sourceGid < 0 {
		prefix = prefix + ": "
	} else {
		prefix = prefix + fmt.Sprintf(" SourceGroup-%d: ", sourceGid)
	}
	if fatal {
		log.Fatalf(prefix+format+"\n", args...)
	} else {
		log.Printf(prefix+format+"\n", args...)
	}
}
