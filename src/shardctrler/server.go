package shardctrler

import (
	"6.824/raft"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

// Following the similar structure as in the key-value service, except that
// 1. the state machine now is configuration,
// 2. all historical configurations will be remained in memory
// 3. states are small so no snapshot needs to be taken
const StaleDetectorSleepMillis int = 1500

// ControllerCommandResponse is used to cache the result of applied command
type ControllerCommandResponse struct {
	ClerkId int64
	SeqNum  int
	Err     Err
	Config  *Config // only useful for QUERY command
}

// RpcContext is used to record the information related to an RPC thread handling an operation
type RpcContext struct {
	ClerkId      int64
	SeqNum       int
	TermAppended int                        // record the term that the log is appended, invalidate the request if the change of term detected
	replyCond    *sync.Cond                 // used to wake up the waiting RPC handler after response is assembled
	Response     *ControllerCommandResponse // used by the sender thread to send the response to the receiver handler
}

// The constructor for RpcContext, mainly for bind the conditional variable to the mutex of the ShardCtrler
func (sc *ShardCtrler) NewRpcContext(clerkId int64, seqNum int, term int) *RpcContext {
	return &RpcContext{
		ClerkId:      clerkId,
		SeqNum:       seqNum,
		TermAppended: term,
		replyCond:    sync.NewCond(&sc.mu),
		Response:     nil,
	}
}
func (rpcContext *RpcContext) deliverResponseAndNotify(response *ControllerCommandResponse) {
	rpcContext.Response = response
	rpcContext.replyCond.Broadcast()
}

// used as a field to be serialized and persisted as operation parameters
type Op struct {
	// ClerkId and SeqNum uniquely identify the operation
	ClerkId  int64
	SeqNum   int
	Type     ControllerOperationType
	Servers  map[int][]string // used for Join
	GIDs     []int            // used for Leave
	Shard    int              // used for Move
	GID      int              // used for Move
	ConfigID int              // used for Query
}

type ShardCtrler struct {
	mu                sync.Mutex
	me                int
	rf                *raft.Raft
	applyCh           chan raft.ApplyMsg
	dead              int32               // set by Kill()
	maxRaftState      int                 // snapshot if log grows this big
	snapshotThreshold int                 // calculated based on maxRaftState
	rpcContexts       map[int]*RpcContext // for each RPC there will be a rpcContext for it to pass in response and notify the finish of response
	lastExecutedIndex int
	// the state machine history
	configs        []Config                             // indexed by the ConfigID, history of the state machine
	duplicateTable map[int64]*ControllerCommandResponse // used to detect duplicated commands to execute or duplicated client request
}

// The RPC handler for all controller operations
func (sc *ShardCtrler) HandleControllerOperation(args *ControllerOperationArgs, reply *ControllerOperationReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	cache, ok := sc.duplicateTable[args.ClerkId]
	// the command has just been executed, need to reply
	if ok && cache.SeqNum == args.SeqNum {
		reply.Err = cache.Err
		reply.Config = cache.Config
		// without entering raft.Start(), no leader info can be reported back to the client
		sc.logRPC(false, args.ClerkId, args.SeqNum, "Executed command found in RPC handler and must reply again")
		return
	}
	// outdated command, the response for which the client must have seen, irrelevant,
	if ok && cache.SeqNum > args.SeqNum {
		reply.Err = OK
		sc.logRPC(false, args.ClerkId, args.SeqNum, "Executed command found in RPC handler and the client"+
			" is guaranteed to have seen the response! Simply ignored!")
		return
	}
	op := Op{
		ClerkId:  args.ClerkId,
		SeqNum:   args.SeqNum,
		Type:     args.Type,
		Servers:  args.Servers,
		GIDs:     args.GIDs,
		Shard:    args.Shard,
		GID:      args.GID,
		ConfigID: args.ConfigID,
	}
	sc.logRPC(false, args.ClerkId, args.SeqNum, "Try to delegate the controller operation to the Raft library")

	indexOrLeader, term, success := sc.rf.Start(op)
	if !success {
		reply.Err = ErrWrongLeader
		sc.logRPC(false, args.ClerkId, args.SeqNum, "Server called by the clerk is not leader controller for term %d!", term)
		return
	}
	// scenario: successfully append the command to the log
	// check index collision
	prevContext, ok := sc.rpcContexts[indexOrLeader]
	if ok {
		// there is a rpcContext outstanding whose index colliding with the current index,
		// the old entry must have been erased so that the current entry can be appended at that index

		// Here is a very interesting corner case: if the clerkId, seqNum, index and the leaderId are all the same
		// (which implies a different term), we can still suggest the handler to resend because next time it is
		// most likely to be detected by duplicateTable (either during initial check or check before execution)
		sc.logRPC(false, args.ClerkId, args.SeqNum, "There is already an RpcContext at the index where the command can reside")
		response := &ControllerCommandResponse{
			ClerkId: prevContext.ClerkId,
			SeqNum:  prevContext.SeqNum,
			Err:     ErrLogEntryErased,
		}
		prevContext.deliverResponseAndNotify(response)
	}
	currentContext := sc.NewRpcContext(args.ClerkId, args.SeqNum, term)
	sc.rpcContexts[indexOrLeader] = currentContext
	// If the leadership changed during waiting for reply, and the command is not erased with its index occupied by another command,
	// e.g. serving a single client, then it might wait forever. We need to detect the change of term
	sc.logRPC(false, args.ClerkId, args.SeqNum, "Started to wait for the apply of the command and response assembled")
	// blocking to receive response
	currentContext.replyCond.Wait()
	delete(sc.rpcContexts, indexOrLeader)
	response := currentContext.Response
	if response == nil {
		sc.logRPC(true, args.ClerkId, args.SeqNum, "ERROR: RPC handler woken up but the response is nil!")
	}
	sc.logRPC(false, args.ClerkId, args.SeqNum, "Received assembled response, wake up the RPC handler")
	if response.SeqNum != args.SeqNum || response.ClerkId != args.ClerkId || response.Err == ErrLogEntryErased {
		// mismatch found, the log entry must have been cleaned up
		reply.Err = ErrLogEntryErased
		sc.logRPC(false, args.ClerkId, args.SeqNum, "Log entry appended found removed! Reply error")
		return
	} else {
		// matched correctly
		reply.Config, reply.Err = response.Config, response.Err
		sc.logRPC(false, args.ClerkId, args.SeqNum, "Correctly reply the client the response generated by applyChannelObserver")
		return
	}
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Observe the applyChannel to actually apply to the current configuration
// if the server is the server that talks to the client, notify the client
// by waking up the RPC thread
func (sc *ShardCtrler) applyChannelObserver() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh
		// must wait until the sc.replyEntry has been properly handled to apply the next command and update sc.replyEntry
		sc.mu.Lock()
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)          // type assertion into Op so that the operation can be applied
			response := sc.validateAndApply(&op) // check if the operation is up-to-date

			// update the executed index if executed
			if applyMsg.CommandIndex > sc.lastExecutedIndex {
				if applyMsg.CommandIndex != sc.lastExecutedIndex+1 {
					sc.logController(true, "ERROR: Jump found in the index of executed commands!")
				}
				sc.lastExecutedIndex = applyMsg.CommandIndex
			}
			rpcContext, ok := sc.rpcContexts[applyMsg.CommandIndex]
			// reply if there is a matched RPC handler (i.e. the leader that talked to the client is the current server)
			if ok {
				rpcContext.deliverResponseAndNotify(response)
				sc.logRPC(false, response.ClerkId, response.SeqNum, "Notify the RPC handler with the reply message!")
			}
			sc.mu.Unlock()
		} else {
			sc.logController(true, "ERROR: ShardController does not support snapshot but snapshot received!")
		}
	}
}

// find if the operation is outdated and for outdated operation if the result needs to be correctly replied,
// update the replyEntry accordingly
// if the seqNum is smaller than the cached seqNum for the clerk, the clerk must have received the reply because
// all operations are synchronous in Raft, so we can safely do nothing
func (sc *ShardCtrler) validateAndApply(operation *Op) *ControllerCommandResponse {
	if operation == nil {
		sc.logController(true, "ERROR: The operation to execute is a nil pointer!")
	}
	cachedEntry, ok := sc.duplicateTable[operation.ClerkId]
	// not outdated or duplicated
	var response *ControllerCommandResponse
	if !ok || cachedEntry.SeqNum < operation.SeqNum {
		response = sc.apply(operation)
		// store the response (especially QUERY) to duplicateTable
		sc.duplicateTable[operation.ClerkId] = response // by pointer assign to duplicateTable
	} else if cachedEntry.SeqNum == operation.SeqNum {
		// duplicated, need to resend reply
		response = cachedEntry
		sc.logRPC(false, operation.ClerkId, operation.SeqNum, "Executed command found in apply channel and must reply again")
	} else {
		// lagged behind operation, the response will be ignored by clerk because the clerk has received reply
		sc.logRPC(false, operation.ClerkId, operation.SeqNum, "Executed command found in apply channel and the client"+
			" is guaranteed to have seen the response! Simply ignored!")
		// just return an empty response as the client is guaranteed to see the response already
		// it will be discarded by the clerk anyway
		response = &ControllerCommandResponse{
			ClerkId: operation.ClerkId,
			SeqNum:  operation.SeqNum,
			Err:     OK,
		}
	}
	return response
}

// Apply the operation to the in-memory configs stateMachine and put the result into sc.replyEntry
func (sc *ShardCtrler) apply(operation *Op) *ControllerCommandResponse {
	if operation == nil {
		sc.logController(true, "ERROR: The operation to execute is a nil pointer!")
	}
	response := &ControllerCommandResponse{
		ClerkId: operation.ClerkId,
		SeqNum:  operation.SeqNum,
	}
	switch operation.Type {
	case QUERY:
		// for out-of-boundary index, simply return the latest configuration
		if operation.ConfigID < 0 || operation.ConfigID >= len(sc.configs) {
			response.Config = &sc.configs[len(sc.configs)-1]
		} else {
			response.Config = &sc.configs[operation.ConfigID]
		}
		response.Err = OK
		sc.logRPC(false, operation.ClerkId, operation.SeqNum, "QUERY operation has been executed!")
	case JOIN:
		// create a new config out of previous config
		newConfig := sc.initializeNewConfig()
		// update server lists for the new groupS
		for gid, servers := range operation.Servers {
			newConfig.Groups[gid] = servers
		}
		// re-balance
		sc.rebalance(newConfig)
		sc.configs = append(sc.configs, *newConfig)
		response.Err = OK
		sc.logRPC(false, operation.ClerkId, operation.SeqNum, "JOIN operation has been executed!")
	case LEAVE:
		// create a new config out of previous config
		newConfig := sc.initializeNewConfig()
		// create a set of group ids to be removed and remove them from gid-servers mappings
		gid2remove := make(map[int]bool)
		for _, gid := range operation.GIDs {
			gid2remove[gid] = true
			delete(newConfig.Groups, gid)
		}
		// set shards as not assigned
		prevConfig := sc.configs[len(sc.configs)-1]
		for shardId, gid := range prevConfig.Shards {
			_, exists := gid2remove[gid]
			if exists {
				newConfig.Shards[shardId] = 0 // set as not assigned to any group
			}
		}
		// re-balance the config
		sc.rebalance(newConfig)
		sc.configs = append(sc.configs, *newConfig)
		response.Err = OK
		sc.logRPC(false, operation.ClerkId, operation.SeqNum, "LEAVE operation has been executed!")
	case MOVE:
		newConfig := sc.initializeNewConfig()
		newConfig.Shards[operation.Shard] = operation.GID
		sc.configs = append(sc.configs, *newConfig)
		response.Err = OK
		sc.logRPC(false, operation.ClerkId, operation.SeqNum, "MOVE operation has been executed!")
	default:
		sc.logController(true, "ERROR: unknown type of controller operation to be applied!")
	}
	return response
}

// initialize a Config to be the same as last Config except the index
func (sc *ShardCtrler) initializeNewConfig() *Config {
	prevConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    prevConfig.Num + 1,
		Shards: prevConfig.Shards, // go array is passed by value (copy happened)
		Groups: deepCopyMap(prevConfig.Groups),
	}
	return &newConfig
}

// Re-balance a config to achieve 2 invariants:
// 1. no shards not yet assigned unless no groups at all
// 2. the maximum number of a shards managed by a group minus the minimum number will not exceed 1
// the pointer of ShardCtrler is passed for logging purpose
func (sc *ShardCtrler) rebalance(cf *Config) {
	// scenario 1: no groups at all
	if len(cf.Groups) == 0 {
		for _, gid := range cf.Shards {
			if gid != 0 {
				sc.logController(true, "ERROR: non-zero gid found when no groups at all")
			}
		}
		return
	}
	// scenario 2: with some groups
	// create the auxiliary gid to shards
	gid2Shards := make(map[int][]int)
	gid2Shards[0] = make([]int, 0)
	for gid, _ := range cf.Groups {
		gid2Shards[gid] = make([]int, 0)
	}
	for shardId, gid := range cf.Shards {
		_, exists := gid2Shards[gid]
		if !exists {
			sc.logController(true, "ERROR: found some shards assigned to a non-existed group id %d", gid)
		}
		gid2Shards[gid] = append(gid2Shards[gid], shardId)
	}
	// first: assign each shard that is not yet assigned
	for len(gid2Shards[0]) > 0 {
		destGID, _ := findGidWithMinShards(cf, gid2Shards)
		var passedShardID int
		gid2Shards[0], gid2Shards[destGID], passedShardID = passFirstElement(gid2Shards[0], gid2Shards[destGID])
		cf.Shards[passedShardID] = destGID
	}
	// last: assign shards from maxGid to minGid until the gap <= 1
	for {
		minGID, minNum := findGidWithMinShards(cf, gid2Shards)
		maxGID, maxNum := findGidWithMaxShards(cf, gid2Shards)
		if maxNum-minNum <= 1 {
			break
		}
		var passedShardID int
		gid2Shards[maxGID], gid2Shards[minGID], passedShardID = passFirstElement(gid2Shards[maxGID], gid2Shards[minGID])
		cf.Shards[passedShardID] = minGID
	}
}

// return the gid associated with the minimum shards and the associated num of shards
// in case a tie, always return the smallest gid
func findGidWithMinShards(cf *Config, gid2Shards map[int][]int) (int, int) {
	minGid := -1
	minLength := len(cf.Shards) + 1
	for gid, shards := range gid2Shards {
		// ignore gid 0, which is not a group
		if gid == 0 {
			continue
		}
		currentLength := len(shards)
		if currentLength < minLength || (currentLength == minLength && gid < minGid) {
			minGid = gid
			minLength = currentLength
		}
	}
	if minGid <= 0 {
		log.Fatalf("ERROR: The gid assigned with minimum number of shards does not exists with gid %d\n", minGid)
	}
	return minGid, minLength
}

// return the gid associated with the maximum shards and the associated num of shards
// in case a tie, always return the minimum gid
func findGidWithMaxShards(cf *Config, gid2Shards map[int][]int) (int, int) {
	maxGid := -1
	maxLength := -1
	for gid, shards := range gid2Shards {
		// ignore gid 0, which is not a group
		if gid == 0 {
			continue
		}
		currentLength := len(shards)
		if currentLength > maxLength || (currentLength == maxLength && gid < maxGid) {
			maxGid = gid
			maxLength = currentLength
		}
	}
	if maxGid <= 0 {
		log.Fatalf("ERROR: The gid assigned with maximum number of shards does not exists with gid %d\n", maxGid)
	}
	return maxGid, maxLength
}

// Periodically check if the term has changed for each ongoing RPC requests. If the term changed, different from its
// previous term, the detector will help to generate response to client, asking the client to retry
// Note that for safety, the command is still likely to be committed, which might be unsafe. But in the synchronous
// setting, the client will always retry. Therefore, there is no safety issue in this particular scenario.
func (sc *ShardCtrler) staleRpcContextDetector() {
	sc.logController(false, "Start the detector for pending client requests whose log entry is outdated in term...")
	for !sc.killed() {
		time.Sleep(time.Duration(StaleDetectorSleepMillis) * time.Millisecond)
		sc.mu.Lock()
		currentTerm, isLeader := sc.rf.GetState()
		sc.logController(false, "Current Term %d; Is server %d leader? "+strconv.FormatBool(isLeader), currentTerm, sc.me)
		for _, rpcContext := range sc.rpcContexts {
			if currentTerm != rpcContext.TermAppended {
				sc.logRPC(false, rpcContext.ClerkId, rpcContext.SeqNum, "Outdated waiting rpc handler detected!")
				reply := &ControllerCommandResponse{
					ClerkId: rpcContext.ClerkId,
					SeqNum:  rpcContext.SeqNum,
					Err:     ErrTermChanged,
				}
				rpcContext.deliverResponseAndNotify(reply)
				sc.logRPC(false, rpcContext.ClerkId, rpcContext.SeqNum, "Notify ErrTermChanged to the handler!")
			}
		}
		sc.mu.Unlock()
	}
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})                        // as the command stored in logs
	labgob.Register(ControllerCommandResponse{}) // as the response cached in duplicate table

	sc := &ShardCtrler{
		me:                me,
		applyCh:           make(chan raft.ApplyMsg),
		dead:              0,
		configs:           make([]Config, 1),
		duplicateTable:    make(map[int64]*ControllerCommandResponse),
		rpcContexts:       make(map[int]*RpcContext),
		lastExecutedIndex: 0,
	}
	// add an empty configuration
	sc.configs[0].Groups = map[int][]string{}

	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.logController(false, "Restart (or start) the controller replica...")

	// run background thread
	go sc.applyChannelObserver()
	go sc.staleRpcContextDetector()
	return sc
}

// logging functions
// 1. logging info regarding the controller replica itself
func (sc *ShardCtrler) logController(fatal bool, format string, args ...interface{}) {
	if ControllerDebug {
		prefix := fmt.Sprintf("Controller-%d: ", sc.me)
		if fatal {
			log.Fatalf(prefix+format+"\n", args...)
		} else {
			log.Printf(prefix+format+"\n", args...)
		}
	}
}

// 2. logging info regarding the communication between controller and service
func (sc *ShardCtrler) logRPC(fatal bool, clerkId int64, seqNum int, format string, args ...interface{}) {
	if ControllerDebug {
		// convert id to base64 and take a prefix for logging purpose
		clerkStr := base64Prefix(clerkId)
		prefixService := fmt.Sprintf("Controller-%d ", sc.me)
		prefixClerk := "Admin-" + clerkStr + " "
		prefixSeq := fmt.Sprintf("Seq-%d: ", seqNum)
		fmtString := fmt.Sprintf(format, args...)
		if fatal {
			log.Fatal(prefixService + prefixClerk + prefixSeq + fmtString + "\n")
		} else {
			log.Print(prefixService + prefixClerk + prefixSeq + fmtString + "\n")
		}
	}
}

// deepCopyMap takes a map of int to slice string and returns a deep copy of it.
func deepCopyMap(originalMap map[int][]string) map[int][]string {
	newMap := make(map[int][]string)
	for key, oldSlice := range originalMap {
		newSlice := make([]string, len(oldSlice))
		copy(newSlice, oldSlice)
		newMap[key] = newSlice
	}
	return newMap
}

// pass the first element of src to dst, return them and the passed value
func passFirstElement(src []int, dst []int) ([]int, []int, int) {
	val := src[0]
	dst = append(dst, src[0])
	src = src[1:]
	return src, dst, val
}
