package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

const RaftDebug bool = false

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For snapshot delivery:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type ServerIdentity int32

const (
	LEADER ServerIdentity = iota
	CANDIDATE
	FOLLOWER
)

func (identity ServerIdentity) String() string {
	switch identity {
	case LEADER:
		return "LEADER"
	case CANDIDATE:
		return "CANDIDATE"
	case FOLLOWER:
		return "FOLLOWER"
	default:
		return "UNKNOWN"
	}
}

const (
	MinElectionTimeoutMillis  int64 = 500
	MaxElectionTimeoutMillis  int64 = 1000
	AppendEntriesPeriodMillis int64 = 100
	TickerPeriodMillis        int64 = 30
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your Data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyChannel      chan ApplyMsg
	currentTerm       int // The term it is in
	LeaderId          int // The leader of current term, -1 represents unknown. used for redirection
	votedFor          int // -1 represents not voted yet
	log               []LogEntry
	commitIndex       int // serves as the right boundary of a slicing window
	lastApplied       int // serves as the left boundary of a slicing window
	identity          ServerIdentity
	nextElectionTime  time.Time  // The next time to initiate an election, can be reset (postponed)
	nextHeartbeatTime time.Time  // The next time to broadcast AppendEntries
	commitCond        *sync.Cond // conditional variable listening to the commit of entry to happen
	// volatile states for leader
	nextIndex  []int //for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server

	// snapshot and related fields
	snapshot          []byte
	snapshotLastIndex int // last index included in snapshot
	snapshotLastTerm  int // the term of last index included in snapshot
}

// A single entry in log
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int // The Term at which the entry is appended to the log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.identity == LEADER
	rf.mu.Unlock()
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	rf.logServer("Save the server states to stable storage!")
	data := rf.serializeRaftState()
	rf.persister.SaveRaftState(data)
}

// used when both snapshot and states changed
func (rf *Raft) persistWithSnapshot() {
	rf.logServer("Save the server states to stable storage!")
	data := rf.serializeRaftState()
	// according to protocol, empty snapshot should be saved as nil
	if len(rf.snapshot) == 0 {
		rf.snapshot = nil
	}

	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

// define what fields to marshall
func (rf *Raft) serializeRaftState() []byte {
	writeBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writeBuffer)
	if encoder.Encode(rf.currentTerm) != nil {
		rf.logServer("Failed to encode current term!")
	}
	if encoder.Encode(rf.votedFor) != nil {
		rf.logServer("Failed to encode who the server voted for!")
	}
	if encoder.Encode(rf.log) != nil {
		rf.logServer("Failed to encode log!")
	}
	if encoder.Encode(rf.snapshotLastIndex) != nil {
		rf.logServer("Failed to encode the last term in snapshot!")
	}
	if encoder.Encode(rf.snapshotLastTerm) != nil {
		rf.logServer("Failed to encode the last index in snapshot!")
	}
	return writeBuffer.Bytes()
}

// api for the service to query if state size too large
func (rf *Raft) IsStateSizeAbove(threshold int) bool {
	return rf.persister.RaftStateSize() >= threshold
}

// api for the service to read snapshot from disk
func (rf *Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

// restore previously persisted Raft state. Used only when the server restarts (from crash) along with read snapshot
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state
		return
	}
	rf.logServer("Read from stable storage states of the server!")
	readBuffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(readBuffer)
	var currentTerm int
	var votedFor int
	var newLog []LogEntry
	var snapshotLastIndex int
	var snapshotLastTerm int
	if decoder.Decode(&currentTerm) != nil {
		rf.logServer("Failed to read current term from persistent states!")
	}
	if decoder.Decode(&votedFor) != nil {
		rf.logServer("Failed to read who the server voted for from persistent states!")
	}
	if decoder.Decode(&newLog) != nil {
		rf.logServer("Failed to read log from persistent states!")
	}
	if decoder.Decode(&snapshotLastIndex) != nil {
		rf.logServer("Failed to read the last index in snapshot from persistent states!")
	}
	if decoder.Decode(&snapshotLastTerm) != nil {
		rf.logServer("Failed to read the last term in snapshot from persistent states!")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = newLog
	rf.snapshotLastIndex = snapshotLastIndex
	rf.snapshotLastTerm = snapshotLastTerm

	// restore volatile states according to persistent states
	rf.commitIndex = max(rf.commitIndex, rf.snapshotLastIndex)
	// Should LastApplied be restored?
	// I previously believe I should not restore rf.lastApplied because it is likely that the snapshot is not applied but persisted
	// In the case where the snapshot is installed from a peer (installSnapshot) and persisted successfully but not applied
	// to the state machine before the server crash.
	// however, if the server crashed in between, after the restart the service will also recover from the snapshot,
	// which will update the lastExecutedIndex field of the service to the snapshotLastIndex.
	// Therefore, lastApplied should be restored.
	rf.lastApplied = max(rf.lastApplied, rf.snapshotLastIndex)

	// Idempotence of the operations guaranteed the correctness when duplicated applies happened after crash.
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// Deprecated in Spring 2022 version of this lab, simply return true
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	return true
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logServer("Service called Snapshot...")

	// To avoid that a more up-to-date snapshot from the peer replaced by a less up-to-date snapshot from the service
	if index <= rf.snapshotLastIndex {
		rf.logServer("Less or equally up-to-date snapshot received and ignored")
		return
	}
	// If Raft received snapshot from the service, the entry in its log with index <= the snapshot index is guaranteed to be committed
	// because no committed entry will ever be replaced by another entry in Raft!
	// Therefore, the term can be accessed from the log

	prev := rf.snapshotLastIndex // for logging

	// guaranteed to have the entry because it has been committed and not replaced
	lastTerm := rf.getLogEntryTerm(index) // must happen before truncation. Once truncated the term is not accessible

	rf.replaceSnapshotAndUpdate(snapshot, index, lastTerm)

	rf.persistWithSnapshot()
	rf.logServer("Snapshot updated from the service: last Index increased from %d to %d.", prev, index)
}

// Replace the snapshot with a new snapshot, should be called only if the snapshot is believed more up-to-date
func (rf *Raft) replaceSnapshotAndUpdate(snapshot []byte, lastIndex int, lastTerm int) {
	rf.snapshot = snapshot

	rf.truncateLogUpTo(lastIndex) // must happen before update snapshot last index, because truncation needs the last index to be correct to calculate offset

	rf.snapshotLastIndex = lastIndex
	rf.snapshotLastTerm = lastTerm
}

// The message from candidate to other nodes to request votes
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int // used for voters to decide whose is more update-to-date and grant vote
	LastLogTerm  int // used for voters to decide whose is more update-to-date and grant vote
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term         int
	VotedGranted bool
}

// The message from leader to other nodes acts as:
// 1. a heartbeat that reset the election timeout
// 2. carries the entries to append
// 3. carries the Index before and through which logs to commit
type AppendEntriesArgs struct {
	Term              int
	LeaderId          int // used for redirection
	PrevLogIndex      int // used to decide whether previous entries match and to reject entries
	PrevLogTerm       int // used to decide whether previous entries match and to reject entries
	Entries           []LogEntry
	LeaderCommitIndex int // used to inform follower to commit
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term            int
	EntriesAccepted bool
	//  if the entries are not accepted,
	//  return the first index of that conflicted term or the first entry it misses if too short
	//  instruction about the index that the Leader should send entries from to accelerate success
	ConflictIndex int
	ConflictTerm  int
}

// The message from leader to a node if the log of that node is way too lagging behind
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshot RPC reply structure
type InstallSnapshotReply struct {
	Term int
}

// AppendEntriees RPC handler
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logProducer(args.LeaderId,
		"Received AppendEntries in leader term %d",
		args.Term)

	// update the Term and identity if higher term encountered on receiving RPC message
	rf.compareTermAndUpdateStates(args.Term)

	// reset election timeout if AppendEntries are from leader of current term
	if args.Term == rf.currentTerm {
		rf.LeaderId = args.LeaderId
		rf.nextElectionTime = time.Now().Add(generateRandomTimeout())
		if rf.identity == CANDIDATE {
			// give up the election if someone else declared winning
			rf.identity = FOLLOWER
			rf.logProducer(args.LeaderId, "Give up election because the winner sent AppendEntries!")
		}
	}
	// update reply entries (2B)
	reply.Term = rf.currentTerm
	// Scenario 1: outdated AppendEntries
	if args.Term < rf.currentTerm {
		reply.EntriesAccepted = false
		rf.logProducer(args.LeaderId, "Reject AppendEntries because the leader is outdated with term %d", args.Term)
		return
	}

	// Scenario 2: prevLogIndex is not in the snapshot of the recipient and
	// does not match the logEntry (either by mismatch term or log too short)
	if rf.snapshotLastIndex < args.PrevLogIndex &&
		(rf.getLastLogIndex()+1 <= args.PrevLogIndex ||
			rf.getLogEntryTerm(args.PrevLogIndex) != args.PrevLogTerm) {
		reply.EntriesAccepted = false
		// case 1: mismatch because no such entry at PrevLogIndex
		if rf.getLastLogIndex()+1 <= args.PrevLogIndex {
			reply.ConflictIndex = rf.getLastLogIndex() + 1
			reply.ConflictTerm = -1 // a mismatching term will lead to resend from this index
		} else {
			// case 2: mismatch because conflicted term at PrevLogIndex, linear probing to find the first current-term entry
			reply.ConflictTerm = rf.getLogEntryTerm(args.PrevLogIndex)
			// it is not possible for entries overed in follower snapshot to conflict! so i >= 1+rf.snapshotLastIndex
			for i := args.PrevLogIndex; i >= 1+rf.snapshotLastIndex && rf.getLogEntryTerm(i) == reply.ConflictTerm; i-- {
				reply.ConflictIndex = i
			}
		}
		rf.logProducer(args.LeaderId, "Reject AppendEntries because previous log does not match with index %d!", args.PrevLogIndex)
		return
	}
	if rf.snapshotLastIndex == args.PrevLogIndex && rf.snapshotLastTerm != args.PrevLogTerm {
		log.Fatalf("Server %d: ERROR: Committed entry in snapshot has a different term from that term in Leader!", rf.me)
	}
	// Scenario 3: matched all previous entries, start to update log afterwards
	// including the case where prevLogIndex of the appendEntries corresponds to an entry replaced by snapshot
	// (snapshot only replace committed entries and committed entries must match in Raft)

	// reply success in all following scenario
	reply.EntriesAccepted = true
	var entries []LogEntry
	var startIndex int
	// scanning mismatch from the first index in args.Entries or rf.log, whichever is larger
	if rf.snapshotLastIndex > args.PrevLogIndex {
		startIndex = rf.snapshotLastIndex + 1
		startOffset := rf.snapshotLastIndex - args.PrevLogIndex
		if startOffset > len(args.Entries) {
			// all elements the server sent has been committed and to avoid out-of-bound error, return immediately
			return
		}
		entries = args.Entries[startOffset:]
	} else {
		startIndex = args.PrevLogIndex + 1
		entries = args.Entries
	}

	logModified := false // the log is modified iff either happened
	for i, entry := range entries {
		if startIndex+i >= rf.getLastLogIndex()+1 {
			rf.log = append(rf.log, entry)
			logModified = true
		} else if rf.getLogEntryTerm(startIndex+i) != entry.Term {
			// by log matching rule, examining term and index is sufficient
			rf.log = append(rf.log[:rf.indexToLogOffset(startIndex+i)], entry)
			logModified = true
		}
	}
	rf.logProducer(args.LeaderId, "Accept AppendEntries!")
	// persist if log modified, must happen before apply to the service (write-ahead logging)
	if logModified {
		rf.persist()
	}
	// Find the last index in the newly appended entries, avoid out-of-bound error
	lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommitIndex > rf.commitIndex {
		prev := rf.commitIndex // for logging purpose
		rf.commitIndex = min(args.LeaderCommitIndex, lastNewEntryIndex)
		rf.commitCond.Broadcast()
		rf.logProducer(args.LeaderId, "Accept of AppendEntries increases the commitIndex from %d to %d", prev, rf.commitIndex)
	}
	return
}

// RequestVote RPC handler.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rf.logProducer(args.CandidateId,
		"Received RequestVote in candidate term %d",
		args.Term)

	// update the Term and identity if higher term encountered on receiving RPC message
	rf.compareTermAndUpdateStates(args.Term)

	// update reply values by not granting by default
	reply.Term = rf.currentTerm
	reply.VotedGranted = false

	// decide if grant vote
	if rf.shouldGrantVote(args.CandidateId,
		args.Term,
		args.LastLogTerm,
		args.LastLogIndex) {
		rf.logProducer(args.CandidateId, "Server grant vote!")
		// grant the vote
		reply.VotedGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		// postpone the election (reset the timeout)
		rf.nextElectionTime = time.Now().Add(generateRandomTimeout())
	}
	rf.mu.Unlock()
}

func (rf *Raft) shouldGrantVote(candidateId int, candidateTerm int,
	candidateLastLogTerm int, candidateLastLogIndex int) bool {
	return candidateTerm >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == candidateId) &&
		!rf.isMoreUpdated(candidateLastLogTerm, candidateLastLogIndex)
}

// InstallSnapshot RPC handler.
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logProducer(args.LeaderId, "Received InstallSnapshot request")
	// update the Term and identity if higher term encountered on receiving RPC message
	rf.compareTermAndUpdateStates(args.Term)
	if args.Term == rf.currentTerm {
		rf.LeaderId = args.LeaderId // update LeaderId
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.snapshotLastIndex {
		// outdated or repeated snapshot, ignore
		rf.logProducer(args.LeaderId, "The snapshot received from RPC is either outdated or repeated, ignored")
		return
	}
	// update states according to the new snapshot
	// we don't know if the log will be truncated or entirely discarded
	rf.replaceSnapshotAndUpdate(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	// update commitIndex, only deliver snapshot to service when commitIndex increased!
	// Otherwise, the service might have more up-to-date snapshot

	// must persist before delivery to service. Never give the opportunity of service replied to client without persist the state
	rf.persistWithSnapshot()
	if rf.commitIndex < rf.snapshotLastIndex {
		prev := rf.commitIndex
		rf.commitIndex = rf.snapshotLastIndex
		// notify the increase of commitIndex
		rf.commitCond.Broadcast()
		rf.logProducer(args.LeaderId, "Accept of InstallSnapshot increases the commitIndex from %d to %d", prev, rf.commitIndex)
	}
	rf.logProducer(args.LeaderId, "Finished the handling of InstallSnapshot request")
}

// RPC encapsulation for sending RPC requests

// send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	rf.logConsumer(serverId, "Send RequestVote to a server")
	rf.mu.Unlock()
	ok := rf.peers[serverId].Call("Raft.HandleRequestVote", args, reply)
	if !ok {
		rf.mu.Lock()
		rf.logConsumer(serverId, "RequestVote RPC encountered issue")
		rf.mu.Unlock()
	}
	return ok
}
func (rf *Raft) sendAppendEntries(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	rf.logConsumer(serverId, "Send AppendEntries to a server")
	rf.mu.Unlock()
	ok := rf.peers[serverId].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(serverId int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	rf.logConsumer(serverId, "Send InstallSnapshot to a server")
	rf.mu.Unlock()
	ok := rf.peers[serverId].Call("Raft.HandleInstallSnapshot", args, reply)
	return ok
}

// send AppendEntries message and handling the response
func (rf *Raft) sendAndHandleAppendEntries(serverId int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	// handling RPC
	ok := rf.sendAppendEntries(serverId, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// response not received
	if !ok {
		rf.logConsumer(serverId, "AppendEntries RPC encountered issue")
		return
	}
	// update term according to the protocol
	rf.compareTermAndUpdateStates(reply.Term)
	// if the AppendEntries are rejected because outdated, don't retry just end
	if rf.currentTerm > args.Term {
		rf.logConsumer(serverId, "AppendEntries might be rejected as term increased from %d, don't retry", args.Term)
		return
	}

	if reply.EntriesAccepted {
		rf.logConsumer(serverId, "Leader notified that AppendEntries accepted by a follower!")
		// update nextIndex and matchIndex,
		// potential out-of-order delivery, must not decrease on successfully received acknowledgement
		lastIndexAppended := args.PrevLogIndex + len(args.Entries)
		if lastIndexAppended >= rf.nextIndex[serverId] {
			prev := rf.nextIndex[serverId]
			rf.nextIndex[serverId] = lastIndexAppended + 1
			rf.logConsumer(serverId, "nextIndex for the follower increased from %d to %d", prev, rf.nextIndex[serverId])
		}
		// matchIndex must not decrease, potential out-of-order delivery
		if lastIndexAppended > rf.matchIndex[serverId] {
			prev := rf.matchIndex[serverId]
			rf.matchIndex[serverId] = lastIndexAppended
			rf.logConsumer(serverId, "matchIndex for the follower increased from %d to %d", prev, rf.matchIndex[serverId])
		}
		// Try to commit the entry if majority agreement holds
		// iterate from highest to lowest among newly appended entries
		// Only need to examine newly appended entries because:
		// 1. Entry from the previous term cannot trigger a commit.
		// 2. An opportunity to commit an entry from the current term will never be missed
		// because the entry will be resent to each server until acknowledged
		// (i.e. nextIndex won't increase until accepted and try-committed)
		firstIndexAppended := args.PrevLogIndex + 1
		for i := lastIndexAppended; i >= max(firstIndexAppended, rf.commitIndex+1); i-- {
			committed := rf.tryCommit(lastIndexAppended)
			if committed {
				rf.logConsumer(serverId, "Entries up to index %d committed", lastIndexAppended)
				break
			}
		}
	} else { // entries rejected not because of term outdated
		if rf.nextIndex[serverId] != args.PrevLogIndex+1 {
			// If entry is rejected but the nextIndex already changed (term is not changed), it means either the two cases happened:
			// 1. nextIndex has increased, i.e. has acknowledged the replicate of the entry, no need to retry
			// 2. nextIndex has decreased, i.e. backoff for the entry has happened, no need to retry again
			// in either case: no need to retry
			rf.logConsumer(serverId, "AppendEntries Rejected but no need to retry as leader nextIndex has changed")
			return
		}
		// obtain the next backoff position according to the protocol to accelerate agreement
		if reply.ConflictIndex <= rf.snapshotLastIndex || rf.getLogEntryTerm(reply.ConflictIndex) != reply.ConflictTerm {
			rf.nextIndex[serverId] = reply.ConflictIndex
		} else {
			rf.nextIndex[serverId] = reply.ConflictIndex
			for i := args.PrevLogIndex - 1; i >= reply.ConflictIndex; i-- {
				if reply.ConflictTerm == rf.getLogEntryTerm(i) {
					rf.nextIndex[serverId] = i
					break
				}
			}
		}
		// if the leader has replaced the log entry at backoff position, install the snapshot and increase the backoff position
		// to the first element in the log
		if rf.nextIndex[serverId] <= rf.snapshotLastIndex {
			installSnapshotArgs := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshotLastIndex,
				LastIncludedTerm:  rf.snapshotLastTerm,
				Data:              rf.snapshot,
			}
			rf.logConsumer(serverId, "Try InstallSnapshot after AppendEntries rejected because the potential backoff position %d discarded due to snapshot", rf.nextIndex[serverId])
			// optimistically increment the nextIndex to the first log entry before install snapshot
			// to avoid repeatedly sending the entire snapshot over the network (too expensive)
			rf.nextIndex[serverId] = rf.snapshotLastIndex + 1 // keep optimistic
			go rf.sendAndHandleInstallSnapshot(serverId, installSnapshotArgs)
			return
		}
		//doing the retry: note that it must use the term and leaderCommit from previous AppendEntries
		//because the term might be outdated and only outdated AppendEntries should be sent in such case
		//e.g. the current leader might impersonate the new leader if new term is used
		lastTimeLastIndex := len(args.Entries) + args.PrevLogIndex
		args.PrevLogIndex = rf.getPrevLogIndex(serverId)
		args.PrevLogTerm = rf.getPrevLogTerm(serverId)
		startOffset := rf.indexToLogOffset(rf.nextIndex[serverId])
		endOffset := rf.indexToLogOffset(lastTimeLastIndex + 1)
		args.Entries = rf.log[startOffset:endOffset]
		rf.logConsumer(serverId, "Retry AppendEntries after rejected while decreasing the next index into %d", rf.nextIndex[serverId])
		go rf.sendAndHandleAppendEntries(serverId, args)
	}
}
func (rf *Raft) sendAndHandleInstallSnapshot(serverId int, args *InstallSnapshotArgs) {
	reply := new(InstallSnapshotReply)
	ok := rf.sendInstallSnapshot(serverId, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		rf.logConsumer(serverId, "InstallSnapshot RPC encountered issue")
		return
	}
	rf.compareTermAndUpdateStates(reply.Term)
	if rf.currentTerm > args.Term {
		rf.logConsumer(serverId, "InstallSnapshot response received but term increased, ignore", args.Term)
		return
	}
	// Note: installSnapshot only contains entries that are already committed.
	// Therefore, the commitIndex in the leader will not increase
	rf.matchIndex[serverId] = max(rf.matchIndex[serverId], args.LastIncludedIndex)
	rf.nextIndex[serverId] = max(rf.nextIndex[serverId], 1+args.LastIncludedIndex)
	// the other side has updated the snapshot to be able to receive, re-appendEntries
	appendEntriesArgs := &AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      rf.snapshotLastIndex,
		PrevLogTerm:       rf.snapshotLastTerm,
		Entries:           rf.log,
		LeaderCommitIndex: rf.commitIndex,
	}
	rf.logConsumer(serverId, "Retry AppendEntries with prevLogIndex %d after the installation of snapshot with last index %d",
		rf.snapshotLastIndex, args.LastIncludedIndex)
	go rf.sendAndHandleAppendEntries(serverId, appendEntriesArgs)
}

// The ticker go routine starts a new election if this peer:
//  1. is not a leader
//  2. has timedOut for a randomized period of time without receiving any of the following:
//     1> received AppendEntries from the current leader
//     2> granted vote
//     3> just started an election
//
// The ticker also responsible to periodically broadcast heartbeats if it is a leader
func (rf *Raft) ticker() {
	rf.mu.Lock()
	rf.logServer("Ticker Started")
	rf.mu.Unlock()
	for !rf.killed() {
		rf.mu.Lock()
		if rf.isLeader() && time.Now().After(rf.nextHeartbeatTime) {
			rf.broadcastAppendEntries(true)
		} else if !rf.isLeader() && time.Now().After(rf.nextElectionTime) {
			rf.runForCandidate()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(TickerPeriodMillis) * time.Millisecond)
	}
}

// Observe events involving commits, increment lastApplied and apply to the state machine by sending applyMsg to the service
// Without crash, each entry is applied exactly once!
func (rf *Raft) commitObserver() {
	// Because the loop is not entirely locked, some notification might be missed while not waiting
	// Last resort is to periodically notify the observer to check if apply is available
	go func() {
		rf.logServer("The timer to send notifications for applying command started!")
		for !rf.killed() {
			time.Sleep(time.Duration(TickerPeriodMillis) * time.Millisecond)
			// wrapped in lock to reduce the number of missed notifications
			rf.mu.Lock()
			rf.commitCond.Broadcast()
			rf.mu.Unlock()
		}
	}()
	rf.logServer("The commit observer started!")
	applyQueue := make([]ApplyMsg, 0)
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			// check invariant for debugging
			if rf.commitIndex < rf.lastApplied {
				rf.logServer("ERROR: Invariant is not maintained! commitIndex smaller than lastApplied")
			}
			rf.commitCond.Wait()
		}
		var lastAddedToApplyQueue int
		lastAddedToApplyQueue = rf.lastApplied
		// if the committed index cannot be accessed by indexing the log, deliver the corresponding snapshot
		if rf.lastApplied < rf.snapshotLastIndex {
			applyQueue = append(applyQueue, ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot, // must guarantee that the rf.snapshot has not changed yet
				SnapshotTerm:  rf.snapshotLastTerm,
				SnapshotIndex: rf.snapshotLastIndex,
			})
			lastAddedToApplyQueue = rf.snapshotLastIndex
		}
		for i := lastAddedToApplyQueue + 1; i <= rf.commitIndex; i++ {
			applyQueue = append(applyQueue, ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogEntry(i).Command,
				CommandIndex: i,
			})
		}
		rf.mu.Unlock()
		// to avoid blocking while holding lock, apply outside the lock
		// the queue has entries in following structure:
		// The first entry might or might not be a snapshot entry
		// All the following entry must be a command entry
		for _, msg := range applyQueue {
			rf.applyChannel <- msg
			rf.mu.Lock()
			if msg.SnapshotValid {
				rf.lastApplied = msg.SnapshotIndex
			} else {
				rf.lastApplied++
			}
			rf.logServer("The log entries up to index %d has been applied!", msg.CommandIndex)
			rf.mu.Unlock()
		}
		// Reset the slice length to zero
		applyQueue = applyQueue[:0]
	}
}

// Broadcast AppendEntries Message to all other servers
func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {

	// must update the next broadcast time if it is heartbeat
	if isHeartbeat {
		rf.nextHeartbeatTime = time.Now().Add(time.Duration(AppendEntriesPeriodMillis) * time.Millisecond)
	}

	rf.logServer("BroadCast AppendEntries Messages...")
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			var entries []LogEntry
			// to have better performance, we make both heartbeats or non-heartbeats sending log entries
			if rf.nextIndex[i] <= rf.snapshotLastIndex {
				rf.nextIndex[i] = rf.snapshotLastIndex + 1
			}
			if rf.getLastLogIndex() >= rf.nextIndex[i] {
				entries = rf.log[rf.indexToLogOffset(rf.nextIndex[i]):]
			}
			// The assembly of arguments must be in the same thread to make sure there is no
			// interleaving increase of term. (The leader is the leader of current term not future term)
			args := &AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      rf.getPrevLogIndex(i),
				PrevLogTerm:       rf.getPrevLogTerm(i),
				Entries:           entries,
				LeaderCommitIndex: rf.commitIndex,
			}
			go rf.sendAndHandleAppendEntries(i, args)
		}
	}
}

func (rf *Raft) runForCandidate() {
	// reset the timer for next election
	rf.nextElectionTime = time.Now().Add(generateRandomTimeout())

	// change states and vote for itself
	rf.logServer("Server declared as a CANDIDATE")
	rf.identity = CANDIDATE
	rf.currentTerm += 1
	rf.LeaderId = -1 // set Leader as unknown
	rf.votedFor = rf.me
	rf.persist()
	// define shared states for threads responsible for sending RequestVote and gathering vote
	// These variables can be loaded and modified in each thread using Closure
	// alternative approaches to closure is passing pointer of struct
	votesToWin := rf.requiredVotesToWin()
	votesGathered := 1
	// shared RPC arguments
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	// The potential change to Leader states is designated to the RPC threads handling response from voters
	// Instead of gathering votes in the current thread, i.e. the thread that gathered the half will declare win
	for voterId := range rf.peers {
		if voterId == rf.me {
			continue
		}
		reply := new(RequestVoteReply)
		// Using function closures to avoid passing a large number of variables, set up RPC RequestVote threads
		go func(voterId int) {
			// guaranteed to return, no need to release resources
			receivedReply := rf.sendRequestVote(voterId, args, reply)
			defer rf.mu.Unlock()
			rf.mu.Lock()

			// Scenario 1: no reply: notify the thread to increment failed votes
			if !receivedReply {
				return
			}
			// message received: update term and possibly step down
			rf.compareTermAndUpdateStates(reply.Term)

			// Scenario 2: election is outdated, including two cases
			// case 1: by the time the RPC received the message, election already timeout
			// case 2: received message has term larger than the request
			if rf.currentTerm > args.Term {
				return
			}
			// Scenario 3: received votes
			if reply.VotedGranted {
				rf.logConsumer(voterId, "Received Vote!")
				votesGathered++
				if votesGathered == votesToWin {
					rf.logConsumer(voterId, "Win the election, step up as leader!")
					rf.initializeLeader()

					// inform other threads of stepping up as leader and send heartbeats
					rf.broadcastAppendEntries(true)
				}
				return
			}
			// Scenario 4: received rejects to votes
			rf.logConsumer(voterId, "Received rejection to vote!")
		}(voterId)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. And the first and second
// value will be the leaderId and term that the node knows otherwise start
// the agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isLeader() {
		return rf.LeaderId, rf.currentTerm, false
	}
	index := rf.getLastLogIndex() + 1
	// update log and matchIndex of itself
	rf.log = append(rf.log, LogEntry{Command: command, Index: index, Term: rf.currentTerm})
	rf.persist()
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	// send appendEntries to all other servers, don't restart heartbeat timer for faster update
	rf.broadcastAppendEntries(false)
	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.logServer("The Server is just Killed!")
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		applyChannel:      applyCh,
		currentTerm:       0,
		LeaderId:          -1,
		votedFor:          -1,
		log:               make([]LogEntry, 0), // with snapshot, the log index will start at 1
		commitIndex:       0,
		lastApplied:       0,
		identity:          FOLLOWER,
		nextElectionTime:  time.Now().Add(generateRandomTimeout()), // for the initial election
		nextHeartbeatTime: time.Now(),                              // not important as not started as a leader
		nextIndex:         make([]int, len(peers)),                 // only used by leader, reinitialize then
		matchIndex:        make([]int, len(peers)),                 // only used by leader, reinitialize then
		snapshot:          nil,
		snapshotLastIndex: 0,
		snapshotLastTerm:  0,
	}
	// associate conditional variable
	rf.commitCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	// in principle the next 2 operations should be atomic. But initialization stage doesn't handle RPCs yet
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	go rf.ticker()
	go rf.commitObserver()

	return rf
}

// Utility functions:

// The term of the last log entry
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshotLastTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

// The index of the last log entry
func (rf *Raft) getLastLogIndex() int {
	return rf.snapshotLastIndex + len(rf.log)
}

// The term of the log entry whose existence in a server we are ascertaining
func (rf *Raft) getPrevLogTerm(serverId int) int {
	index := rf.getPrevLogIndex(serverId)
	if index < rf.snapshotLastIndex {
		rf.logServer("Attempted to obtain the term of a log entry that has been replaced by snapshot! -2 returned!")
		return -2 // we don't know the term because it is early part of snapshot
	} else if index == rf.snapshotLastIndex {
		return rf.snapshotLastTerm
	} else {
		return rf.getLogEntryTerm(index)
	}
}

// The index of the log entry whose existence in a server we are ascertaining
func (rf *Raft) getPrevLogIndex(serverId int) int {
	return rf.nextIndex[serverId] - 1
}

// On receiving RPC requests or responses, update the Term if needed
func (rf *Raft) compareTermAndUpdateStates(term int) bool {
	if term > rf.currentTerm {
		rf.logServer("Higher term encountered!")
		rf.currentTerm = term
		rf.LeaderId = -1 // by default the server does not know the ID of new leader
		if rf.isLeader() {
			rf.logServer("Step down from leader!")
		}
		rf.identity = FOLLOWER
		rf.votedFor = -1 // not voted in the Term
		rf.persist()
		return true
	}
	return false
}

// determine if a rf is a leader
func (rf *Raft) isLeader() bool {
	return rf.identity == LEADER
}

// initialize when a server turns into leader
func (rf *Raft) initializeLeader() {
	rf.identity = LEADER
	rf.LeaderId = rf.me
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	// always match to itself
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
}

// On receiving AppendEntries success message, try to see if it is possible to commit the index
// It is guaranteed that newlyAckedIndex is in the log instead of snapshot because snapshot only has committed entries
func (rf *Raft) tryCommit(newlyAckedIndex int) bool {
	if newlyAckedIndex >= rf.getLastLogIndex()+1 {
		log.Fatalf("Server-%d: ERROR: try to commit with index higher than the highest entry in log!", rf.me)
		return false
	}
	// safety requirement: don't commit log of past term until log of current term committed
	if newlyAckedIndex <= rf.commitIndex || rf.currentTerm != rf.getLogEntryTerm(newlyAckedIndex) {
		return false
	}
	votes := 1
	for serverId, matched := range rf.matchIndex {
		// always count itself
		if serverId == rf.me {
			continue
		}
		if matched >= newlyAckedIndex {
			votes++
		}
	}
	if votes >= rf.requiredVotesToWin() {
		// commit if votes enough
		rf.commitIndex = newlyAckedIndex
		rf.logServer("Entries up to index %d committed", newlyAckedIndex)
		rf.commitCond.Broadcast()
		return true
	}
	return false
}

// determining the rf is more updated than some other rf server given its last Term and last Index
func (rf *Raft) isMoreUpdated(lastTerm int, lastIndex int) bool {
	rfIndex := rf.getLastLogIndex()
	rfTerm := rf.getLastLogTerm()
	if rfTerm > lastTerm {
		return true
	}
	if rfTerm == lastTerm && rfIndex > lastIndex {
		return true
	}
	return false
}
func (rf *Raft) requiredVotesToWin() int {
	return (len(rf.peers))/2 + 1
}

// define logging format for nonRPC messages
func (rf *Raft) logServer(format string, args ...interface{}) {
	if !RaftDebug {
		return
	}
	prefix := fmt.Sprintf("TERM-%d Server-%d: ", rf.currentTerm, rf.me)
	message := prefix + format + "\n"
	log.Printf(message, args...)
}

// define logging format for RPC messages where the current server act as the RPC producer (request sender)
func (rf *Raft) logProducer(consumerId int, format string, args ...interface{}) {
	if !RaftDebug {
		return
	}
	prefix := fmt.Sprintf("TERM-%d %d->%d:", rf.currentTerm, consumerId, rf.me)
	message := prefix + format + "\n"
	log.Printf(message, args...)
}

// define logging format for RPC messages where the current server act as the RPC consumer (request receiver)
func (rf *Raft) logConsumer(producerId int, format string, args ...interface{}) {
	if !RaftDebug {
		return
	}
	prefix := fmt.Sprintf("TERM-%d %d->%d: ", rf.currentTerm, rf.me, producerId)
	message := prefix + format + "\n"
	log.Printf(message, args...)
}
func generateRandomTimeout() time.Duration {
	return time.Duration(rand.Int63n(MaxElectionTimeoutMillis-MinElectionTimeoutMillis)+
		MinElectionTimeoutMillis) * time.Millisecond
}

// Given the index of a log entry, get that entry (copy). Fail-fast is used for manifesting errors as early as possible
// Robust error handling is used to prevent fault snowballs quietly
// When only tried to access term, getLogEntryTerm is preferred to incorporate the scenario of last entry of snapshot
func (rf *Raft) getLogEntry(index int) LogEntry {
	logOffset := rf.indexToLogOffset(index)
	if logOffset < 0 {
		debug.PrintStack()
		log.Fatalf("Server-%d: ERROR: try to access entire log entry that has been replaced by snapshot!", rf.me)
	}
	if logOffset >= len(rf.log) {
		debug.PrintStack()
		log.Fatalf("Server-%d: ERROR: Index of log overflows, length of log %d but log offset is %d!",
			rf.me, len(rf.log), logOffset)
	}
	logEntry := rf.log[logOffset]
	if logEntry.Index != index {
		debug.PrintStack()
		log.Fatalf("Server-%d: ERROR: Inconsistency between the log index %d and and the index field of log entry %d!",
			rf.me, index, logEntry.Index)
	}
	return logEntry
}

// Given the index of a log entry, get that term. Fail-fast is used for manifesting errors as early as possible
// The smallest index allowed int this function is rf.snapshotLastIndex
func (rf *Raft) getLogEntryTerm(index int) int {
	logOffset := rf.indexToLogOffset(index)
	if logOffset < -1 {
		debug.PrintStack()
		log.Fatalf("Server-%d: ERROR: try to access term of log entry that has been replaced by snapshot!", rf.me)
	}
	if logOffset >= len(rf.log) {
		debug.PrintStack()
		log.Fatalf("Server-%d: ERROR: Index of log overflows, length of log %d but log offset is %d!",
			rf.me, len(rf.log), logOffset)
	}
	if logOffset == -1 {
		// allowing the access of last index of the snapshot
		return rf.snapshotLastTerm
	}
	if rf.log[logOffset].Index != index {
		debug.PrintStack()
		log.Fatalf("Server-%d: ERROR: Inconsistency between the log index %d and and the index field of log entry %d!",
			rf.me, index, rf.log[logOffset].Index)
	}
	return rf.log[logOffset].Term
}

// Truncate all the entries in the log up to an index, if the index is too large, discard entire log
func (rf *Raft) truncateLogUpTo(lastIndexToDelete int) {
	logOffset := rf.indexToLogOffset(lastIndexToDelete)
	if logOffset >= len(rf.log) {
		rf.log = nil
		rf.logServer("The log has been discarded with last index in snapshot as %d", lastIndexToDelete)
		return
	}
	rf.log = rf.log[logOffset+1:]
	rf.logServer("The log has been truncated up to %d", lastIndexToDelete)
}

// map the index to the log offset (index in the slice)
func (rf *Raft) indexToLogOffset(index int) int {
	return index - (rf.snapshotLastIndex + 1)
}

func max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}
func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}
