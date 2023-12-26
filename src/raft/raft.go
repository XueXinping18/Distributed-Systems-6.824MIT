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
	"fmt"
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type ServerIdentity int32

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
	MIN_ELECTION_TIMEOUT_MILLIS  int64 = 500
	MAX_ELECTION_TIMEOUT_MILLIS  int64 = 1000
	APPEND_ENTRIES_PERIOD_MILLIS int64 = 100
	TICKER_PERIOD_MILLIS         int64 = 30
)
const (
	LEADER ServerIdentity = iota
	CANDIDATE
	FOLLOWER
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyChannel      chan ApplyMsg
	currentTerm       int
	votedFor          int // -1 represents not voted yet
	log               []LogEntry
	commitIndex       int // serves as the right boundary of a slicing window
	lastApplied       int // serves as the left boundary of a slicing window
	identity          ServerIdentity
	nextElectionTime  time.Time // The next time to initiate an election, can be reset (postponed)
	nextHeartbeatTime time.Time // The next time to broadcast AppendEntries

	// volatile states for leader
	nextIndex  []int //for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server

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
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.identity == LEADER
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	// Your data here (2A).
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
}

// AppendEntriees RPC handler
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.logProducer(args.LeaderId,
		"Received AppendEntries in leader term %d",
		args.Term)

	// update the Term and identity if higher term encountered on receiving RPC message
	rf.compareTermAndUpdateStates(args.Term)

	// reset election timeout if appendEntries are from leader of current term
	if args.Term == rf.currentTerm {
		rf.nextElectionTime = time.Now().Add(generateRandomTimeout())
		if rf.identity == CANDIDATE {
			// give up the election if someone else declared winning
			rf.identity = FOLLOWER
		}
	}
	// update reply entries (2B)
	reply.Term = rf.currentTerm
	reply.EntriesAccepted = false

	rf.mu.Unlock()
	return
}

// RequestVote RPC handler.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.logProducer(args.CandidateId,
		"Received RequestVote in candidate term %d",
		args.Term)

	// update the Term and identity if higher term encountered on receiving RPC message
	rf.compareTermAndUpdateStates(args.Term)

	// update reply values by not granting by default
	reply.Term = rf.currentTerm
	reply.VotedGranted = false

	// TODO: potential issue: count of duplicate votes
	// decide if grant vote
	if rf.shouldGrantVote(args.CandidateId,
		args.Term,
		args.LastLogTerm,
		args.LastLogIndex) {
		rf.logProducer(args.CandidateId, "Server grant vote!")
		// grant the vote
		reply.VotedGranted = true
		rf.votedFor = args.CandidateId

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
	if !ok {
		rf.mu.Lock()
		rf.logConsumer(serverId, "AppendEntries RPC encountered issue")
		rf.mu.Unlock()
	}
	return ok
}

// send AppendEntries message and handling the response
func (rf *Raft) sendAndHandleAppendEntries(serverId int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, args, reply)
	// response not received
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logConsumer(serverId, "Received reply of AppendEntries")
	rf.compareTermAndUpdateStates(reply.Term)
	// more operations (2B)
	// if the AppendEntries are rejected because outdated, don't retry just end
	if rf.currentTerm > args.Term {
		rf.logConsumer(serverId, "AppendEntries Rejected because the term of the leader server itself was only %d", args.Term)
		return
	}

	if reply.EntriesAccepted {
		// update nextIndex and matchIndex,
		// potential out-of-order delivery, must not decrease on successfully received acknowledgement
		lastIndexAppended := args.Entries[len(args.Entries)-1].Index
		if lastIndexAppended >= rf.nextIndex[serverId] {
			rf.nextIndex[serverId] = lastIndexAppended + 1
		}
		// matchIndex must not decrease, potential out-of-order delivery
		if lastIndexAppended >= rf.matchIndex[serverId] {
			rf.matchIndex[serverId] = lastIndexAppended
		}
		committed := rf.tryCommit(lastIndexAppended)
		if committed {
			rf.logConsumer(serverId, "Entries up to index %d commited", lastIndexAppended)
		}
	} else {
		// rejected not because term outdated:
		if rf.nextIndex[serverId] != args.PrevLogIndex+1 {
			// If entry is rejected but the nextIndex already changed (term is not changed), it means either the two cases happened:
			// 1. nextIndex has increased, i.e. has acknowledged the replicate of the entry, no need to retry
			// 2. nextIndex has decreased, i.e. backoff for the entry has happened, no need to retry
			// in either case: no need to retry
			return
		}
		//doing the retry: note that it must use the term and leaderCommit from previous AppendEntries
		//because the term might be outdated and only outdated AppendEntries should be sent in such case
		//e.g. the current leader might impersonate the new leader if new term is used
		args.PrevLogIndex = rf.getPrevLogIndex(serverId)
		args.PrevLogTerm = rf.getPrevLogTerm(serverId)
		args.Entries = append([]LogEntry{rf.log[rf.nextIndex[serverId]]}, args.Entries...)
		rf.logConsumer(serverId, "AppendEntries Rejected and Retry after decrement the next index into %d", rf.nextIndex[serverId])
		go rf.sendAndHandleAppendEntries(serverId, args)
	}
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
		time.Sleep(time.Duration(TICKER_PERIOD_MILLIS) * time.Millisecond)
	}
}

// Broadcast AppendEntries Message to all other servers
func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {

	// must update the next broadcast time if it is heartbeat
	if isHeartbeat {
		rf.nextHeartbeatTime = time.Now().Add(time.Duration(APPEND_ENTRIES_PERIOD_MILLIS) * time.Millisecond)
	}

	rf.logServer("BroadCast AppendEntries Messages...")
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			var entries []LogEntry
			// to have better performance, we make heartbeat sending log entries
			if rf.getLastLogIndex() >= rf.nextIndex[i] {
				entries = rf.log[rf.nextIndex[i]:]
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
	rf.votedFor = rf.me
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
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isLeader() {
		return -1, -1, false
	}
	index := len(rf.log)
	// update log and matchIndex of itself
	rf.log = append(rf.log, LogEntry{Command: command, Index: index, Term: rf.currentTerm})
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
		votedFor:          -1,
		log:               make([]LogEntry, 1), // include a sentinel logEntry
		commitIndex:       0,
		lastApplied:       0,
		identity:          FOLLOWER,
		nextElectionTime:  time.Now().Add(generateRandomTimeout()), // for the initial election
		nextHeartbeatTime: time.Now(),                              // not important as not started as a leader
		nextIndex:         make([]int, len(peers)),                 // only used by leader, reinitialize then
		matchIndex:        make([]int, len(peers)),                 // only used by leader, reinitialize then
	}
	// TODO : reconcile the initialization from nothing (above written by me) and from crash (below given)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	return rf
}

// Utility functions:

// The term of the last log entry
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// The index of the last log entry
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// The term of the log entry whose existence in a server we are ascertaining
func (rf *Raft) getPrevLogTerm(serverId int) int {
	return rf.log[rf.nextIndex[serverId]-1].Term
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
		if rf.isLeader() {
			rf.logServer("Step down from leader!")
		}
		rf.identity = FOLLOWER
		rf.votedFor = -1 // not voted in the Term
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
func (rf *Raft) tryCommit(newlyAckedIndex int) bool {
	// safety requirement: don't commit log of past term until log of current term committed
	if newlyAckedIndex <= rf.commitIndex || rf.currentTerm != rf.log[newlyAckedIndex].Term {
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
		return true
	}
	return false
}

// judging the rf is more updated than some other rf server given its last Term and last Index
func (rf *Raft) isMoreUpdated(lastTerm int, lastIndex int) bool {
	rfIndex := len(rf.log) - 1
	rfTerm := rf.log[rfIndex].Term
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

func (rf *Raft) logServer(format string, args ...interface{}) {
	prefix := fmt.Sprintf("TERM-%d Server-%d: ", rf.currentTerm, rf.me)
	message := prefix + format + "\n"
	log.Printf(message, args...)
}
func (rf *Raft) logProducer(consumerId int, format string, args ...interface{}) {
	prefix := fmt.Sprintf("TERM-%d %d->%d:", rf.currentTerm, consumerId, rf.me)
	message := prefix + format + "\n"
	log.Printf(message, args...)
}
func (rf *Raft) logConsumer(producerId int, format string, args ...interface{}) {
	prefix := fmt.Sprintf("TERM-%d %d->%d: ", rf.currentTerm, rf.me, producerId)
	message := prefix + format + "\n"
	log.Printf(message, args...)
}
func generateRandomTimeout() time.Duration {
	return time.Duration(rand.Int63n(MAX_ELECTION_TIMEOUT_MILLIS-MIN_ELECTION_TIMEOUT_MILLIS)+
		MIN_ELECTION_TIMEOUT_MILLIS) * time.Millisecond
}
