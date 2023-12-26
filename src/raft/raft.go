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
	"regexp"
	"runtime"
	"strconv"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

type Lock struct {
	mu    sync.Mutex
	owner int64
}

func (l *Lock) Lock() {
	l.mu.Lock()
	l.owner = getGoroutineID()
}
func (l *Lock) Unlock() {
	l.owner = 0
	l.mu.Unlock()
}
func (l *Lock) Owner() int64 {
	return l.owner
}
func getGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := string(buf[:n])
	re := regexp.MustCompile(`\d+`)
	if id := re.FindString(idField); id != "" {
		idInt, _ := strconv.ParseInt(id, 10, 64)
		return idInt
	}
	return 0
}

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
)
const (
	LEADER ServerIdentity = iota
	CANDIDATE
	FOLLOWER
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu           Lock                // Lock to protect shared access to this peer's state
	stepUpCond   *sync.Cond          // Used to synchronize the thread sending heartbeats with the threads that appreciate to leader
	electionCond *sync.Cond          // Used to synchronize the thread holding an election with the threads that control conditions
	timedOut     bool                // used by sleeping thread after waking up to make sure there is no reset of timer
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's Index into peers[]
	dead         int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyChannel         chan ApplyMsg
	currentTerm          int
	votedFor             int // -1 represents not voted yet
	log                  []LogEntry
	commitIndex          int // serves as the right boundary of a slicing window
	lastApplied          int // serves as the left boundary of a slicing window
	identity             ServerIdentity
	prevResetTimeoutTime time.Time
}

// A single entry in log
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int // The Term at which the entry is appended to the log
}

// The message from leader to other nodes acts as:
// 1. a heartbeat that reset the election timeout
// 2. carries the entries to append
// 3. carries the Index before and through which logs to commit
type AppendEntriesMsg struct {
	Term              int
	LeaderId          int // used for redirection
	PrevLogIndex      int // used to decide whether previous entries match and to reject entries
	PrevLogTerm       int // used to decide whether previous entries match and to reject entries
	Entries           []LogEntry
	LeaderCommitIndex int // used to inform follower to commit
}

// The message from candidate to other nodes to request votes
type RequestVoteMsg struct {
	Term         int
	CandidateId  int
	LastLogIndex int // used for voters to decide whose is more update-to-date and grant vote
	LastLogTerm  int // used for voters to decide whose is more update-to-date and grant vote
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.identity == LEADER
	//rf.mu.Unlock()
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

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	RequestVoteInfo *RequestVoteMsg
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	AppendEntriesInfo *AppendEntriesMsg
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term            int
	EntriesAccepted bool
}

// AppendEntriees RPC handler
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.logProducer(args.AppendEntriesInfo.LeaderId,
		"Received AppendEntries in leader term %d",
		args.AppendEntriesInfo.Term)
	rf.compareTermAndUpdate(args.AppendEntriesInfo.Term)
	// update reply entries (2B)
	reply.Term = rf.currentTerm
	reply.EntriesAccepted = false
	// reset election timeout if appendEntries are from leader of current term
	if args.AppendEntriesInfo.Term == rf.currentTerm {
		rf.prevResetTimeoutTime = time.Now()
	}
	rf.mu.Unlock()
	return
}

// RequestVote RPC handler.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.logProducer(args.RequestVoteInfo.CandidateId,
		"Received RequestVote in candidate term %d",
		args.RequestVoteInfo.Term)

	// update the Term on receiving RPC message
	rf.compareTermAndUpdate(args.RequestVoteInfo.Term)

	// update reply values
	reply.Term = rf.currentTerm
	reply.VotedGranted = false
	// reject if higher Term
	if args.RequestVoteInfo.Term < rf.currentTerm {
		return
	}
	// TODO: potential issue: count of duplicate votes
	// accept if not voted yet (or duplicate reply received) and
	if rf.decideGrantVote(args.RequestVoteInfo.CandidateId,
		args.RequestVoteInfo.LastLogTerm,
		args.RequestVoteInfo.LastLogIndex) {
		rf.logProducer(args.RequestVoteInfo.CandidateId, "Server grant vote!")
		reply.VotedGranted = true
		rf.votedFor = args.RequestVoteInfo.CandidateId
		rf.prevResetTimeoutTime = time.Now()
	}
	rf.mu.Unlock()
}

func (rf *Raft) decideGrantVote(candidateId int, candidateLastLogTerm int, candidateLastLogIndex int) bool {
	return (rf.votedFor == -1 || rf.votedFor == candidateId) &&
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
	if !ok {
		return
	}
	rf.mu.Lock()
	rf.logConsumer(serverId, "Received reply of AppendEntries")
	rf.compareTermAndUpdate(reply.Term)
	rf.mu.Unlock()
	// more operations (2B)
}

// The ticker go routine starts a new election if this peer:
//  1. is not a leader
//  2. has timedOut for a randomized period of time without receiving any of the following:
//     1> received AppendEntries from the current leader
//     2> granted vote
func (rf *Raft) electionTicker() {
	rf.mu.Lock()
	rf.logServer("Election Ticker Started")
	rf.mu.Unlock()
	for !rf.killed() {
		rf.logServer("A new round of election ticking as a " + rf.identity.String())
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.logServer("BLOCK: Waiting for lock, owner ID: %d", rf.mu.Owner())
		rf.mu.Lock()
		rf.logServer("UNBLOCK: Lock Acquired")
		// We will not set up a timeout time for a leader
		for !(!rf.isLeader() && rf.timedOut) {
			rf.logServer("BLOCK: Election timer put to sleep: %d", rf.mu.Owner())
			rf.electionCond.Wait()
			rf.logServer("UNBLOCK: Wake up from the waiting for step down as leader")
		}
		// No reset happened to prevent an election: just set current as reset time and reset timedOut as false
		rf.prevResetTimeoutTime = time.Now()
		rf.timedOut = false
		go rf.electionTimeout()
		// Create threads each handling a RequestVote RPC
		rf.runForCandidate()
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	rf.logServer("Election ticker is Terminated!")
	rf.mu.Unlock()
}

// This thread sleep for a random time and wake up to check if there is an interleaving reset happened
// If such reset happened, the thread does nothing. Otherwise, set the variable rf.timedOut to be true
// and notify the election-ticker thread
func (rf *Raft) electionTimeout() {
	// randomize election timeout time
	var timeout time.Duration
	timeout = time.Duration(rand.Int63n(MAX_ELECTION_TIMEOUT_MILLIS-MIN_ELECTION_TIMEOUT_MILLIS)+
		MIN_ELECTION_TIMEOUT_MILLIS) * time.Millisecond
	// timer start
	rf.mu.Lock()
	rf.logServer("An timeout timer started for " + timeout.String())
	rf.mu.Unlock()
	time.Sleep(timeout)

	// wake up and check if timeout
	defer rf.mu.Unlock()
	rf.mu.Lock()
	electionTime := rf.prevResetTimeoutTime.Add(timeout)
	now := time.Now()
	if now.After(electionTime) || now.Equal(electionTime) {
		// Within the same lock creating threads each to request a vote and update the candidate accordingly
		rf.logServer("Election triggered after a timeout for" + timeout.String())
		// new election will trigger a reset
		rf.timedOut = true
		rf.electionCond.Broadcast()
	}
}

// The leaderTicker go routine periodically sends to other servers the appendEntries request
func (rf *Raft) leaderTicker() {
	rf.mu.Lock()
	rf.logServer("Leader Ticker started!")
	rf.mu.Unlock()

	for !rf.killed() {
		rf.logServer("A new round of leader ticking")
		rf.mu.Lock()
		for !rf.isLeader() {
			rf.stepUpCond.Wait()
		}

		message := &AppendEntriesMsg{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			PrevLogIndex:      rf.getPrevLogIndex(),
			PrevLogTerm:       rf.getPrevLogTerm(),
			Entries:           nil, // to be updated
			LeaderCommitIndex: rf.commitIndex,
		}
		rf.mu.Unlock()
		args := &AppendEntriesArgs{message}
		for i := 0; i < len(rf.peers); i++ {
			if rf.me != i {
				go rf.sendAndHandleAppendEntries(i, args)
			}
		}
		period := time.Duration(APPEND_ENTRIES_PERIOD_MILLIS) * time.Millisecond
		time.Sleep(period)
	}
	rf.mu.Lock()
	rf.logServer("Leader Ticker is Terminated")
	rf.mu.Unlock()
}

func (rf *Raft) runForCandidate() {
	// change states to itself and vote for itself
	rf.identity = CANDIDATE
	rf.currentTerm += 1
	rf.logServer("Server declared as a CANDIDATE")
	rf.votedFor = rf.me
	// define requestVote message
	requestVoteMsg := &RequestVoteMsg{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getPrevLogIndex(),
		LastLogTerm:  rf.getPrevLogTerm(),
	}
	votesToWin := rf.requiredVotesToWin()
	votesGathered := 1
	// shared arguments
	args := &RequestVoteArgs{requestVoteMsg}
	// The potential change to Leader states is designated to the RPC threads handling response from voters
	// Instead of gathering votes in the current thread
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
			rf.compareTermAndUpdate(reply.Term)

			// Scenario 2: election is outdated, including two cases
			// case 1: by the time the RPC received the message, election already timeout
			// case 2: received message has term larger than the request
			if rf.currentTerm > args.RequestVoteInfo.Term {
				return
			}
			// Scenario 3: received votes
			if reply.VotedGranted {
				rf.logConsumer(voterId, "Received Vote!")
				votesGathered++
				if votesGathered == votesToWin {
					rf.logConsumer(voterId, "Win the election, step up as leader!")
					rf.identity = LEADER
					// inform other threads of stepping up as leader and send heartbeats
					rf.stepUpCond.Broadcast()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	rf.logServer("Kill the server")
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.applyChannel = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // Add the placeholder sentinel entry
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.identity = FOLLOWER
	rf.stepUpCond = sync.NewCond(&rf.mu.mu)
	rf.electionCond = sync.NewCond(&rf.mu.mu)
	// TODO : reconcile the initialization from nothing (above written by me) and from crash (below given)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.timedOut = true
	rf.prevResetTimeoutTime = time.Now()
	go rf.electionTicker()
	go rf.leaderTicker()

	return rf
}

// Utility functions:

func (rf *Raft) getPrevLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}
func (rf *Raft) getPrevLogIndex() int {
	return len(rf.log) - 1
}

// On receiving RPC requests or responses, update the Term if needed
func (rf *Raft) compareTermAndUpdate(term int) bool {
	if term > rf.currentTerm {
		rf.logServer("Higher term encountered!")
		rf.currentTerm = term
		if rf.isLeader() {
			rf.logServer("Step down from leader!")
			rf.electionCond.Broadcast()
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
