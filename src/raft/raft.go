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

const (
	MIN_ELECTION_TIMEOUT_MILLIS  int64 = 300
	MAX_ELECTION_TIMEOUT_MILLIS  int64 = 1000
	APPEND_ENTRIES_PERIOD_MILLIS int64 = 50
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
	applyChannel chan ApplyMsg
	currentTerm  int
	votedFor     int // -1 represents not voted yet
	log          []LogEntry
	commitIndex  int // serves as the right boundary of a slicing window
	lastApplied  int // serves as the left boundary of a slicing window
	identity     ServerIdentity
	// TODO: create a atomic time class using its own lock
	prevResetTimeoutTime time.Time
	resetChannel         chan int // channel to inform the reset of election timeout time
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
	defer rf.mu.Unlock()
	rf.mu.Lock()
	rf.compareTermAndUpdate(args.AppendEntriesInfo.Term)

	// update reply entries
	reply.Term = rf.currentTerm
	reply.EntriesAccepted = false
	if args.AppendEntriesInfo.Term == rf.currentTerm {
		rf.resetChannel <- 1
	}
}

// RequestVote RPC handler.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	defer rf.mu.Unlock()
	rf.mu.Lock()
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
	if (rf.votedFor == -1 || rf.votedFor == args.RequestVoteInfo.CandidateId) &&
		!rf.isMoreUpdated(args.RequestVoteInfo.LastLogTerm, args.RequestVoteInfo.LastLogIndex) {
		reply.VotedGranted = true
		rf.resetChannel <- 1
	}
	return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
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
	rf.compareTermAndUpdate(reply.Term)
	rf.mu.Unlock()
	// more operations (2B)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		go rf.electionTimeout()
		_, ok := <-rf.resetChannel
		if !ok { // the channel is closed as the server is dead
			break
		}
		rf.mu.Lock()
		rf.prevResetTimeoutTime = time.Now()
		rf.mu.Unlock()
	}
}
func (rf *Raft) electionTimeout() {
	// randomize election timeout time
	var timeout time.Duration
	timeout = time.Duration(rand.Int63n(MAX_ELECTION_TIMEOUT_MILLIS-MIN_ELECTION_TIMEOUT_MILLIS)+
		MIN_ELECTION_TIMEOUT_MILLIS) * time.Millisecond
	// timer start
	time.Sleep(timeout)

	// wake up and check if timeout
	rf.mu.Lock()
	electionTime := rf.prevResetTimeoutTime.Add(timeout)
	rf.mu.Unlock()
	now := time.Now()
	if now.After(electionTime) || now.Equal(electionTime) {
		// timeout: requests for votes, restart the timeout ticker
		go rf.runForCandidate()
		// new election will trigger a reset
		rf.resetChannel <- 1
	}
}

// The leaderTicker go routine periodically sends to other servers the appendEntries request
func (rf *Raft) leaderTicker() {
	for !rf.killed() && rf.isLeader() {
		message := &AppendEntriesMsg{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			PrevLogIndex:      rf.getPrevLogIndex(),
			PrevLogTerm:       rf.getPrevLogTerm(),
			Entries:           nil, // to be updated
			LeaderCommitIndex: rf.commitIndex,
		}
		args := &AppendEntriesArgs{message}
		for i := 0; i < len(rf.peers); i++ {
			if rf.me != i {
				go rf.sendAndHandleAppendEntries(i, args)
			}
		}
		period := time.Duration(APPEND_ENTRIES_PERIOD_MILLIS) * time.Millisecond
		time.Sleep(period)
	}
}

// TODO: abstract out the case where Term is updated by RPC request/response
// TODO: what if the election aborted by receiving appendEntries? Not handled yet
func (rf *Raft) runForCandidate() {
	// change states to itself and vote for itself
	rf.mu.Lock()
	rf.identity = CANDIDATE
	rf.currentTerm += 1
	// record the Term of the candidacy
	candidacyTerm := rf.currentTerm
	rf.votedFor = rf.me
	// define requestVote message
	requestVoteMsg := &RequestVoteMsg{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getPrevLogIndex(),
		LastLogTerm:  rf.getPrevLogTerm(),
	}
	rf.mu.Unlock()
	votesToWin := rf.requiredVotesToWin()
	failuresToLose := rf.unsuccessfulVotesToLose()
	votesGathered := 1
	failuresGathered := 0
	termLearned := candidacyTerm
	requestVoteArgs := &RequestVoteArgs{requestVoteMsg}
	// Using conditional variable to synchronize vote gathering threads and vote counter thread (current thread)
	// Producers : each thread that listens the reply of one voter and gather vote or update Term
	// Consumer : current candidate thread
	// Shared variables : votesGathered, failuresToLost, termLearned
	// we need to count both votes and failures to make sure the function will not wait forever
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	for i := 0; i < len(rf.peers); i++ {
		voterId := i
		if voterId != rf.me {
			requestVoteReply := &RequestVoteReply{}
			go func(voterId int) {
				// guaranteed to return, no need to release resources
				receivedReply := rf.sendRequestVote(voterId, requestVoteArgs, requestVoteReply)
				if !receivedReply {
					mu.Lock()
					failuresToLose++
					cond.Broadcast()
					mu.Unlock()
					return
				}
				// if rf.currentTerm is updated by interleaving other RPC requests/reply
				rf.mu.Lock()
				rf.compareTermAndUpdate(requestVoteReply.Term)
				rf.mu.Unlock()
				// TODO: replace the isHigherTermEncountered += the previous result and do the test
				isHigherTermEncountered := requestVoteReply.Term > candidacyTerm

				// higher Term found : no need to check voteGranted
				if isHigherTermEncountered {
					mu.Lock()
					termLearned = requestVoteReply.Term
					cond.Broadcast()
					mu.Unlock()
					return
				}
				if requestVoteReply.VotedGranted {
					mu.Lock()
					votesGathered++
					cond.Broadcast()
					mu.Unlock()
					return
				} else {
					mu.Lock()
					failuresGathered++
					cond.Broadcast()
					mu.Unlock()
					return
				}

			}(voterId)
		}
	}
	mu.Lock()
	// TODO: what if producer finished produce before the consumer ever entered into the for loop? will be safety issue?
	// If rf.currentTerm will change by receiving RPCs other than RequestVote reply message with Term larger than current Term,
	// Then it is not possible for the candidate to gather enough votes in the current Term
	// Therefore, the candidate will lose silently with no state change
	// So, we don't need to handle those scenarios here but make sure failure goes silently
	for !(termLearned > candidacyTerm || votesGathered >= votesToWin || failuresGathered >= failuresToLose) {
		cond.Wait()
	}
	// scenario 1: termination of election
	switch true {
	case termLearned > candidacyTerm:
		mu.Unlock()
		return
	case votesGathered >= votesToWin:
		// scenario 2: win
		mu.Unlock()
		rf.mu.Lock()
		rf.identity = LEADER
		go rf.leaderTicker() // immediately send the appendEntries and periodically do so
		rf.mu.Unlock()
		return
	default:
		// scenario 3: lose, simply return
		mu.Unlock()
		return
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
	// close the reset channel to end election ticks
	close(rf.resetChannel)
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
	rf.resetChannel = make(chan int)
	// TODO : reconcile the initialization from nothing (above written by me) and from crash (below given)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.prevResetTimeoutTime = time.Now()
	go rf.electionTicker()

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
		rf.currentTerm = term
		rf.identity = FOLLOWER
		rf.votedFor = -1 // not voted in the Term
		return true
	}
	return false
}

// Atomic operation to determine if a rf is a leader
// no lock required
func (rf *Raft) isLeader() bool {
	return atomic.LoadInt32((*int32)(&rf.identity)) == int32(LEADER)
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
func (rf *Raft) unsuccessfulVotesToLose() int {
	return (len(rf.peers) + 1) / 2
}
