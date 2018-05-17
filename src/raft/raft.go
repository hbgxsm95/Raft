package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

var (
	ElectionTimeOut  = time.Duration(200)
	HeartBeatTimeOut    = time.Duration(160)
	ElectionDuration = 200
)

const (
	Follower = iota
	Candidate
	Leader

)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	CurrentTerm int
	VoteFor int
	// Remember that the index starts with 1
	Log []int

	CommitIndex int
	LastApplied int

	NextIndex []int
	MatchIndex []int
	State int

	// For the Follower
	ReceivedHB chan int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.State == Leader
	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries[] int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if (rf.CurrentTerm > args.Term){
		reply.Success = false
		return
	}
	if rf.State == Follower {
		rf.ReceivedHB <- 1
	} else if rf.State == Candidate{
		rf.State = Follower
	}
	if (args.Term > rf.CurrentTerm){
		rf.CurrentTerm = args.Term
		rf.State = Follower
	} else{
		rf.Log = append(rf.Log, args.Entries...)
	}
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State == Follower {
		rf.ReceivedHB <- 1
	}
	if args.Term < rf.CurrentTerm{
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.CurrentTerm{
		rf.State = Follower
		rf.CurrentTerm = args.Term
	}
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateID{
		if args.LastLogTerm > rf.Log[len(rf.Log) - 1]{
			reply.Term = rf.Log[len(rf.Log) - 1]
			reply.VoteGranted = true
			return
		} else if args.LastLogTerm == rf.Log[len(rf.Log) - 1]{
			if args.LastLogIndex >= len(rf.Log){
				reply.Term = rf.Log[len(rf.Log) - 1]
				reply.VoteGranted = true
				return
			}
		}
	}
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
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
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) MainRoutine(){
	for{
		rf.mu.Lock()
		currentState := rf.State
		rf.mu.Unlock()
		switch currentState {
		case Follower:
			rf.FollowerRoutine()
		case Candidate:
			rf.CandidateRoutine()
		case Leader:
			rf.LeaderRoutine()
		}
	}
}

func (rf *Raft) FollowerRoutine(){
	var heartBeatTimeoutRand = HeartBeatTimeOut + time.Duration(rand.Intn(ElectionDuration))
	select {
	case <- time.After(heartBeatTimeoutRand):
		rf.ConversionToCandidate()
	case <- rf.ReceivedHB:
	}
}

func (rf *Raft) ConversionToCandidate(){
	rf.mu.Lock();
	defer rf.mu.Unlock()
	rf.State = Candidate
}

func (rf *Raft) CandidateRoutine(){
	go rf.startElection()
	var electionTimeoutRand = ElectionTimeOut + time.Duration(rand.Intn(ElectionDuration))
	select {
	case <- time.After(electionTimeoutRand):
	}
}

func (rf *Raft) startElection(){
	rf.mu.Lock()
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.VoteFor = rf.me
	args := &RequestVoteArgs{}
	args.Term = rf.CurrentTerm
	args.LastLogIndex = len(rf.Log)
	args.CandidateID = rf.me
	if len(rf.Log) != 0{
		args.LastLogTerm = rf.Log[len(rf.Log) - 1]
	}
	rf.mu.Unlock()
	VoteCount := 0

	for peerIndex := range rf.peers{
		if peerIndex != rf.me{
			reply := &RequestVoteReply{}
			args.Term = rf.CurrentTerm
			ok := rf.sendRequestVote(peerIndex, args, reply)
			if ok == true{
				VoteCount = VoteCount + 1
			}
		}
	}
	// Received from majority of servers
	if VoteCount > (len(rf.peers) - 1) / 2{
		rf.mu.Lock()
		rf.State = Leader
		rf.mu.Unlock()
	}

}

func (rf *Raft) LeaderRoutine(){
	var heartBeatTimeoutRand = HeartBeatTimeOut + time.Duration(rand.Intn(ElectionDuration))
	select {
	case <- time.After(heartBeatTimeoutRand):
		go rf.sendHeartBeat()
	}
}

func (rf *Raft) sendHeartBeat(){
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	for peerIndex := range rf.peers {
		if peerIndex != rf.me{
			rf.sendAppendEntries(peerIndex, args, reply)
		}
	}
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.State = Follower
	rf.ReceivedHB = make(chan int)
	go rf.MainRoutine()
	return rf
}
