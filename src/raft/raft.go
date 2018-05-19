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

type LogEntry struct {
	Term int
	Command interface{}
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
	Log []LogEntry

	CommitIndex int
	LastApplied int

	NextIndex []int
	MatchIndex []int
	State int

	// For the Follower
	ReceivedHB chan int
	ReceivedVT chan int
	UpdatedLD chan int
	VoteCount int

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
	Entries[] LogEntry
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
	if rf.State == Candidate{
		rf.CurrentTerm = args.Term
		rf.ConversionToFollower()
	}
	if (args.Term > rf.CurrentTerm){
		rf.CurrentTerm = args.Term
		rf.ConversionToFollower()
	}
	reply.Success = true
	rf.Log = append(rf.Log, args.Entries...)
	rf.ReceivedHB <- 1

}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Println("Received Vote Request by ", rf.me, args.CandidateID, rf.VoteFor)
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if rf.State == Follower {
		rf.ReceivedVT <- 1
	}
	//fmt.Println("Args this Rf", args.Term, rf.CurrentTerm)
	if args.Term < rf.CurrentTerm{
		reply.VoteGranted = false
		return
	} else if args.Term > rf.CurrentTerm{
		rf.ConversionToFollower()
		rf.CurrentTerm = args.Term
	}
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateID{
		if (len(rf.Log) == 1){
			reply.VoteGranted = true
			rf.VoteFor = args.CandidateID
			return
		}
		if args.LastLogTerm > rf.Log[len(rf.Log) - 1].Term{
			reply.VoteGranted = true
			rf.VoteFor = args.CandidateID
			return
		} else if args.LastLogTerm == rf.Log[len(rf.Log) - 1].Term{
			if args.LastLogIndex >= len(rf.Log){
				rf.VoteFor = args.CandidateID
				reply.VoteGranted = true
				return
			}
		}
	}
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
	var electionTimeoutRand = ElectionTimeOut + time.Duration(rand.Intn(ElectionDuration))
	//fmt.Println("Test", rf.me)
	select {
	case <- rf.ReceivedHB:
	case <- rf.ReceivedVT:
	case <- time.After(electionTimeoutRand * time.Millisecond):
		rf.ConversionToCandidate()
	}
}

func (rf *Raft) ConversionToCandidate(){
	//fmt.Println("*** New Candidate", rf.me)
	rf.mu.Lock();
	defer rf.mu.Unlock()
	rf.State = Candidate
	return
}

func (rf *Raft) ConversionToLeader(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.State = Leader
	return
}

func (rf *Raft) ConversionToFollower(){
	if (rf.State != Follower){
		rf.State = Follower
	}
	rf.VoteFor = -1
}

func (rf *Raft) CandidateRoutine(){
	rf.startElection()
	var electionTimeoutRand = ElectionTimeOut + time.Duration(rand.Intn(ElectionDuration))
	select {
	case <- rf.ReceivedHB:
	case <- rf.UpdatedLD:
	case <- time.After(electionTimeoutRand * time.Millisecond):
	}
}

func (rf *Raft) startElection(){
	//fmt.Println("Current State", rf.State)
	rf.mu.Lock()
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.VoteFor = rf.me
	args := &RequestVoteArgs{}
	args.Term = rf.CurrentTerm
	args.LastLogIndex = len(rf.Log)
	args.CandidateID = rf.me
	args.LastLogTerm = rf.Log[len(rf.Log) - 1].Term
	rf.VoteCount = 0
	rf.mu.Unlock()
	for peerIndex := range rf.peers{
		if peerIndex != rf.me{
			go func(rf *Raft, args *RequestVoteArgs, peerIndex int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(peerIndex, args, reply)
				rf.mu.Lock()
				state := rf.State
				rf.mu.Unlock()
				if state == Candidate{
					if ok == true  && reply.VoteGranted == true{
						rf.mu.Lock()
						if rf.State != Candidate{
							rf.mu.Unlock()
							return
						}
						rf.VoteCount = rf.VoteCount + 1
						// Received from majority of servers
						if (rf.VoteCount + 1)  * 2 > len(rf.peers){
							rf.State = Leader
							rf.UpdatedLD <- 1
						}
						rf.mu.Unlock()
					}
				} else{
					return
				}
			}(rf, args, peerIndex)
		}
	}


}

func (rf *Raft) LeaderRoutine(){
	select {
	case <- rf.ReceivedHB:
	case <- time.After(HeartBeatTimeOut * time.Millisecond):
		rf.sendHeartBeat()
	}
}

func (rf *Raft) sendHeartBeat(){
	args := &AppendEntriesArgs{}
	rf.mu.Lock()
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	rf.mu.Unlock()
	for peerIndex := range rf.peers {
		if peerIndex != rf.me{
			go func(rf *Raft, args *AppendEntriesArgs, peerIndex int) {
				reply := &AppendEntriesReply{}
				rf.mu.Lock()
				state := rf.State
				rf.mu.Unlock()
				if state != Leader{
					return
				}
				ok := rf.sendAppendEntries(peerIndex, args, reply)
				//fmt.Println("Sending Appended Entries to: ", peerIndex, rf.me )
				if ok == true{
					rf.mu.Lock()
					if reply.Success == false && reply.Term > rf.CurrentTerm{
						rf.CurrentTerm = reply.Term
						rf.ConversionToFollower()
					}
					rf.mu.Unlock()
				}
				//fmt.Println("Finish Sending Appended Entries to: ", peerIndex, rf.me, ok)
			}(rf, args, peerIndex)
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
	rand.Seed(time.Now().UnixNano() + int64(me))
	rf.me = me
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.State = Follower
	rf.ReceivedHB = make(chan int)
	rf.ReceivedVT = make(chan int)
	rf.UpdatedLD = make(chan int)
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Log = make([]LogEntry, 0)
	rf.Log = append(rf.Log, LogEntry{-1, 0})
	rf.VoteCount = 0
	go rf.MainRoutine()
	return rf
}
