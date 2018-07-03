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
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"



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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//node information, important
	nodeState 	string		//only 3 states exists: "Follower", "Candidate", "Leader"
	term 		int	
	voteFor		int
	logs		[]LogEntry

	//time related
	resetTimer	chan struct{}
	electionTimer	*time.Timer
	electionTimeout	time.Duration
	HBInterval 	time.Duration	//HeartBeat Interval
	
	//not used in Task2A
	commitIndex	int
	lastApplied	int
	nextIndex	[]int
	matchIndex	[]int
}


// The following structs are derived from Raft paper:https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf
// type logEntry added in Task2A
type LogEntry struct{
	term		int
	index		int
	command 	interface{}
}




// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = (rf.nodeState=="Leader")

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	candidateId	int
	term		int
	lastLogIndex	int
	lastLogTerm	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term		int
	voteGranted	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) fillRequestVoteArgs(args *RequestVoteArgs) {
	// Your code here (2A, 2B).
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

// Task2A: AppendEntries
type AppendEntriesArgs struct{
	term		int
	leaderId	int
	prevLogIndex	int
	prevLogTerm	int
	entries		[]LogEntry
}

// Task2A: AppendEntries
type AppendEntriesReply struct{
	term		int
	success		bool
}


// Task2A: AppendEntries RPC handler

func (rf *Raft) fillAppendEntriesArgs(args *AppendEntriesArgs){
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	// Your code here (2A, 2B).	
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



//Task2A: electionDeamon
func (rf *Raft) electionDaemon(){
	select{
		//each time receive heartbeat, timer is reset
		case <-rf.resetTimer:			
			rf.electionTimer.Reset(rf.electionTimeout)
		//once there is a period of time not receving heartbeat, than hold voting for the new leader 
		case <-rf.electionTimer.C:		 
			rf.electionTimer.Reset(rf.electionTimeout)
			rf.nodeState = "Candidate"		
			go rf.canvassVotes()	//need multithread
	}
}

//Task2A: canvassVotes
func (rf *Raft) canvassVotes(){
	var voteArgs RequestVoteArgs
	rf.fillRequestVoteArgs(&voteArgs)	//TODO: write this functionlater

	//request vote and get replies
	var wg sync.WaitGroup
	
	replies :=make(chan RequestVoteReply)
	votes:=0

	for i:=0; i<len(rf.peers); i++{
		//"Candidate" vote for himself
		if i==rf.me && rf.nodeState=="Candidate"{
			rf.resetTimer<- struct{}{}		
			votes++
		}else{//send request to others
			var reply RequestVoteReply
			wg.Add(1)
			go func(n int){
				if rf.sendRequestVote(i, &voteArgs, &reply){//TODO: request/reply may fail. use channel to replace for loop
					replies<- reply
				} 		
				wg.Done()
			}(i)
		}	
	}	

	go func(){
		//wait until getting replies from all nodes
		//node may fail. This is the reason why using multithread	
		wg.Wait()
		close(replies)
	}()	


	for reply :=range(replies){
		if reply.voteGranted{
			votes++
			if votes > len(rf.peers)/2{
				rf.mu.Lock()
				rf.nodeState = "Leader"
				rf.mu.Unlock()

				//Once leader is elected, send heartbeat regularly
				go rf.heartbeatDaemon()

				return
			}
			
		}	


		// the case Candidate=> Follower
		if reply.term>rf.term{
			rf.mu.Lock()
			rf.nodeState = "Follower"
			rf.voteFor = -1
			rf.mu.Unlock()
			return
		}	
	}


}

//Task2A: heartbeatDaemon
func (rf *Raft)heartbeatDaemon(){
	for{
		_, isLeader :=rf.GetState()
		if isLeader{
			
			for i:=0; i<len(rf.peers); i++{
				if i==rf.me{
					rf.resetTimer<- struct{}{}				
				}else{
					go rf.heartbeat(i)
				}

			}
			time.Sleep(rf.HBInterval)
		}else{
			break
		}
	}
	return
}

//Task2A: heartbeat
func (rf *Raft)heartbeat(n int){
	var HBargs AppendEntriesArgs	
	rf.fillAppendEntriesArgs(&HBargs)

	var reply AppendEntriesReply
	if rf.sendAppendEntries(n, &HBargs, &reply){
		if !reply.success{
			rf.mu.Lock()

			if reply.term>rf.term{
				rf.term = reply.term
				rf.nodeState = "follower"
				rf.voteFor = -1
			}else{
				//TODO: As leader, send log to xiaodi
			}

			rf.mu.Unlock()
		}
	}
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

	// Your code here (2B).


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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.nodeState = "Follower"
	rf.term = 0
	rf.voteFor = -1
	rf.logs = make([]LogEntry,1)

	rf.resetTimer = make(chan struct{})
	rf.electionTimeout = (400+ time.Duration(rand.Intn(150)))*time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	rf.HBInterval = 100 *time.Millisecond  
	
	//not used right now
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = len(rf.logs)
	}


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start election
	go rf.electionDaemon()	

	return rf
}
