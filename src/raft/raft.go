package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, TERM, isleader)
//   start agreement on a new log entry
// rf.GetState() (TERM, isLeader)
//   ask a Raft for its current TERM, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	//"fmt"

	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//"strconv"
	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that SUCCESSive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	test      int
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	cv                *sync.Cond
	currentTerm       int
	lastAppendEntries time.Time
	timeout           int
	r                 *rand.Rand
	numVotes          int
	// "f", "c", "l"
	role           string
	logEntries     []LogEntry
	countResponses int
	commitIdx      int
	votedFor       int
	applyCh        chan ApplyMsg
	nextIdx        map[int]int
	matchIdx       map[int]int
}

// return currentTERM and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.role == "l"
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTERM int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TERM        int
	LASTLOGIDX  int
	LASTLOGTERM int
	CANDIDATEID int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TERM        int
	VOTEGRANTED bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println(strconv.Itoa(rf.me) + " with currentTerm " + strconv.Itoa(rf.currentTerm) + " received requestVote from " + strconv.Itoa(args.CANDIDATEID) + " who has term " + strconv.Itoa(args.TERM))
	//rf.lastAppendEntries = time.Now()
	reply.TERM = rf.currentTerm
	reply.VOTEGRANTED = false
	if args.TERM < rf.currentTerm {
		return
	}

	if args.TERM > rf.currentTerm {
		if rf.role == "l" {
		}
		rf.role = "f"

		rf.currentTerm = args.TERM
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CANDIDATEID) && args.LASTLOGIDX >= len(rf.logEntries)-1 && rf.logEntries[len(rf.logEntries)-1].TERM <= args.LASTLOGTERM {
		reply.VOTEGRANTED = true
		rf.votedFor = args.CANDIDATEID
		fmt.Println(strconv.Itoa(rf.me) + " voted for " + strconv.Itoa(args.CANDIDATEID))
	}

}

type LogEntry struct {
	CMD  interface{}
	TERM int
}

func (rf *Raft) printLogEntries(prefix string) {
	fmt.Print(prefix + " logEntries for " + strconv.Itoa(rf.me) + ": ")
	for i := 0; i < len(rf.logEntries); i++ {
		intCmd, intOk := rf.logEntries[i].CMD.(int)
		if intOk {
			fmt.Print("[CMD: " + strconv.Itoa(intCmd) + "; TERM: " + strconv.Itoa(rf.logEntries[i].TERM) + "]")
		}
		stringCmd, stringOk := rf.logEntries[i].CMD.(string)
		if stringOk {
			fmt.Print("[CMD: " + stringCmd + "; TERM: " + strconv.Itoa(rf.logEntries[i].TERM) + "]")
		}

	}
	fmt.Println("")
}

type AppendEntriesArgs struct {
	LEADERTERM   int
	LEADERID     int
	PREVLOGIDX   int
	PREVLOGTERM  int
	ENTRIES      []LogEntry
	LEADERCOMMIT int
}

type AppendEntriesReply struct {
	TERM    int
	SUCCESS bool
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LEADERTERM >= rf.currentTerm {
		if rf.role == "l" {
		}
		rf.role = "f"
		if rf.currentTerm < args.LEADERTERM {
			rf.votedFor = -1
			rf.currentTerm = args.LEADERTERM
		}

		if len(args.ENTRIES) == 0 { // if no log entries to append
			reply.SUCCESS = false
		} else if args.PREVLOGIDX < len(rf.logEntries) && (rf.logEntries[args.PREVLOGIDX].TERM == args.PREVLOGTERM) { // if able to append log entries sent
			rf.logEntries = rf.logEntries[:args.PREVLOGIDX+1]
			reply.SUCCESS = true
			rf.logEntries = append(rf.logEntries, args.ENTRIES...)
		} else if args.PREVLOGIDX < len(rf.logEntries) && rf.logEntries[args.PREVLOGIDX].TERM != args.PREVLOGTERM { // unable to append log entries due to mismatched terms
			rf.logEntries = rf.logEntries[:args.PREVLOGIDX]
			reply.SUCCESS = false
		} else { // unable to append log entries as do not have up to args.PREVLOGIDX
			reply.SUCCESS = false
		}
	} else { // unable to append because leader aint a leader
		reply.SUCCESS = false
	}

	if args.LEADERCOMMIT > rf.commitIdx {
		prevCommitIdx := rf.commitIdx
		rf.commitIdx = min(args.LEADERCOMMIT, len(rf.logEntries)-1)
		// Apply all new commits
		for i := prevCommitIdx + 1; i <= rf.commitIdx; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[i].CMD,
				CommandIndex: i,
			}
		}
	}
	reply.TERM = rf.currentTerm
	rf.lastAppendEntries = time.Now()

	// fmt.Print("AppendEntries logEntries for " + strconv.Itoa(rf.me) + ": ")
	// fmt.Printf("%+q", rf.logEntries)
	// fmt.Println(rf.logEntries)
	rf.printLogEntries("AppendEntries")
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      0,
		CommandIndex: 0,
	}
	for rf.killed() == false {

		// Candidate state once entered this if statement
		if rf.role != "l" && time.Now().Sub(rf.lastAppendEntries) > time.Duration(rf.timeout)*time.Millisecond {
			rf.role = "c"
			rf.currentTerm++
			fmt.Println("Candidate server: " + strconv.Itoa(rf.me) + " currentTerm is: " + strconv.Itoa(rf.currentTerm))
			rf.numVotes = 1
			rf.lastAppendEntries = time.Now()
			rf.votedFor = rf.me
			// TODO: maybe lock over this, args in sendRequestVote
			for server_idx, _ := range rf.peers {
				if server_idx != rf.me {
					go rf.sendRequestVote(server_idx)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(rf.timeout))
		rf.mu.Lock()
		rf.timeout = 1000 + rand.Intn(500)
	}
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
func (rf *Raft) sendRequestVote(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := RequestVoteArgs{}
	args.TERM = rf.currentTerm
	args.LASTLOGIDX = len(rf.logEntries) - 1
	args.LASTLOGTERM = rf.logEntries[args.LASTLOGIDX].TERM
	args.CANDIDATEID = rf.me
	reply := RequestVoteReply{}
	fmt.Println("Sending requestVote from " + strconv.Itoa(rf.me) + " to " + strconv.Itoa(server))
	rf.mu.Unlock()
	rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	rf.mu.Lock()
	fmt.Println("Return from sending requestVote from " + strconv.Itoa(rf.me) + " to " + strconv.Itoa(server))
	if rf.role != "c" {
		return
	}
	if reply.TERM > rf.currentTerm {
		rf.currentTerm = reply.TERM
		if rf.role == "l" {
		}
		rf.role = "f"
		rf.votedFor = -1
		rf.lastAppendEntries = time.Now()
		return
	}
	if reply.VOTEGRANTED {
		rf.numVotes++
	}
	// Won election
	if rf.numVotes > (len(rf.peers) / 2) {
		rf.role = "l"
		// Reset rf.nextIdx and rf.matchIdx
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIdx[i] = len(rf.logEntries)
			rf.matchIdx[i] = 0
		}
		rf.leader_routine()
	}
	return
}

func (rf *Raft) leader_routine() {
	//rf.mu.Lock()

	for rf.killed() == false && rf.role == "l" {
		// Match index algo
		rf.matchIdx[rf.me] = len(rf.logEntries) - 1
		for N := rf.commitIdx + 1; N < len(rf.logEntries); N++ {
			count := 0
			for j := 0; j < len(rf.matchIdx); j++ {
				if rf.matchIdx[j] >= N && rf.logEntries[N].TERM == rf.currentTerm {
					count++
				}
				if count > len(rf.matchIdx)/2 {
					for i := rf.commitIdx + 1; i <= N; i++ {
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.logEntries[i].CMD,
							CommandIndex: i,
						}
					}
					rf.commitIdx = N
					N = len(rf.logEntries) // to break out of outer for
					break
				}
			}
		}

		fmt.Println("Leader routine of leader " + strconv.Itoa(rf.me))
		for server_idx, _ := range rf.peers {
			if server_idx != rf.me {
				go rf.sendAppendEntry(server_idx)
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
	}
}

func (rf *Raft) sendAppendEntry(server_idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := AppendEntriesArgs{}
	args.LEADERTERM = rf.currentTerm
	args.LEADERID = rf.me
	args.PREVLOGIDX = rf.nextIdx[server_idx] - 1
	args.PREVLOGTERM = rf.logEntries[args.PREVLOGIDX].TERM
	args.ENTRIES = rf.logEntries[args.PREVLOGIDX+1:]
	args.LEADERCOMMIT = rf.commitIdx
	reply := AppendEntriesReply{}
	if rf.role != "l" {
		return
	}
	rf.mu.Unlock()
	ok := rf.peers[server_idx].Call("Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()
	if ok && rf.role == "l" {
		if reply.TERM > rf.currentTerm {
			rf.currentTerm = reply.TERM
			if rf.role == "l" {
			}
			rf.role = "f"
			rf.votedFor = -1
			rf.lastAppendEntries = time.Now()
			return
		}
		if reply.SUCCESS {
			rf.nextIdx[server_idx] = len(rf.logEntries)
			rf.matchIdx[server_idx] = len(rf.logEntries) - 1
			rf.countResponses++
			if rf.countResponses > len(rf.peers)/2 || rf.role == "f" {
			}
		} else if len(args.ENTRIES) != 0 {
			rf.nextIdx[server_idx]--
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// TERM. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.role == "l" {
		logEntryEl := LogEntry{command, rf.currentTerm}
		rf.logEntries = append(rf.logEntries, logEntryEl)
		rf.countResponses = 1

		// fmt.Printf("%+q", rf.logEntries)
		// fmt.Printf("%+q", rf.matchIdx)
		// fmt.Print("Leader " + strconv.Itoa(rf.me) + " logEntries: ")
		// fmt.Printf("%+q", rf.logEntries)
		rf.printLogEntries("Start leader")

		return len(rf.logEntries) - 1, rf.currentTerm, rf.role == "l"

	}
	// fmt.Printf("%+q", rf.logEntries)

	return len(rf.logEntries), rf.currentTerm, rf.role == "l"
}

// current term: 5
// 2 3 4 5
// 2 3 4 4 4
// 2 3 4 4 4

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.r = rand.New(rand.NewSource(99))
	rf.cv = sync.NewCond(&rf.mu)
	rf.currentTerm = 0
	rf.timeout = 1000 + rand.Intn(500)
	rf.role = "f"
	rf.lastAppendEntries = time.Now()
	rf.numVotes = 0
	rf.countResponses = 0
	rf.votedFor = -1
	rf.test = 0
	// ??? possible fuck up
	rf.applyCh = applyCh
	rf.nextIdx = make(map[int]int)
	rf.matchIdx = make(map[int]int)
	rf.commitIdx = 0
	rf.logEntries = append(rf.logEntries, LogEntry{0, 0})

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIdx[i] = 1
		rf.matchIdx[i] = 0
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
