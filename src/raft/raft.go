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

	// "fmt"
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//"strconv"
	//	"6.824/labgob"
	"6.824/labgob"
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

type LogEntry struct {
	CMD  interface{}
	TERM int
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
	role              string
	logEntries        []LogEntry
	countResponses    int
	commitIdx         int
	votedFor          int
	applyCh           chan ApplyMsg
	nextIdx           map[int]int
	matchIdx          map[int]int
	lastIncludedTerm  int
	lastIncludedIndex int
	// commitIdx >= snapshot index, matchIdx >= snapshotIdx, currentTerm >= snapshot term
}

func (rf *Raft) printLogEntries(prefix string) {
	fmt.Print(prefix + " logEntries for " + strconv.Itoa(rf.me) + ": ")
	for i := 0; i < len(rf.logEntries); i++ {
		intCmd, intOk := rf.logEntries[rf.raftIdxToLogIdx(i)].CMD.(int)
		if intOk {
			fmt.Print("[CMD: " + strconv.Itoa(intCmd) + "; TERM: " + strconv.Itoa(rf.logEntries[rf.raftIdxToLogIdx(i)].TERM) + "]")
		}
		stringCmd, stringOk := rf.logEntries[i].CMD.(string)
		if stringOk {
			fmt.Print("[CMD: " + stringCmd + "; TERM: " + strconv.Itoa(rf.logEntries[rf.raftIdxToLogIdx(i)].TERM) + "]")
		}

	}
	fmt.Println("")
}

// return currentTERM and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.role == "l"
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
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	rf.persister.SaveRaftState(rf.getPersistenceData())
}

func (rf *Raft) getPersistenceData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logEntries)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) bool {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return false
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logEntries []LogEntry
	var votedFor int
	var currentTerm int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&logEntries) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("Error with decoding in readPersist.")
	} else {
		rf.logEntries = logEntries
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}

	return true
}

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
		rf.persist()
		lastLogIdx, _ := rf.lastIdxAndTerm()
		return lastLogIdx, rf.currentTerm, rf.role == "l"

	}
	lastLogIdx, _ := rf.lastIdxAndTerm()
	return lastLogIdx, rf.currentTerm, rf.role == "l"
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
	// fmt.Println("Called Snapshot before lock on index: " + strconv.Itoa(index) + " and server " + strconv.Itoa(rf.me))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("Called Snapshot after lock on index: " + strconv.Itoa(index))
	prevLogIdx := rf.raftIdxToLogIdx(index)
	rf.lastIncludedTerm = rf.logEntries[prevLogIdx].TERM
	rf.lastIncludedIndex = index
	rf.logEntries = rf.logEntries[min(prevLogIdx+1, len(rf.logEntries)):]
	rf.persister.SaveStateAndSnapshot(rf.getPersistenceData(), snapshot)
}

func (rf *Raft) raftIdxToLogIdx(raftIdx int) int {
	return raftIdx - rf.lastIncludedIndex - 1
}

func (rf *Raft) lastIdxAndTerm() (int, int) {
	lastLogIdx := len(rf.logEntries) + rf.lastIncludedIndex
	lastLogTerm := -1
	if len(rf.logEntries) == 0 {
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogTerm = rf.logEntries[rf.raftIdxToLogIdx(lastLogIdx)].TERM
	}
	return lastLogIdx, lastLogTerm
}

type InstallSnapshotArgs struct {
	TERM         int
	LASTINCLIDX  int
	LASTINCLTERM int
	DATA         []byte
}

type InstallSnapshotReply struct {
	TERM int
}

// Lock is already acquired when this function is called
func (rf *Raft) sendInstallSnapshot(server int) {
	args := InstallSnapshotArgs{}
	args.DATA = rf.persister.ReadSnapshot()
	args.LASTINCLIDX = rf.lastIncludedIndex
	args.LASTINCLTERM = rf.lastIncludedTerm
	args.TERM = rf.currentTerm
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	rf.mu.Lock()
	if rf.currentTerm == args.TERM && rf.role == "l" {
		if reply.TERM > rf.currentTerm {
			rf.currentTerm = reply.TERM
			rf.role = "f"
			rf.persist()
		}
		if ok {
			rf.matchIdx[server] = max(args.LASTINCLIDX, rf.matchIdx[server])
			rf.nextIdx[server] = max(args.LASTINCLIDX+1, rf.nextIdx[server])
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.TERM = rf.currentTerm
	if rf.currentTerm > args.TERM || args.LASTINCLIDX <= rf.lastIncludedIndex || rf.role == "l" {
		return
	}
	rf.currentTerm = args.TERM
	lastLogIdx, _ := rf.lastIdxAndTerm()
	if args.LASTINCLIDX <= lastLogIdx && args.LASTINCLTERM == rf.logEntries[rf.raftIdxToLogIdx(args.LASTINCLIDX)].TERM {
		rf.logEntries = rf.logEntries[rf.raftIdxToLogIdx(args.LASTINCLIDX)+1:]
	} else {
		rf.logEntries = nil
	}
	rf.lastIncludedIndex = args.LASTINCLIDX
	rf.lastIncludedTerm = args.LASTINCLTERM
	rf.commitIdx = args.LASTINCLIDX
	rf.persister.SaveStateAndSnapshot(rf.getPersistenceData(), args.DATA)
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		Snapshot:      args.DATA,
		SnapshotTerm:  args.LASTINCLTERM,
		SnapshotIndex: args.LASTINCLIDX,
	}
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
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
	args.LASTLOGIDX, args.LASTLOGTERM = rf.lastIdxAndTerm()
	args.CANDIDATEID = rf.me
	reply := RequestVoteReply{}
	// fmt.Println("Sending requestVote from " + strconv.Itoa(rf.me) + " to " + strconv.Itoa(server))
	rf.mu.Unlock()
	rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	rf.mu.Lock()
	// fmt.Println("Return from sending requestVote from " + strconv.Itoa(rf.me) + " to " + strconv.Itoa(server))
	if rf.role != "c" || rf.currentTerm != args.TERM {
		return
	}
	if reply.TERM > rf.currentTerm {
		rf.currentTerm = reply.TERM
		rf.role = "f"
		rf.votedFor = -1
		rf.persist()
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
			rf.nextIdx[i] = args.LASTLOGIDX + 1
			rf.matchIdx[i] = 0
		}
		rf.leader_routine()
	}
	return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println(strconv.Itoa(rf.me) + " with currentTerm " + strconv.Itoa(rf.currentTerm) + " received requestVote from " + strconv.Itoa(args.CANDIDATEID) + " who has term " + strconv.Itoa(args.TERM))
	//rf.lastAppendEntries = time.Now()
	reply.TERM = rf.currentTerm
	reply.VOTEGRANTED = false
	if args.TERM < rf.currentTerm {
		return
	}
	needPersist := false

	if args.TERM > rf.currentTerm {
		rf.role = "f"
		rf.currentTerm = args.TERM
		rf.votedFor = -1
		needPersist = true
	}

	lastLogIdx, lastLogTerm := rf.lastIdxAndTerm()

	if (rf.votedFor == -1 || rf.votedFor == args.CANDIDATEID) &&
		(lastLogTerm < args.LASTLOGTERM) ||
		((lastLogTerm == args.LASTLOGTERM) && args.LASTLOGIDX >= lastLogIdx) {
		reply.VOTEGRANTED = true
		rf.votedFor = args.CANDIDATEID
		needPersist = true
		// fmt.Println(strconv.Itoa(rf.me) + " voted for " + strconv.Itoa(args.CANDIDATEID))
	}

	if needPersist {
		rf.persist()
	}

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
	TERM         int
	SUCCESS      bool
	NEXTIDX      int
	CONFLICTTERM int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server_idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.nextIdx[server_idx] <= rf.lastIncludedIndex {
		rf.sendInstallSnapshot(server_idx)
		return
	}
	args := AppendEntriesArgs{}
	args.LEADERTERM = rf.currentTerm
	args.LEADERID = rf.me
	args.PREVLOGIDX = rf.nextIdx[server_idx] - 1
	if rf.raftIdxToLogIdx(args.PREVLOGIDX) == -1 {
		args.PREVLOGTERM = rf.lastIncludedTerm
	} else if rf.raftIdxToLogIdx(args.PREVLOGIDX) >= 0 {
		args.PREVLOGTERM = rf.logEntries[rf.raftIdxToLogIdx(args.PREVLOGIDX)].TERM
	} else {
		fmt.Println("Error: rf.raftIdxToLogIdx(args.PREVLOGIDX) < -1")
	}
	args.ENTRIES = rf.logEntries[rf.raftIdxToLogIdx(args.PREVLOGIDX+1):]
	args.LEADERCOMMIT = rf.commitIdx
	reply := AppendEntriesReply{}
	lastLogIdx, _ := rf.lastIdxAndTerm()
	if rf.role != "l" {
		return
	}
	rf.mu.Unlock()
	ok := rf.peers[server_idx].Call("Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()
	if ok && rf.role == "l" && rf.currentTerm == args.LEADERTERM {
		if reply.TERM > rf.currentTerm {
			rf.currentTerm = reply.TERM
			rf.votedFor = -1
			rf.persist()
			rf.role = "f"
			rf.lastAppendEntries = time.Now()
			return
		}
		if reply.SUCCESS {
			rf.nextIdx[server_idx] = lastLogIdx + 1
			rf.matchIdx[server_idx] = lastLogIdx
		} else {
			if reply.CONFLICTTERM == -1 {
				rf.nextIdx[server_idx] = reply.NEXTIDX
			} else {
				// search to check if leader has reply.CONFLICTTERM
				hasConflictTerm := false
				for i := lastLogIdx; i > rf.lastIncludedIndex; i-- {
					if rf.logEntries[rf.raftIdxToLogIdx(i)].TERM == reply.CONFLICTTERM {
						rf.nextIdx[server_idx] = i
						hasConflictTerm = true
						break
					}
					if rf.logEntries[rf.raftIdxToLogIdx(i)].TERM < reply.CONFLICTTERM {
						break
					}
				}
				if !hasConflictTerm {
					rf.nextIdx[server_idx] = reply.NEXTIDX
				}
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.TERM = rf.currentTerm
	rf.lastAppendEntries = time.Now()
	savePersist := false

	if args.LEADERTERM >= rf.currentTerm {
		rf.role = "f"
		if rf.currentTerm < args.LEADERTERM {
			rf.votedFor = -1
			rf.currentTerm = args.LEADERTERM
			savePersist = true
		}

		// ensure rf.logEntries is only at most as long as leader's
		if rf.raftIdxToLogIdx(args.PREVLOGIDX+1) <= len(rf.logEntries) {
			rf.logEntries = rf.logEntries[:rf.raftIdxToLogIdx(args.PREVLOGIDX+1)]
		}
		lastLogIdx, lastLogTerm := rf.lastIdxAndTerm()

		// if able to append log entries sent, includes case where args.ENTRIES is empty
		if args.PREVLOGIDX <= lastLogIdx && (lastLogTerm == args.PREVLOGTERM) {
			reply.SUCCESS = true
			rf.logEntries = append(rf.logEntries, args.ENTRIES...)
			// commit
			if args.LEADERCOMMIT > rf.commitIdx {
				prevCommitIdx := rf.commitIdx
				rf.commitIdx = min(args.LEADERCOMMIT, lastLogIdx)
				// Apply all new commits
				for i := prevCommitIdx + 1; i <= rf.commitIdx; i++ {
					// fmt.Println("Follower server " + strconv.Itoa(rf.me) + " commited index " + strconv.Itoa(i))
					// TODO: Possible problem for rf.persist() at end of function (things to persist might have changed).
					applyMsg := ApplyMsg{
						CommandValid:  true,
						Command:       rf.logEntries[rf.raftIdxToLogIdx(i)].CMD,
						CommandIndex:  i,
						SnapshotValid: false,
					}
					rf.mu.Unlock()
					rf.applyCh <- applyMsg
					rf.mu.Lock()
				}
			}
			savePersist = true
		} else if args.PREVLOGIDX <= lastLogIdx && lastLogTerm != args.PREVLOGTERM { // unable to append log entries due to mismatched terms
			reply.SUCCESS = false
			reply.CONFLICTTERM = lastLogTerm
			// Possibly remove???
			rf.logEntries = rf.logEntries[:len(rf.logEntries)-1]
			// find first entry of this last mismatched term and set reply.NEXTIDX to it
			// 0 1 2 3
			reply.NEXTIDX = -1
			for i := lastLogIdx - 1; i > rf.lastIncludedIndex; i-- {
				if rf.logEntries[rf.raftIdxToLogIdx(i)].TERM != reply.CONFLICTTERM {
					reply.NEXTIDX = i + 1
					break
				}
			}
			// case where only up to rf.lastIncludedIndex is to be kept
			if reply.NEXTIDX == -1 {
				reply.NEXTIDX = rf.lastIncludedIndex + 1
			}
			savePersist = true
		} else { // unable to append log entries as do not have up to args.PREVLOGIDX
			reply.NEXTIDX = lastLogIdx + 1
			reply.SUCCESS = false
			reply.CONFLICTTERM = -1
		}
	} else { // unable to append because leader aint a leader
		reply.SUCCESS = false
	}

	if savePersist {
		rf.persist()
	}

	// rf.printLogEntries("AppendEntries")
	// fmt.Print("args: ")
	// fmt.Println(args)
	// fmt.Print("reply: ")
	// fmt.Println(reply)
}

func (rf *Raft) commitN() {
	lastLogIdx, _ := rf.lastIdxAndTerm()
	rf.matchIdx[rf.me] = lastLogIdx
	for N := lastLogIdx; N > rf.commitIdx; N-- {
		if rf.logEntries[rf.raftIdxToLogIdx(N)].TERM < rf.currentTerm {
			break
		}
		count := 0
		for j := 0; j < len(rf.matchIdx); j++ {
			if rf.matchIdx[j] >= N && rf.logEntries[rf.raftIdxToLogIdx(N)].TERM == rf.currentTerm {
				count++
			}
			if count > len(rf.matchIdx)/2 {
				for i := rf.commitIdx + 1; i <= N; i++ {
					// fmt.Println("Before leader server " + strconv.Itoa(rf.me) + " commited index " + strconv.Itoa(i))
					applyMsg := ApplyMsg{
						CommandValid:  true,
						Command:       rf.logEntries[rf.raftIdxToLogIdx(i)].CMD,
						CommandIndex:  i,
						SnapshotValid: false,
					}
					rf.mu.Unlock()
					rf.applyCh <- applyMsg
					rf.mu.Lock()
					// fmt.Println("After leader server " + strconv.Itoa(rf.me) + " commited index " + strconv.Itoa(i))
				}
				rf.commitIdx = N
				N = -1 // break out of outer for loop
				break
			}
		}
	}
}

func (rf *Raft) leader_routine() {
	//rf.mu.Lock()

	for rf.killed() == false && rf.role == "l" {
		// fmt.Println("before commitN for leader: " + strconv.Itoa(rf.me))
		rf.commitN()
		// fmt.Println("after commitN for leader: " + strconv.Itoa(rf.me))

		// fmt.Println("Leader routine of leader: " + strconv.Itoa(rf.me) + " with commitIdx: " + strconv.Itoa(rf.commitIdx))
		for server_idx, _ := range rf.peers {
			if server_idx != rf.me {
				go rf.sendAppendEntries(server_idx)
			}
		}
		rf.mu.Unlock()
		// fmt.Println("leader_routine unlock")
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		// fmt.Println("leader_routine lock")

	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.killed() == false {

		// Candidate state once entered this if statement
		if rf.role != "l" && time.Now().Sub(rf.lastAppendEntries) > time.Duration(rf.timeout)*time.Millisecond {
			rf.role = "c"
			rf.currentTerm++
			// fmt.Println("Candidate server: " + strconv.Itoa(rf.me) + " currentTerm is: " + strconv.Itoa(rf.currentTerm))
			rf.numVotes = 1
			rf.votedFor = rf.me
			rf.persist()
			rf.lastAppendEntries = time.Now()
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
	// Your initialization code here (2A, 2B, 2C).
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
	rf.applyCh = applyCh
	rf.nextIdx = make(map[int]int)
	rf.matchIdx = make(map[int]int)
	rf.commitIdx = 0
	rf.logEntries = append(rf.logEntries, LogEntry{0, 0})
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	// initialize from state persisted before a crash
	newInit := rf.readPersist(persister.ReadRaftState())
	if newInit {
		rf.persist()
	}
	if rf.lastIncludedIndex > -1 {
		rf.persister.ReadSnapshot()
		rf.commitIdx = rf.lastIncludedIndex
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			CommandValid:  false,
			Snapshot:      rf.persister.ReadSnapshot(),
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
		}
		applyCh <- applyMsg
	}

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIdx[i] = 1
		rf.matchIdx[i] = 0
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
