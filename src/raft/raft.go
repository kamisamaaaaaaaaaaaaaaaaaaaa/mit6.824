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

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	IsLeader bool

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	resetTimer    chan struct{}
	electionTimer *time.Timer

	commitCond   *sync.Cond
	newEntryCond []*sync.Cond

	applyChan chan ApplyMsg

	shutdown chan struct{}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.IsLeader

	return term, isleader

	// return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm int
	FirstIndex   int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Println(rf.me, "收到了来自", args.CandidateId, "的投票！")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//不给同期（同期的cand）或者term小于自己的投票
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		//对于投票者要将自己强行转化为follower（可能原本是leader）
		// if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.IsLeader = false
		rf.votedFor = -1

		// }
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1 ||
				args.LastLogTerm > rf.log[len(rf.log)-1].Term {
				rf.resetTimer <- struct{}{}

				rf.IsLeader = false
				rf.votedFor = args.CandidateId

				reply.VoteGranted = true
			}
		}
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Println("收到了来自", args.LeaderCommit, "的心跳")
	select {
	case <-rf.shutdown:
		return
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.IsLeader {
		rf.IsLeader = false
		rf.wakeupConsistencyCheck()
	}

	if rf.votedFor != args.LeaderId {
		rf.votedFor = args.LeaderId
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}

	rf.resetTimer <- struct{}{}

	preLogIdx, preLogTerm := 0, 0

	if len(rf.log) > args.PrevLogIndex {
		preLogIdx = args.PrevLogIndex
		preLogTerm = rf.log[preLogIdx].Term
	}

	if preLogIdx == args.PrevLogIndex && preLogTerm == args.PrevLogTerm {
		reply.Success = true
		rf.log = rf.log[:preLogIdx+1]
		rf.log = append(rf.log, args.Entries...)
		var last = len(rf.log) - 1

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, last)
			go func() {
				rf.commitCond.Broadcast()
			}()
		}

		reply.ConflictTerm = rf.log[last].Term
		reply.FirstIndex = last
	} else {
		reply.Success = false

		var first = 1
		reply.ConflictTerm = preLogTerm

		if reply.ConflictTerm == 0 {
			first = len(rf.log)
			reply.ConflictTerm = rf.log[first-1].Term
		} else {
			for i := preLogIdx - 1; i > 0; i-- {
				if rf.log[i].Term != preLogTerm {
					first = i + 1
					break
				}
			}
		}

		reply.FirstIndex = first
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
// term. the third return value is true if this server believes it is
// the leader.

func (rf *Raft) wakeupConsistencyCheck() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.newEntryCond[i].Broadcast()
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := 0
	isLeader := false

	// Your code here (2B).
	select {
	case <-rf.shutdown:
		return index, term, isLeader
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.IsLeader {
		log := LogEntry{Command: command, Term: rf.currentTerm}
		rf.log = append(rf.log, log)
		index = len(rf.log) - 1
		term = rf.currentTerm
		isLeader = true

		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		rf.wakeupConsistencyCheck()
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Heartbeat() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			// fmt.Println(rf.me, "发起心跳")
			rf.resetTimer <- struct{}{}
		} else {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf *Raft) FillRequestArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1
	rf.votedFor = rf.me

	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term
	args.Term = rf.currentTerm
}

func (rf *Raft) Election() {
	//填充参数
	args := RequestVoteArgs{}
	rf.FillRequestArgs(&args)

	//存取来自不同投票者的回复
	replies := make(chan RequestVoteReply, len(rf.peers))

	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.resetTimer <- struct{}{}
		} else {
			wg.Add(1)
			go func(u int) {
				defer wg.Done()

				reply := RequestVoteReply{}

				ok := rf.sendRequestVote(u, &args, &reply)
				if !ok {
					return
				}
				replies <- reply

			}(i)

		}
	}

	go func() {
		wg.Wait()
		close(replies)
	}()

	votes := 1
	has_been_leader := false
	for reply := range replies {
		if reply.VoteGranted && reply.Term == rf.currentTerm {
			votes += 1
			if votes > len(rf.peers)/2 {
				//只发起一次心跳
				if !has_been_leader {
					rf.mu.Lock()
					rf.IsLeader = true
					has_been_leader = true
					rf.mu.Unlock()

					rf.resetOnElection()

					go rf.Heartbeat()

					go rf.logEntryAgree()
				}

			}
			//若cand任期小，强制变回follower
		} else if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.IsLeader = false
			rf.votedFor = -1
			rf.mu.Unlock()
			rf.resetTimer <- struct{}{}
			return
		}
	}

}

func (rf *Raft) logEntryAgree() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.consistencyCheck(i)
		}
	}
}

func (rf *Raft) consistencyCheck(u int) {
	for {
		rf.mu.Lock()
		rf.newEntryCond[u].Wait()

		select {
		case <-rf.shutdown:
			rf.mu.Unlock()
			return
		default:
		}

		if rf.IsLeader {
			var args AppendEntriesArgs
			args.LeaderCommit = rf.commitIndex
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[u] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Term = rf.currentTerm

			if rf.nextIndex[u] < len(rf.log) {
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[u]:]...)
			} else {
				args.Entries = nil
			}
			rf.mu.Unlock()

			replych := make(chan AppendEntriesReply, 1)
			go func() {
				var reply AppendEntriesReply

				if rf.sendAppendEntries(u, &args, &reply) {
					replych <- reply
				}
			}()

			select {
			case reply := <-replych:
				rf.mu.Lock()
				if reply.Success {
					rf.matchIndex[u] = reply.FirstIndex
					rf.nextIndex[u] = rf.matchIndex[u] + 1
					//TODO更新索引
					rf.updateCommitIndex()
				} else {
					if reply.Term > args.Term {
						rf.currentTerm = reply.Term
						rf.votedFor = -1

					}

					if rf.IsLeader {
						rf.IsLeader = false
						rf.wakeupConsistencyCheck()
					}
					rf.mu.Unlock()
					rf.resetTimer <- struct{}{}
					return
				}

				var know, lastIndex = false, 0
				if reply.ConflictTerm != 0 {
					for i := len(rf.log) - 1; i > 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							know = true
							lastIndex = i
							break
						}
					}

					if know {
						if lastIndex > reply.FirstIndex {
							lastIndex = reply.FirstIndex
						}
						rf.nextIndex[u] = lastIndex
					} else {
						rf.nextIndex[u] = reply.FirstIndex
					}

				} else {
					rf.nextIndex[u] = reply.FirstIndex
				}

				rf.nextIndex[u] = min(max(rf.nextIndex[u], 1), len(rf.log))
			}
			rf.mu.Unlock()

		} else {
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			break
		}

		cnt := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= n {
				cnt++
				if cnt > len(rf.peers)/2 {
					rf.commitIndex = n
					break
				}
			}
		}
	}
}

func (rf *Raft) resetOnElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
		if i == rf.me {
			rf.matchIndex[i] = len(rf.log) - 1
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		select {
		//检查是否有重置超时
		case <-rf.resetTimer:
			// fmt.Println(rf.me, "重置时间")
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(time.Duration(400+(rand.Int63()%400)) * time.Millisecond)

			//检查是否选举超时
		case <-rf.electionTimer.C:
			//发起选举
			// fmt.Println(rf.me, "发起选举")
			go rf.Election()

			rf.electionTimer.Reset(time.Duration(400+(rand.Int63()%400)) * time.Millisecond)

		}

	}
}

func (rf *Raft) applyEntry() {
	for {
		var logs []LogEntry

		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.commitCond.Wait()
			select {
			case <-rf.shutdown:
				close(rf.applyChan)
				rf.mu.Unlock()
				return
			default:
			}

		}

		last, cur := rf.lastApplied, rf.commitIndex
		if last < cur {
			rf.lastApplied = rf.commitIndex
			logs = make([]LogEntry, cur-last)
			copy(logs, rf.log[last+1:cur+1])
		}

		rf.mu.Unlock()

		for i := 0; i < cur-last; i++ {
			reply := ApplyMsg{
				CommandIndex: last + 1 + i,
				Command:      logs[i],
				CommandValid: true,
			}
			rf.applyChan <- reply
		}
	}
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.IsLeader = false

	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0
	rf.log[0].Command = nil

	// fmt.Println(len(rf.log))

	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}

	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
	}

	rf.resetTimer = make(chan struct{})
	rf.electionTimer = time.NewTimer(time.Duration(400+(rand.Int63()%400)) * time.Millisecond)

	rf.commitCond = sync.NewCond(&rf.mu)
	rf.newEntryCond = make([]*sync.Cond, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.newEntryCond[i] = sync.NewCond(&rf.mu)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// fmt.Println("id: ", rf.me, " logs长度: ", len(rf.log))
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyEntry()

	return rf
}
