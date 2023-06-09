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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	Index   int
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

	commitCond *sync.Cond

	applyChan chan ApplyMsg

	shutdown chan struct{}

	lastIncludedIndex int
	lastIncludedTerm  int
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
	//fmt.Printf("-------------------------------------------%v调用了Persist-----------------------------------------\n", rf.me)
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	raftstate := w.Bytes()

	rf.persister.SaveRaftState(raftstate)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//fmt.Printf("-------------------------------------------%v调用了readPersist-----------------------------------------\n", rf.me)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var CurrentTerm int
	var VotedFor int
	var Log []LogEntry
	var LastIncludedIndex int
	var LastIncludedTerm int

	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil || d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil {
		if d.Decode(&CurrentTerm) != nil {
			//fmt.Printf("------------------currentTerm decode failed----------------\n")
		}
		if d.Decode(&VotedFor) != nil {
			//fmt.Printf("------------------votefor decode failed----------------\n")
		}
		if d.Decode(&Log) != nil {
			//fmt.Printf("------------------log decode failed----------------\n")
		}
		if d.Decode(&LastIncludedIndex) != nil {
			//fmt.Printf("------------------lastIncludedIndex decode failed----------------\n")
		}
		if d.Decode(&LastIncludedTerm) != nil {
			//fmt.Printf("------------------lastIncludedTerm decode failed----------------\n")
		}

		// //fmt.Printf("------------------decode failed----------------\n")
	} else {
		rf.mu.Lock()
		rf.currentTerm = CurrentTerm
		rf.votedFor = VotedFor
		rf.log = Log
		rf.lastIncludedIndex = LastIncludedIndex
		rf.lastIncludedTerm = LastIncludedTerm
		rf.log[0].Index = rf.lastIncludedIndex
		rf.log[0].Term = rf.lastIncludedTerm
		rf.commitIndex = LastIncludedIndex
		rf.lastApplied = LastIncludedIndex

		//fmt.Printf("%v重启后的lastindex为: %v, lastterm为: %v, log为: %v\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log)

		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	//fmt.Printf("%v (是否为leader:%v) 在index %v 生成快照\n", rf.me, rf.IsLeader, index)

	rf.mu.Lock()
	if index <= rf.GetLogHeadIndex() || index > rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	//fmt.Printf("生成快照前的log为:%v\n", rf.log)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[rf.ToSeqInedx(index)].Term
	rf.log = rf.log[rf.ToSeqInedx(index):]
	rf.log[0].Command = nil
	rf.commitIndex = max(index, rf.commitIndex)
	rf.lastApplied = max(index, rf.lastApplied)
	rf.persist()
	rf.persister.SaveSnapshot(snapshot)
	//fmt.Printf("生成快照后的log为:%v\n", rf.log)

	//fmt.Printf("生成快照后的内容为:SnapshotIndex: %v SnapshotTerm:%v \n", rf.lastIncludedIndex, rf.lastIncludedTerm)

	rf.mu.Unlock()
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
	// //fmt.Printf("%v收到了来自%v的投票要求\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// //fmt.Printf("cand %v 的term为 %v ,当前 %v 的term为 %v\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	reply.VoteGranted = false

	//不给同期（同期的cand）或者term小于自己的投票
	if args.Term <= rf.currentTerm {
		// //fmt.Printf("%v的term %v 小于等于当前 %v 的term %v,不予投票\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		//对于投票者要将自己强行转化为follower（可能原本是leader）
		// if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// //fmt.Printf("%v的isleader变为false\n", rf.me)
		rf.IsLeader = false
		rf.votedFor = -1
		reply.VoteGranted = false

		// }
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.GetLogTailIndex() ||
				args.LastLogTerm > rf.log[len(rf.log)-1].Term {
				rf.resetTimer <- struct{}{}

				rf.IsLeader = false

				// //fmt.Printf("%v的isleader变为false\n", rf.me)
				rf.votedFor = args.CandidateId

				reply.VoteGranted = true

				// //fmt.Printf("%v 给 %v 投票\n", rf.me, args.CandidateId)
			}
		}

		rf.persist()

		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("%v(是否为leader:%v, term为%v)收到了来自%v的心跳(term为%v)\n", rf.me, rf.IsLeader, rf.currentTerm, args.LeaderId, args.Term)
	select {
	case <-rf.shutdown:
		return
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		//fmt.Printf("leader %v 的term %v 比自己 %v 的 term %v 小，拒绝\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.IsLeader {
		//fmt.Printf("%v的isleader变为false\n", rf.me)
		rf.IsLeader = false

	}

	if rf.votedFor != args.LeaderId {
		rf.votedFor = args.LeaderId
	}

	if rf.currentTerm < args.Term {
		//fmt.Printf("%v收到了leader %v的心跳，将term %v 同步为 %v\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
	}

	rf.resetTimer <- struct{}{}

	preLogIdx, preLogTerm := 0, 0

	if rf.GetLogTailIndex() >= args.PrevLogIndex {
		preLogIdx = args.PrevLogIndex
		// fmt.Printf("index:%v headindex:%v\n", preLogIdx, rf.GetLogHeadIndex())
		if rf.ToSeqInedx(preLogIdx) < 0 {
			reply.Success = false
			reply.ConflictTerm = 0
			reply.FirstIndex = rf.commitIndex + 1
			return
		}
		preLogTerm = rf.log[rf.ToSeqInedx(preLogIdx)].Term
	}

	//fmt.Printf("%v原来的log为:%v,接收到的log为:%v\n", rf.me, rf.log, args.Entries)
	if preLogIdx == args.PrevLogIndex && preLogTerm == args.PrevLogTerm {
		reply.Success = true
		rf.log = rf.log[:rf.ToSeqInedx(preLogIdx)+1]
		rf.log = append(rf.log, args.Entries...)

		rf.persist()

		// var last = len(rf.log) - 1
		var last = rf.GetLogTailIndex()

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, last)
			//fmt.Printf("%v收到%v的logrepli,commitindex更新为%v\n", rf.me, args.LeaderId, rf.commitIndex)
			go func() {
				rf.commitCond.Broadcast()
			}()
		}

		reply.ConflictTerm = rf.log[rf.ToSeqInedx(last)].Term
		reply.FirstIndex = last
	} else {
		reply.Success = false

		var first = rf.lastIncludedIndex + 1
		reply.ConflictTerm = preLogTerm

		if reply.ConflictTerm == 0 {
			first = rf.GetLogTailIndex() + 1
			reply.ConflictTerm = rf.log[rf.ToSeqInedx(first)-1].Term
		} else {
			for i := rf.ToSeqInedx(preLogIdx) - 1; i > 0; i-- {
				if rf.log[i].Term != preLogTerm {
					first = rf.log[i+1].Index
					break
				}
			}
		}

		reply.FirstIndex = first
	}

	//fmt.Printf("%v的log变为:%v\n", rf.me, rf.log)
}

func (rf *Raft) Detect_Few_Partition() bool {
	replies := 1

	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		} else {
			wg.Add(1)
			go func(u int) {
				defer wg.Done()

				args := AppendEntriesArgs{}
				rf.FillAppendEntriesArgs(u, &args)

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(u, &args, &reply)

				// timer := time.NewTimer(200 * time.Millisecond)

				// fmt.Printf("leader %v receive reply from %v,is_ok:%v\n", rf.me, u, ok)

				if ok {
					mu.Lock()
					replies += 1
					mu.Unlock()
				}

			}(i)
		}
	}

	wg.Wait()
	// fmt.Printf("leader %v receive %v replies , tot:%v\n", rf.me, replies, len(rf.peers))

	return replies <= len(rf.peers)/2
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
	// //fmt.Printf("%v发送requesetvoterpc到%v\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

func (rf *Raft) TryStart() (int, int, bool) {
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
		index = rf.GetLogTailIndex()
		term = rf.currentTerm
		isLeader = true
	}

	return index, term, isLeader
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
		//fmt.Printf("---------------------------------------%v接收到命令%v\n", rf.me, command)
		log := LogEntry{Command: command, Term: rf.currentTerm, Index: rf.GetLogTailIndex() + 1}
		rf.log = append(rf.log, log)
		//fmt.Printf("接受到命令后leader的log变为:%v\n", rf.log)
		index = rf.GetLogTailIndex()
		term = rf.currentTerm
		isLeader = true

		rf.persist()

		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
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

func (rf *Raft) FillAppendEntriesArgs(u int, args *AppendEntriesArgs) {
	rf.mu.Lock()

	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[u] - 1

	if args.PrevLogIndex < rf.lastIncludedIndex {
		args.PrevLogIndex = -1
		rf.mu.Unlock()
		return
	}

	args.PrevLogTerm = rf.log[rf.ToSeqInedx(args.PrevLogIndex)].Term
	args.Term = rf.currentTerm

	if rf.nextIndex[u] <= rf.GetLogTailIndex() {
		args.Entries = append(args.Entries, rf.log[rf.ToSeqInedx(rf.nextIndex[u]):]...)
	} else {
		args.Entries = nil
	}

	//fmt.Printf("此时leader %v 的 log为 %v \n", rf.me, rf.log)
	//fmt.Printf("%v 发给 %v 的 rpc内容为：leadercommit:%v leader_id: %v prevlogindex:%v prevlogterm:%v term:%v logs:%v\n ",
	// rf.me, u, args.LeaderCommit, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Term, args.Entries)

	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	NextIndex int
	Term      int
	Success   bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//fmt.Printf("%v(是否为leader:%v, term为%v)收到了来自%v的install(term为%v)\n", rf.me, rf.IsLeader, rf.currentTerm, args.LeaderId, args.Term)
	select {
	case <-rf.shutdown:
		return
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		//fmt.Printf("leader %v 的term %v 比自己 %v 的 term %v 小，拒绝install\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Success = false
		reply.NextIndex = rf.commitIndex + 1
		return
	}

	if rf.IsLeader {
		//fmt.Printf("(install)%v的isleader变为false\n", rf.me)
		rf.IsLeader = false

	}

	reply.Success = true

	if rf.votedFor != args.LeaderId {
		rf.votedFor = args.LeaderId
	}

	if rf.currentTerm < args.Term {
		//fmt.Printf("%v收到了leader %v的install，将term %v 同步为 %v\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
	}

	rf.resetTimer <- struct{}{}

	// rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	//fmt.Printf("%v收到了leader %v的install后,lastindex更新为%v,lastterm更新为%v\n", rf.me, args.LeaderId, rf.lastIncludedIndex, rf.lastIncludedTerm)
	//fmt.Printf("%v收到了leader %v的install后裁剪前的log为%v\n", rf.me, args.LeaderId, rf.log)

	agree_index := -1
	for i := 1; i < len(rf.log); i++ {
		if rf.log[i].Index == rf.lastIncludedIndex && rf.log[i].Term == rf.lastIncludedTerm {
			agree_index = i
			break
		}
	}

	//fmt.Printf("找到的agreeindex为%v\n", agree_index)

	if agree_index == -1 {
		rf.log = rf.log[:1]
		rf.log[0].Index = rf.lastIncludedIndex
		rf.log[0].Term = rf.lastIncludedTerm
	} else {
		rf.log = rf.log[agree_index:]
	}
	rf.log[0].Command = nil

	rf.persist()
	rf.persister.SaveSnapshot(args.Data)

	//fmt.Printf("%v收到了leader %v的install后裁剪后的log为%v\n", rf.me, args.LeaderId, rf.log)

	//fmt.Printf("%v的原来的commitindex为%v lastapplied为%v\n", rf.me, rf.commitIndex, rf.lastApplied)

	var put_snapshot_in_chan bool = true

	if rf.lastIncludedIndex <= rf.lastApplied {
		put_snapshot_in_chan = false
	}

	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

	//fmt.Printf("%v的原来的commitindex更新为%v lastapplied更新为%v\n", rf.me, rf.commitIndex, rf.lastApplied)

	applymsg := ApplyMsg{}
	applymsg.CommandValid = false
	applymsg.SnapshotValid = true
	applymsg.SnapshotIndex = rf.lastIncludedIndex
	applymsg.SnapshotTerm = rf.lastIncludedTerm
	applymsg.Snapshot = args.Data

	// if rf.commitIndex>=

	if put_snapshot_in_chan {
		go func() { rf.applyChan <- applymsg }()
	}

}

func (rf *Raft) FillInstallSnapshotArgs(args *InstallSnapshotArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.Data = rf.persister.ReadSnapshot()
}

func (rf *Raft) SingleReplcate() {
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		} else {
			wg.Add(1)
			go func(u int) {
				defer wg.Done()
				var args AppendEntriesArgs

				rf.FillAppendEntriesArgs(u, &args)

				//此时发送snapshot给follower
				if args.PrevLogIndex == -1 {
					ins_args := InstallSnapshotArgs{}
					rf.FillInstallSnapshotArgs(&ins_args)

					//fmt.Printf("leader %v 发给 %v的install内容为:term:%v leaderid:%v snapshotindex:%v snapshotterm:%v\n", rf.me, u, ins_args.Term, ins_args.LeaderId, ins_args.LastIncludedIndex, ins_args.LastIncludedIndex)

					reply := InstallSnapshotReply{}

					ok := rf.sendInstallSnapshot(u, &ins_args, &reply)

					if ok {
						if reply.Success {
							rf.nextIndex[u] = rf.lastIncludedIndex + 1
						} else {
							if reply.Term > rf.currentTerm {
								rf.mu.Lock()
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								if rf.IsLeader {
									rf.IsLeader = false
								}
								rf.persist()

								rf.mu.Unlock()
								rf.resetTimer <- struct{}{}
							} else {
								rf.nextIndex[u] = reply.NextIndex
							}
						}
					}

					return
				}

				if _, isleader := rf.GetState(); !isleader {
					return
				}

				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(u, &args, &reply)
				if !ok {
					return
				}

				//fmt.Printf("leader:%v，%v的回复的内容为:term : %v success:%v confi:%v first:%v\n", rf.me, u, reply.Term, reply.Success, reply.ConflictTerm, reply.FirstIndex)
				rf.mu.Lock()
				if reply.Success {
					rf.matchIndex[u] = reply.FirstIndex
					rf.nextIndex[u] = rf.matchIndex[u] + 1
					//fmt.Printf("leader %v 收到了 %v 的成功回复，对应的next变为 %v,match变为 %v\n", rf.me, u, rf.nextIndex[u], rf.matchIndex[u])
					//TODO更新索引
					rf.updateCommitIndex()
				} else {
					if reply.Term > args.Term {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						if rf.IsLeader {
							rf.IsLeader = false

						}
						rf.persist()

						rf.mu.Unlock()
						rf.resetTimer <- struct{}{}
						return

					}

					var know, lastIndex = false, 0
					if reply.ConflictTerm != 0 {
						for i := len(rf.log) - 1; i > 0; i-- {
							if rf.log[i].Term == reply.ConflictTerm {
								know = true
								lastIndex = rf.log[i].Index
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
					//fmt.Printf("leader 为 %v，%v此时的next变为 %v,match变为 %v\n", rf.me, u, rf.nextIndex[u], rf.matchIndex[u])
				}
				rf.nextIndex[u] = min(rf.nextIndex[u], rf.GetLogTailIndex()+1)
				//fmt.Printf("leader 为 %v，%v最终的next变为 %v,match变为 %v\n", rf.me, u, rf.nextIndex[u], rf.matchIndex[u])
				// }
				rf.mu.Unlock()

			}(i)

		}
	}

	wg.Wait()
	// //fmt.Printf("leader %v (是否为leader: %v)wait结束\n", rf.me, rf.IsLeader)

}

func (rf *Raft) LogReplication() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			//fmt.Printf("%v发起心跳，term为%v\n", rf.me, rf.currentTerm)
			rf.resetTimer <- struct{}{}

			go rf.SingleReplcate()
		} else {
			//fmt.Printf("leader %v (是否为leader: %v) 变回follower\n", rf.me, rf.IsLeader)
			break
		}
		rf.resetTimer <- struct{}{}
		time.Sleep(30 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	//fmt.Printf("%v进入updatecommitindex\n", rf.me)
	for n := rf.GetLogTailIndex(); n > rf.commitIndex; n-- {
		if rf.log[rf.ToSeqInedx(n)].Term != rf.currentTerm {
			//fmt.Printf("update失败\n")
			break
		}

		cnt := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= n {
				cnt++
				if cnt > len(rf.peers)/2 {
					rf.commitIndex = n
					//fmt.Printf("%v的commitindex更新为:%v\n", rf.me, rf.commitIndex)
					rf.commitCond.Broadcast()
					return
				}
			}
		}

		//fmt.Printf("对于index %v 已经有 %v 个follower 复制到了\n", n, cnt)
	}
}

func (rf *Raft) FillRequestArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1
	rf.votedFor = rf.me

	rf.persist()

	args.CandidateId = rf.me
	args.LastLogIndex = rf.GetLogTailIndex()
	args.LastLogTerm = rf.log[rf.ToSeqInedx(args.LastLogIndex)].Term
	args.Term = rf.currentTerm
}

func (rf *Raft) Election() {
	if _, isleader := rf.GetState(); isleader {
		return
	}

	//fmt.Printf("%v（是否为leader :%v ）发起选举\n", rf.me, rf.IsLeader)
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

	//fmt.Printf("%v已收集完选票\n", rf.me)

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
					//fmt.Printf("%v当选leader，收到选票数为%v\n", rf.me, votes)
					rf.mu.Lock()
					rf.IsLeader = true
					has_been_leader = true
					rf.mu.Unlock()

					rf.Start(nil)

					rf.resetOnElection()
					go rf.LogReplication()
				}

			}
			//若cand任期小，强制变回follower
		} else if reply.Term > rf.currentTerm {
			//fmt.Printf("%v的任期号小，为%v，收到follower的任期号为%v，变回follower\n", rf.me, rf.currentTerm, reply.Term)
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.IsLeader = false
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			rf.resetTimer <- struct{}{}
			return
		}
	}

	if !has_been_leader {
		//fmt.Printf("%v收到的选票不够，当选失败\n", rf.me)
		rf.IsLeader = false
	}

}

func (rf *Raft) resetOnElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.GetLogTailIndex() + 1
		rf.matchIndex[i] = 0
		if i == rf.me {
			rf.matchIndex[i] = rf.GetLogTailIndex()
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
			// //fmt.Println(rf.me, "重置时间")
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(time.Duration(400+(rand.Int63()%400)) * time.Millisecond)

			//检查是否选举超时
		case <-rf.electionTimer.C:
			//发起选举
			// //fmt.Println(rf.me, "发起选举")
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
			copy(logs, rf.log[rf.ToSeqInedx(last)+1:rf.ToSeqInedx(cur)+1])
		}

		rf.mu.Unlock()

		//fmt.Printf("%v提交的log为:%v\n", rf.me, logs)

		for i := 0; i < cur-last; i++ {
			reply := ApplyMsg{
				CommandIndex: last + 1 + i,
				Command:      logs[i].Command,
				CommandValid: true,
			}
			//fmt.Printf("%v将%v塞到apply中\n", rf.me, reply)
			rf.applyChan <- reply
		}
	}
}

func (rf *Raft) GetLogTailIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) GetLogHeadIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) ToSeqInedx(index int) int {
	return index - rf.GetLogHeadIndex()
}

func (rf *Raft) GetLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var leader_id int

	if rf.IsLeader {
		leader_id = rf.me
	} else {
		leader_id = rf.votedFor
	}

	return leader_id
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
	rf.applyChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.IsLeader = false

	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0
	rf.log[0].Command = nil
	rf.log[0].Index = 0

	// //fmt.Println(len(rf.log))

	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = rf.GetLogTailIndex() + 1
	}

	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
	}

	rf.resetTimer = make(chan struct{})
	rf.electionTimer = time.NewTimer(time.Duration(400+(rand.Int63()%400)) * time.Millisecond)

	rf.commitCond = sync.NewCond(&rf.mu)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// //fmt.Println("id: ", rf.me, " logs长度: ", len(rf.log))
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyEntry()

	return rf
}
