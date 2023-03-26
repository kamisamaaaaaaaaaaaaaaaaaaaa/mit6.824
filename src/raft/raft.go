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

	nextIndex []int

	//判断是否已经处理完冲突，没有处理完则则发过去的rpc不带数据，处理完冲突后再带上数据
	hasfixconflict []bool

	matchIndex []int

	resetTimer    chan struct{}
	electionTimer *time.Timer

	commitCond *sync.Cond

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

	//成功添加log时，或者二分探测不匹配是由日志更短导致时，返回len(rf.log)
	NextIndex int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("%v收到了来自%v的投票要求\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("cand %v 的term为 %v ,当前 %v 的term为 %v\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	reply.VoteGranted = false

	//不给同期（同期的cand）或者term小于自己的投票
	if args.Term <= rf.currentTerm {
		// fmt.Printf("%v的term %v 小于等于当前 %v 的term %v,不予投票\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		//对于投票者要将自己强行转化为follower（可能原本是leader）
		// if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// fmt.Printf("%v的isleader变为false\n", rf.me)
		rf.IsLeader = false
		rf.votedFor = -1
		reply.VoteGranted = false

		// }
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1 ||
				args.LastLogTerm > rf.log[len(rf.log)-1].Term {
				rf.resetTimer <- struct{}{}

				rf.IsLeader = false

				// fmt.Printf("%v的isleader变为false\n", rf.me)
				rf.votedFor = args.CandidateId

				reply.VoteGranted = true

				// fmt.Printf("%v 给 %v 投票\n", rf.me, args.CandidateId)
			}
		}
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("%v(是否为leader:%v, term为%v)收到了来自%v的心跳(term为%v)\n", rf.me, rf.IsLeader, rf.currentTerm, args.LeaderId, args.Term)
	select {
	case <-rf.shutdown:
		return
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// fmt.Printf("leader %v 的term %v 比自己 %v 的 term %v 小，拒绝\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.IsLeader {
		// fmt.Printf("%v的isleader变为false\n", rf.me)
		rf.IsLeader = false
	}

	if rf.votedFor != args.LeaderId {
		rf.votedFor = args.LeaderId
	}

	if rf.currentTerm < args.Term {
		// fmt.Printf("%v收到了leader %v的心跳，将term %v 同步为 %v\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
	}

	rf.resetTimer <- struct{}{}

	// fmt.Printf("%v原来的log为:%v,接收到的log为:%v\n", rf.me, rf.log, args.Entries)

	//二分失败，返回失败响应
	if len(rf.log) < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.NextIndex = 0
		//如果是由于日志长度较短导致的，返回日志长度，方便右端点更新
		if len(rf.log) < args.PrevLogIndex+1 {
			reply.NextIndex = len(rf.log)
		}

	} else {
		reply.Success = true

		//非二分探测报文的话，成功后添加日志，若为二分探测报文，则直接返回成功响应

		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.NextIndex = len(rf.log)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			// fmt.Printf("%v收到%v的logrepli,commitindex更新为%v\n", rf.me, args.LeaderId, rf.commitIndex)
			go func() {
				rf.commitCond.Broadcast()
			}()
		}

	}

	// fmt.Printf("%v 的 log 变为: %v\n", rf.me, rf.log)

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
	// fmt.Printf("%v发送requesetvoterpc到%v\n", rf.me, server)
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

	if rf.IsLeader {
		// fmt.Printf("---------------------------------------%v接收到命令%v\n", rf.me, command)
		log := LogEntry{Command: command, Term: rf.currentTerm}
		rf.log = append(rf.log, log)
		index = len(rf.log) - 1
		term = rf.currentTerm
		isLeader = true

		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		rf.mu.Unlock()

	} else {
		rf.mu.Unlock()
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
	args.Term = rf.currentTerm

	args.PrevLogIndex = rf.nextIndex[u] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

	if rf.nextIndex[u] < len(rf.log) && rf.hasfixconflict[u] {
		args.Entries = append(args.Entries, rf.log[rf.nextIndex[u]:]...)
	} else {
		//二分探测报文或者没有数据发时发nil即可，减少通信字节数
		args.Entries = nil
	}

	// fmt.Printf("%v 发给 %v 的 rpc内容为：Find:%v, leadercommit:%v leader_id: %v prevlogindex:%v prevlogterm:%v term:%v logs:%v\n ",
	// rf.me, u, args.FindPrevIndex, args.LeaderCommit, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Term, args.Entries)

	rf.mu.Unlock()
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

				//若该leader为非法leader，会收到其它协程follower的失败回复，并更新自己的term
				//在下面更新自己term时会拿到锁，导致当前协程的fillargs被阻塞，当改完自己的term，
				//后，才会放开锁执行fillargs，但此时term已经不是原来的term了，也就是会拿着修改后的
				//的term带着旧日志发过去，由于此时term是最新的，follower会误以为当前心跳合法，然后用
				//非法的旧日志去更新已经提交的日志。若原本有leader的话，该leader收到该心跳后，还会变回
				//follower。所以在send之前要及时判断，若term已经修改过，isleader变为false，则停止发送
				if _, isleader := rf.GetState(); !isleader {
					return
				}

				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(u, &args, &reply)
				if !ok {
					return
				}

				// fmt.Printf("leader:%v，%v的回复的内容为:term : %v success:%v nextindex:%v\n", rf.me, u, reply.Term, reply.Success, reply.NextIndex)
				rf.mu.Lock()
				if reply.Success {
					rf.matchIndex[u] = reply.NextIndex - 1
					rf.nextIndex[u] = rf.matchIndex[u] + 1
					//若添加成功，说明已经处理完冲突了
					rf.hasfixconflict[u] = true

					// fmt.Printf("leader %v 收到了 %v 的成功回复，对应的next变为 %v,match变为 %v,left变为%v,right变为%v\n", rf.me, u, rf.nextIndex[u], rf.matchIndex[u], rf.sectionForPrevIndex[u][0], rf.sectionForPrevIndex[u][1])
					//TODO更新索引
					rf.updateCommitIndex()
				} else {
					// fmt.Printf("leader %v 收到了 %v 的失败回复\n", rf.me, u)
					if reply.Term > args.Term {
						// fmt.Printf("follower %v 的term %v 比自己leader %v 的term %v大，更新并放弃leader\n", u, reply.Term, rf.me, rf.currentTerm)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						if rf.IsLeader {
							rf.IsLeader = false
						}
						rf.mu.Unlock()
						rf.resetTimer <- struct{}{}
						return

					}

					//若仍然冲突，则nextindex减半
					//ps:有可能减到分界点之前，会重新发送一段包含已经提交过的数据过去，但不影响正确性
					//被覆盖后,follower已经提交的部分不变
					rf.nextIndex[u] = rf.nextIndex[u] / 2

					if rf.nextIndex[u] > reply.NextIndex {
						rf.nextIndex[u] = reply.NextIndex
					}

				}
				rf.nextIndex[u] = min(max(rf.nextIndex[u], 1), len(rf.log))
				// fmt.Printf("leader 为 %v，%v最终的next变为 %v,match变为 %v\n", rf.me, u, rf.nextIndex[u], rf.matchIndex[u])
				// }
				rf.mu.Unlock()

			}(i)

		}
	}

	wg.Wait()
	// fmt.Printf("leader %v (是否为leader: %v)wait结束\n", rf.me, rf.IsLeader)
}

func (rf *Raft) LogReplication() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			// fmt.Printf("%v发起心跳，term为%v\n", rf.me, rf.currentTerm)

			//重置leader时间，防止leader选举超时再次发起选举
			rf.resetTimer <- struct{}{}

			//这里一定要开协程，否则每次wait之前还要等该轮响应处理完，耗时长，导致心跳间隔变大，使follower容易
			//选举超时发起选举
			go rf.SingleReplcate()
		} else {
			// fmt.Printf("leader %v (是否为leader: %v) 变回follower\n", rf.me, rf.IsLeader)
			break
		}
		rf.resetTimer <- struct{}{}
		time.Sleep(110 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	// fmt.Printf("%v进入updatecommitindex\n", rf.me)

	//找到最大一个索引，这个索引之前的部分（包括这个索引）被超过半数复制，且当前项的term等于currentterm
	//这里可以考虑二分来找，有二段性
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			// fmt.Printf("update失败\n")
			break
		}

		cnt := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= n {
				cnt++
				if cnt > len(rf.peers)/2 {
					rf.commitIndex = n
					// fmt.Printf("%v的commitindex更新为:%v\n", rf.me, rf.commitIndex)
					rf.commitCond.Broadcast()
					return
				}
			}
		}

		// fmt.Printf("对于index %v 已经有 %v 个follower 复制到了\n", n, cnt)
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
	if _, isleader := rf.GetState(); isleader {
		return
	}

	// fmt.Printf("%v（是否为leader :%v ）发起选举\n", rf.me, rf.IsLeader)
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

	// fmt.Printf("%v已收集完选票\n", rf.me)

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
					// fmt.Printf("%v当选leader，收到选票数为%v\n", rf.me, votes)
					rf.mu.Lock()
					rf.IsLeader = true
					has_been_leader = true
					rf.mu.Unlock()

					rf.resetOnElection()
					go rf.LogReplication()

					// go rf.logEntryAgree()
				}

			}
			//若cand任期小，强制变回follower
		} else if reply.Term > rf.currentTerm {
			// fmt.Printf("%v的任期号小，为%v，收到follower的任期号为%v，变回follower\n", rf.me, rf.currentTerm, reply.Term)
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.IsLeader = false
			rf.votedFor = -1
			rf.mu.Unlock()
			rf.resetTimer <- struct{}{}
			return
		}
	}

	if !has_been_leader {
		// fmt.Printf("%v收到的选票不够，当选失败\n", rf.me)
		rf.IsLeader = false
	}

}

func (rf *Raft) resetOnElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {

		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
		rf.hasfixconflict[i] = false
		if i == rf.me {
			rf.matchIndex[i] = len(rf.log) - 1
			rf.hasfixconflict[i] = true
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

		// fmt.Printf("%v提交的log为:%v\n", rf.me, logs)

		for i := 0; i < cur-last; i++ {
			reply := ApplyMsg{
				CommandIndex: last + 1 + i,
				Command:      logs[i].Command,
				CommandValid: true,
			}
			// fmt.Printf("%v将%v塞到apply中\n", rf.me, reply)
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

	// fmt.Println(len(rf.log))

	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}

	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
	}

	rf.hasfixconflict = make([]bool, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.hasfixconflict[i] = false
	}

	rf.resetTimer = make(chan struct{})
	rf.electionTimer = time.NewTimer(time.Duration(400+(rand.Int63()%400)) * time.Millisecond)

	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// fmt.Println("id: ", rf.me, " logs长度: ", len(rf.log))
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyEntry()

	return rf
}
