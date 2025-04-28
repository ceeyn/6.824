package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}
type State int

// 枚举，状态机，光靠 isLeader 不严谨
const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state 日志，快照，周期，投票人
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 。。持久化
	currentTerm int
	votedFor    int
	// index 从 1 开始
	log []LogEntry
	// 。。
	//isLeader bool // 是否是 leader
	state State
	// 。。leader 独有
	matchIndex []int // 每个 follower 的最后日志 CandidateId，用于判断更新 CommitId
	nextIndex  []int // leader 给 follower 发送的下一个数据
	// 。。
	//Term              int                 // 周期，最小为 1
	lastHeartbeatTime time.Time     // 上一个心跳的时间
	electionOutTime   time.Duration // 选举过期时间
	CommitId          int           // 当前 raft 提交的最大日志 id，0代表什么也没提交，因为 log 从 1 开始
	ApplyId           int           // 当前 raft 应用到状态机的最大日志 id
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	DPrintf("%v begin getState", rf.me)
	var term int
	var isleader bool
	// Your code here (2A).
	isleader, term = rf.state == Leader, rf.currentTerm
	if isleader {
		DPrintf("%v is Leader", rf.me)
	}
	rf.mu.Unlock()
	DPrintf("GetState release lock")
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//rf.mu.Lock()
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	log := rf.log
	//rf.mu.Unlock()
	DPrintf("before persist....rf.me:%v, rf.currentTerm:%v, rf.votedFor:%v, rf.log:%v", rf.me, currentTerm,
		votedFor, log)
	e.Encode(currentTerm)
	e.Encode(votedFor)
	e.Encode(log)
	data := w.Bytes()
	DPrintf("finish persist....")
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf("decode error....")
	} else {
		DPrintf("begin readPersist....")
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
		DPrintf("finish readPersist....rf.me:%v, rf.currentTerm: %v, rf.votedfor: %v, rf.log:%v", rf.me,
			currentTerm, votedFor, log)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LastLogIndex int
	LastLogTerm  int
	// 当前候选者的 id，发起投票的人
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 说明当前的 Term 大于 候选者，当前更适合当 leader
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// todo
	//rf.lastHeartbeatTime = time.Now()
	//DPrintf("sendRequestVote,args.Term:%v,args.CandidateId:%v"+
	//	"rf.me：%v,rf.Term：%v,rf.voteFor：%v,rf.state：%v，args.LastLogTerm:%v,args.LastLogIndex:%v,curLastLogIndex: %v,"+
	//	"curLastLogTerm : %v", args.Term,
	//	args.CandidateId, rf.me, rf.currentTerm, rf.votedFor, rf.state, args.LastLogTerm, args.LastLogIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("sendRequestVote,args.Term:%v,args.CandidateId:%v，rf.me：%v,rf.Term：%v,rf.voteFor：%v,rf.state：%v", args.Term,
			args.CandidateId, rf.me, rf.currentTerm, rf.votedFor, rf.state)
		return
	}
	if rf.currentTerm < args.Term {
		rf.ConvertToFollower(args.Term)
		DPrintf("sendRequestVote become follower,args.Term:%v,args.CandidateId:%v，rf.me：%v,rf.Term：%v,rf.voteFor：%v,rf.state：%v", args.Term,
			args.CandidateId, rf.me, rf.currentTerm, rf.votedFor, rf.state)
	}
	reply.Term = rf.currentTerm
	DPrintf("sendRequestVote,args.Term:%v,args.CandidateId:%v，rf.me：%v,rf.Term：%v,rf.voteFor：%v,rf.state：%v", args.Term,
		args.CandidateId, rf.me, rf.currentTerm, rf.votedFor, rf.state)
	curLastLogIndex := 0
	curLastLogTerm := 0
	n := len(rf.log)
	curLastLogIndex = n - 1
	if n-1 >= 0 {
		curLastLogTerm = rf.log[n-1].Term
	}
	DPrintf("sendRequestVote,args.Term:%v,args.CandidateId:%v，rf.me：%v,rf.Term：%v,rf.voteFor：%v,rf.state：%v", args.Term,
		args.CandidateId, rf.me, rf.currentTerm, rf.votedFor, rf.state)
	if curLastLogTerm > args.LastLogTerm || (curLastLogTerm == args.LastLogTerm && args.LastLogIndex < curLastLogIndex) {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
	return

}

// AppendEntriesArgs 心跳连接，写请求
type AppendEntriesArgs struct {
	Entries      []LogEntry //写内容
	PreLogIndex  int        // 验证数据有效性的, return false
	PreLogTerm   int
	LeaderCommit int // tarCommit = min(LeaderCommit, tarCommit)
	LeaderId     int
	LeaderEpoch  int // tarCommit > LeaderId return false
}

type AppendReply struct {
	Term    int
	Success bool // 是否追加成功
	// 冲突时才有用
	Xterm  int
	XIndex int
	XLen   int
}

// AppendEntries 心跳/追加 rpc handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.LeaderEpoch {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//收到现任 Leader 的心跳请求，如果 AppendEntries 请求参数的任期是过期的(args.Term < currentTerm)，不能重置；
	//节点开始了一次选举；
	//节点投票给了别的节点（没投的话也不能重置）；
	rf.resetElectionTimer()
	if args.LeaderEpoch > rf.currentTerm {
		rf.ConvertToFollower(args.LeaderEpoch)
		DPrintf("AppendEntries become follower,args.leaderId: %v, args.epoch:%v，rf.me：%v,rf.Term：%v,"+
			"rf.voteFor：%v,rf.state：%v", args.LeaderId, args.LeaderEpoch, rf.me, rf.currentTerm, rf.votedFor, rf.state)
	}
	reply.Term = rf.currentTerm
	DPrintf("AppendEntries,args.leaderId:%v,args.epoch:%v，args.preIndex: %v, args.preTerm: %v, rf.me：%v,rf.Term：%v,"+
		"rf.voteFor：%v,rf.state：%v", args.LeaderId, args.LeaderEpoch, args.PreLogIndex, args.PreLogTerm, rf.me, rf.currentTerm, rf.votedFor, rf.state)
	if args.PreLogIndex < 0 {
		reply.Success = false
		return
	}
	// 冲突优化
	if args.PreLogIndex >= len(rf.log) {
		reply.Xterm = -1
		reply.XLen = len(rf.log)
		reply.Success = false
		return
	}
	if rf.log[args.PreLogIndex].Term != args.PreLogTerm {
		// 找到冲突 term 的第一个 log
		i := args.PreLogIndex
		for ; i >= 0 && rf.log[i].Term == rf.log[args.PreLogIndex].Term; i-- {
		}
		i++
		reply.XIndex = i
		reply.Xterm = rf.log[args.PreLogIndex].Term
		reply.Success = false
		return
	}
	if args.PreLogIndex < len(rf.log) {
		rf.log = rf.log[:args.PreLogIndex+1]
	}
	DPrintf("before leaderId: %v, rf.me: %v, LeaderCommit: %v, rf.log:%v", args.LeaderId,
		rf.me, args.LeaderCommit, rf.log)
	rf.log = append(rf.log, args.Entries...)
	DPrintf("after leaderId: %v, rf.me: %v, LeaderCommit: %v, rf.log:%v", args.LeaderId,
		rf.me, args.LeaderCommit, rf.log)
	rf.persist()
	//DPrintf("server: %v, append after log: %v", rf.me, rf.log)
	//DPrintf("AppendEntries CommitId: %v", rf.CommitId)
	if rf.CommitId < args.LeaderCommit {
		DPrintf("leaderId: %v, rf.me: %v, LeaderCommit: %v, len(rf.log)-1: %v", args.LeaderId,
			rf.me, args.LeaderCommit, len(rf.log)-1)
		DPrintf("rf.CommitId: %v", rf.CommitId)
		if rf.CommitId < args.LeaderCommit {
			rf.CommitId = min(args.LeaderCommit, len(rf.log)-1)
		}
	}
	reply.Success = true
}

// 发送心跳/追加 rpc，对所有情况都适用：同时发这个，1.追加最新的，2.追加旧的，3.心跳
func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%vsendRequestAppendEntries to %v before args.PreLogIndex: %v, args.term:%v,rf.epoch:%v,rf.state:%v", rf.me, server,
		args.PreLogIndex, args.LeaderEpoch, rf.currentTerm, rf.state)
	if rf.currentTerm != args.LeaderEpoch || rf.state != Leader {
		return false
	}
	if !reply.Success {
		// follower 的 Term 大于 leader 的，follower 成为 leader
		if reply.Term > rf.currentTerm {
			rf.ConvertToFollower(reply.Term)
			return false
		}
		// 日志冲突
		//rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
		// 冲突优化
		if reply.Xterm == -1 {
			// case 1
			rf.nextIndex[server] = reply.XLen
		} else {
			// 每次回退一个 term
			// case 2，假如follower 有 term 对应的日志
			lastTermIndex := -1
			for i := len(rf.log) - 1; i >= 1; i-- {
				if rf.log[i].Term == reply.Xterm {
					lastTermIndex = i
					break
				}
			}
			if lastTermIndex != -1 {
				rf.nextIndex[server] = lastTermIndex + 1
			} else {
				// case 3
				rf.nextIndex[server] = reply.XIndex
			}
		}
		return false
	}
	rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	DPrintf("%vsendRequestAppendEntries to %v  after rf.matchIndex: %v, rf.nextIndex: %v", rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
	rf.updateCommitIndex()
	return ok
}

// 尝试推进 CommitIndex
func (rf *Raft) updateCommitIndex() {
	// 尝试从最大 id 开始更新
	for i := len(rf.log) - 1; i > rf.CommitId && i > 0; i-- {
		count := 1
		for server := range len(rf.peers) {
			if server != rf.me && rf.matchIndex[server] >= i {
				count++
			}
		}
		// 当前 leader 只能提交复制到大多数且 Term 等于当前 Term 的 log，如论文图 8，
		// 否则就会出现一个之前任期的日志复制到其它 server，覆盖别的 server 上正确的数据，
		// 如果只提交自己 Term 的数据则可以顺便将之前任期的数据成功提交
		if count > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
			rf.CommitId = i
			DPrintf("leaderCommitId update: %v", rf.CommitId)
			break
		}
	}
}

// 重置选举时间
func AppendEntriesHandler() {

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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	// Your code here (2B).
	n := len(rf.log)
	index = n
	rf.log = append(rf.log, LogEntry{term, command})
	rf.persist()
	DPrintf("%vstart rf.log:%v", rf.me, rf.log)
	rf.mu.Unlock()
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
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ConvertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
}

func (rf *Raft) ConvertToLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	log.Printf("matchIndex : %v", len(rf.matchIndex))
	lastLogIndex := len(rf.log)
	for server := range rf.nextIndex {
		// Leader 假设每个 Follower 已经和它一致，并尝试从尾部开始追加日志
		rf.nextIndex[server] = lastLogIndex
		rf.matchIndex[server] = 0
	}
}

func (rf *Raft) ConvertToFollower(newTerm int) {
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = newTerm
	rf.persist()
}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeartbeatTime = time.Now()
	rf.electionOutTime = time.Duration(200+rand.Intn(200)) * time.Millisecond
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
	rf.CommitId = 0
	// rf.electionOutTime = time.Duration(200+rand.Float32()*150) * time.Millisecond
	//rf.resetElectionTimeout()
	rf.resetElectionTimer()
	//rf.lastHeartbeatTime = time.Time{}
	//rf.state = Follower
	//rf.votedFor = -1
	//rf.currentTerm = 0
	//rf.persister.mu.Lock()
	pn := len(rf.persister.raftstate)
	//rf.persister.mu.Unlock()
	if pn == 0 {
		rf.log = append(rf.log, LogEntry{0, 20516})
		rf.ConvertToFollower(0)
	}
	//rf.ConvertToFollower(0)
	//if len(rf.log) == 0 {
	//	rf.log = append(rf.log, LogEntry{Term: 0}) // 初始化空日志（索引0占位）
	//}
	// Your initialization code here (2A, 2B, 2C).
	// 开始心跳
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			// DPrintf("raft【%v】 is leader: %v, raft echo is %v", rf.me, rf.isLeader, rf.currentTerm)
			// 是 leader 才发送心跳
			if isLeader {
				// i 是 int 类型，_, i := range peers i才是peer 类型
				//DPrintf("leader come: %v", me)
				for i := range peers {
					if i == me {
						continue
					}
					go func(server int) {
						rf.mu.Lock()
						if !(rf.state == Leader) {
							rf.mu.Unlock()
							return
						}
						//preLogIndex := rf.matchIndex[server]
						preLogIndex := rf.nextIndex[server] - 1
						preLogTerm := rf.log[preLogIndex].Term
						leaderCommitId := rf.CommitId
						leaderId := rf.me
						leaderEpoch := rf.currentTerm
						var entries []LogEntry
						entries = make([]LogEntry, len(rf.log[preLogIndex+1:]))
						copy(entries, rf.log[preLogIndex+1:])
						rf.mu.Unlock()
						var req *AppendEntriesArgs = &AppendEntriesArgs{Entries: entries, PreLogIndex: preLogIndex,
							PreLogTerm: preLogTerm, LeaderCommit: leaderCommitId, LeaderId: leaderId,
							LeaderEpoch: leaderEpoch}
						reply := &AppendReply{}
						ok := rf.sendRequestAppendEntries(server, req, reply)
						if !ok {
							return
						}
					}(i)
				}
				// 1s 10次心跳，100ms每次，但是需要更快的心跳才能通过2c
				time.Sleep(70 * time.Millisecond)
			} else {
				time.Sleep(30 * time.Millisecond)
			}
		}
	}()

	// 开始选举
	go func() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)
			// 不能在 rpc 调用过程持有锁，在获取参数或者处理返回结果时候持有
			rf.mu.Lock()
			if rf.state != Leader && time.Since(rf.lastHeartbeatTime) > rf.electionOutTime {
				DPrintf("%v begin elec", rf.me)
				rf.ConvertToCandidate()
				candidateId := rf.me
				currentTerm := rf.currentTerm
				lastLogIndex := len(rf.log) - 1
				lastLogTerm := 0
				if lastLogIndex >= 0 {
					lastLogTerm = rf.log[lastLogIndex].Term
				}
				rf.mu.Unlock()
				curVote := 1
				for i := range peers {
					if i == me {
						continue
					}
					go func(server int) {
						DPrintf("curIndex : %v, curCandidate: %v", server, candidateId)
						req := &RequestVoteArgs{currentTerm, lastLogIndex,
							lastLogTerm, candidateId}
						reply := &RequestVoteReply{}
						ok := rf.sendRequestVote(server, req, reply)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if !ok || currentTerm != rf.currentTerm || rf.state != Candidate {
							return
						}
						voteGranted := reply.VoteGranted
						if voteGranted == true {
							curVote++
							if curVote > len(peers)/2 {
								if rf.currentTerm != currentTerm || rf.state != Candidate {
									return
								}
								rf.ConvertToLeader()
								DPrintf("new leader: %v\n, curVote: %v, nums: %v",
									rf.me, curVote, len(peers))
							}
						}
						DPrintf("%v votedFor %v is %v",
							server, rf.me, reply.VoteGranted)
					}(i)
				}
			} else {
				rf.mu.Unlock()
			}
		}
	}()

	// 提交 log 到状态机
	go func() {
		for !rf.killed() {
			time.Sleep(5 * time.Millisecond)

			rf.mu.Lock()
			var msgs []ApplyMsg
			for i := rf.ApplyId + 1; i <= rf.CommitId && i < len(rf.log); i++ {
				//DPrintf("id: %v, msg: %v", rf.me, msg)
				msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
				msgs = append(msgs, msg)
				rf.ApplyId = i
			}
			rf.mu.Unlock()

			// unlock 后再发送，避免阻塞 applyCh 导致锁无法释放
			for _, msg := range msgs {
				DPrintf("id: %v, msg: %v", rf.me, msg)
				applyCh <- msg
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
