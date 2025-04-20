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
	"math/rand"
	"sync"
	"time"
)
import "log"
import "sync/atomic"
import "../labrpc"

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
	term    int
	Command interface{}
}

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
	log         []LogEntry
	// 。。
	isLeader bool // 是否是 leader
	// 。。leader 独有
	matchIndex []int // 每个 follower 的最后日志 CandidateId，用于判断更新 lastCommitId
	nextIndex  []int // leader 给 follower 发送的下一个数据
	// 。。
	//Term              int                 // 周期，最小为 1
	lastHeartbeatTime time.Time     // 上一个心跳的时间
	electionOutTime   time.Duration // 选举过期时间
	lastCommitId      int           // 上一个提交的 CandidateId
	lastApplyId       int           // 上一个应用的 CandidateId
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	log.Printf("%v begin getState", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	isleader, term = rf.isLeader, rf.currentTerm
	if isleader {
		log.Printf("%v is Leader", rf.me)
	}
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

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 说明当前的 Term 大于 候选者，当前更适合当 leader
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.me == args.CandidateId {
		rf.votedFor = rf.me
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		log.Printf("%v sendRequestVote to %v，rf：%v", args, reply, rf)
		return
	}
	if rf.currentTerm != args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isLeader = false
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Printf("%v sendRequestVote to %v，rf：%v", args, reply, rf)
		return
	} else {
		n := len(rf.log)
		if n == 0 {
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				log.Printf("%v sendRequestVote to %v，rf：%v", args, reply, rf)
				return
			}
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			log.Printf("%v sendRequestVote to %v，rf：%v", args, reply, rf)
			return
		}
		// 比较最后一条日志是否是更新的
		lastLog := rf.log[n-1]
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			(lastLog.term < args.LastLogTerm || (args.LastLogTerm == lastLog.term &&
				args.LastLogIndex > n-1)) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			log.Printf("%v sendRequestVote to %v，rf：%v", args, reply, rf)
			return
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Printf("%v sendRequestVote to %v，rf：%v", args, reply, rf)
		return
	}
}

// AppendEntriesArgs 心跳连接，写请求
type AppendEntriesArgs struct {
	IsHeart      bool          // 是否是心跳
	Entries      []interface{} //写内容
	PreLogIndex  int           // 验证数据有效性的, return false
	PreLogTerm   int
	LeaderCommit int // tarCommit = min(LeaderCommit, tarCommit)
	LeaderId     int
	LeaderEpoch  int // tarCommit > LeaderId return false
}

type AppendReply struct {
	Term    int
	Success bool // 是否追加成功
}

// AppendEntries 心跳/追加 rpc handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//log.Printf("heartbeat from %v to %v", args.LeaderId, rf.me)
	rf.isLeader = false
	if rf.currentTerm != args.LeaderEpoch {
		rf.currentTerm = args.LeaderEpoch
		rf.votedFor = -1
		rf.isLeader = false
	}
	if args.IsHeart {
		rf.lastHeartbeatTime = time.Now()
		reply.Success = true
		// 收到请求的term
		reply.Term = rf.currentTerm
		// 说明进入了新的 epoch
		log.Printf("%v AppendEntries %v，rf：%v", args, reply, rf)
	} else {
		// 追加请求的处理
		log1 := rf.log
		currentTerm := rf.currentTerm
		// follower 比 leader 新, 也不会比 leader 旧
		if currentTerm > args.LeaderEpoch {
			reply.Term = currentTerm
			reply.Success = false
			// 成为 leader
			rf.isLeader = true
		}
		if currentTerm < args.LeaderEpoch {
			log.Printf("出现错误。。。。currentTerm < args.LeaderEpoch")
			reply.Term = currentTerm
			reply.Success = false
		}
		n := len(log1)
		// 数据校验不通过
		if n != 0 && (log1[n-1].term != args.PreLogTerm || n-1 != args.PreLogIndex) {
			// 将本地 follower 的数据按照 leader 进行更新
			reply.Success = false
		}
		// 开始从 PreLogIndex 更新
		for _, entry := range args.Entries {
			log1 = append(log1, LogEntry{currentTerm, entry})
		}
		rf.lastCommitId = min(rf.lastCommitId, args.LeaderCommit)
		reply.Success = true
		reply.Term = currentTerm
		log.Printf("%v AppendEntries %v，rf：%v", args, reply, rf)
	}
}

// 发送心跳/追加 rpc，对所有情况都适用：同时发这个，1.追加最新的，2.追加旧的，3.心跳
func (rf *Raft) sendRequestAppendEntries(server int, args *AppendEntriesArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//defer rf.mu.Unlock()
	//rf.mu.Lock()
	if !ok {
		// follower 的 Term 大于 leader 的，follower 成为 leader
		if reply.Term > rf.lastCommitId {
			rf.isLeader = false
			return false
		}
		// 日志冲突
		rf.nextIndex[server]--
		return false
	}
	rf.matchIndex[server]++
	rf.nextIndex[server]++
	return ok
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
	rf.electionOutTime = time.Duration(200+rand.Float32()*150) * time.Millisecond
	rf.votedFor = -1
	rf.lastHeartbeatTime = time.Time{}
	rf.currentTerm = 0
	// Your initialization code here (2A, 2B, 2C).
	// 开始心跳
	go func() {
		for {
			rf.mu.Lock()
			//log.Printf("raft【%v】 is leader: %v, raft echo is %v", rf.me, rf.isLeader, rf.currentTerm)
			// 是 leader 才发送心跳
			if rf.isLeader {
				// i 是 int 类型，_, i := range peers i才是peer 类型
				log.Printf("leader come: %v", me)
				for i := range peers {
					j := i
					go func() {
						if j == me {
							return
						}
						var req *AppendEntriesArgs = &AppendEntriesArgs{IsHeart: true, LeaderId: rf.me,
							LeaderEpoch: rf.currentTerm}
						reply := &AppendReply{}
						rf.sendRequestAppendEntries(j, req, reply)
					}()
				}
				//log.Printf("%v heartbeat sleep...", me)
				// 1s 10次心跳
				time.Sleep(100 * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}()

	// 开始选举
	go func() {
		for {
			time.Sleep(rf.electionOutTime)
			rf.mu.Lock()
			if !rf.isLeader && time.Now().Sub(rf.lastHeartbeatTime) >= rf.electionOutTime {
				log.Printf("%v begin election, epoch: %v", rf.me, rf.currentTerm)
				curVote := 0
				// 每次选举 term++
				rf.currentTerm++
				for i := range peers {
					//if i == me {
					//	continue
					//}
					i1 := i
					go func() {
						n := len(rf.log)
						lastLogIndex := 0
						lastLogTerm := 0
						if n != 0 {
							lastLog := rf.log[n-1]
							lastLogIndex = n - 1
							lastLogTerm = lastLog.term
						}
						log.Printf("curIndex : %v, curCandidate: %v", i1, rf.me)
						req := &RequestVoteArgs{rf.currentTerm, lastLogIndex,
							lastLogTerm, rf.me}
						reply := &RequestVoteReply{}
						rf.sendRequestVote(i1, req, reply)
						if reply.VoteGranted == true {
							curVote++
							if curVote > len(peers)/2 {
								rf.isLeader = true
								rf.nextIndex = make([]int, len(peers))
								rf.matchIndex = make([]int, len(peers))
								log.Printf("new leader: %v\n, curVote: %v, nums: %v",
									rf.me, curVote, len(peers))
							}
						}
						log.Printf("%v votedFor %v is %v",
							i1, rf.me, reply.VoteGranted)
					}()
				}
			}
			rf.mu.Unlock()
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
