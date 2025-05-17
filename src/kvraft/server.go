package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0
const RaftTimeout = 500 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type CommandType string

const (
	PUT    CommandType = "Put"
	GET    CommandType = "Get"
	APPEND CommandType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandType CommandType
	Key         string
	Value       string
	Seq         int
	CliId       int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 每个 client 最后执行的 id，结果
	lastResult map[int]int
	kvs        map[string]string
	// 每个 index 对应的通道
	indexChan map[int]chan ApplyNotifyMsg
	//// 用作状态超过 maxraftstate 的通知
	//snapSizeCond *sync.Cond
	//snapSizeFlag bool
}
type ApplyNotifyMsg struct {
	value string
	err   Err
	term  int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cliId := args.CliId
	// 如果不做这步，超时的情况虽然不会让状态机重复执行，但是会让 log 一直重复增长, 而且因为对于一个 cli 来说只能同步执行，所以会一直超时重试
	// 让后面的命令全都执行不了
	kv.mu.Lock()
	if maxSeq, ok := kv.lastResult[cliId]; ok && args.Seq <= maxSeq {
		reply.Value = kv.kvs[args.Key]
		reply.Err = OK
		DPrintf("%v get repeat request, key: %v, val:%v", kv.me, args.Key, reply.Value)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{CommandType: GET, Key: args.Key, Seq: args.Seq, CliId: cliId}
	//DPrintf("begin start get: %v", op)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("get wrongLeader，%v", kv.me)
		return
	}
	kv.mu.Lock()
	DPrintf("%v is leader", kv.me)
	kv.indexChan[index] = make(chan ApplyNotifyMsg, 1)
	ch := kv.indexChan[index]
	kv.mu.Unlock()
	select {
	case res := <-ch:
		if res.term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Value = res.value
			reply.Err = res.err
		}
		DPrintf("%v get indexChan: %v", kv.me, res)
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		DPrintf("%v get timeOut in %v", kv.me, index)
	}
	kv.mu.Lock()
	delete(kv.indexChan, index)
	DPrintf("%v delete chan %v", kv.me, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cliId := args.CliId
	kv.mu.Lock()
	if maxSeq, ok := kv.lastResult[cliId]; ok && args.Seq <= maxSeq {
		reply.Err = OK
		DPrintf("%v PutAppend repeat request, key: %v", kv.me, args.Key)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{CommandType: CommandType(args.Op), Key: args.Key, Value: args.Value, Seq: args.Seq, CliId: cliId}
	DPrintf("%v begin start putappend: %v", kv.me, op)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("PutAppend wrongLeader，%v", kv.me)
		return
	}
	kv.mu.Lock()
	DPrintf("%v is leader index: %v", kv.me, index)
	kv.indexChan[index] = make(chan ApplyNotifyMsg, 1)
	ch := kv.indexChan[index]
	kv.mu.Unlock()
	select {
	case res := <-ch:
		if res.term != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = res.err
			DPrintf("%v PutAppend indexChan: %v", kv.me, res)
		}
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		DPrintf("%v PutAppend timeOut in %v", kv.me, index)
	}
	kv.mu.Lock()
	delete(kv.indexChan, index)
	DPrintf("%v delete chan %v", kv.me, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func AbsInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

//// 保存应用层 server 持久化状态 快照，lastResult
//func (kv *KVServer) saveSnapShot() []byte {
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//	DPrintf("%v begin saveSnapShot....kv.kvs:%v, kv.lastResult:%v", kv.me, kv.kvs, kv.lastResult)
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w)
//	e.Encode(kv.kvs)
//	e.Encode(kv.lastResult)
//	data := w.Bytes()
//	kv.rf.SaveSnapShot(data)
//	return data
//}

func (kv *KVServer) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	DPrintf("%v begin readSnapShot....", kv.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvs map[string]string
	// 如果不保存这个，就会出现刚开始日志提交了两个重复的 append，假如 server 挂了重新恢复的时候，维护的每个 cli最后一个的值没了，
	// 这个时候重新执行就会执行成功。
	var lastResult map[int]int
	var LastIncludedIndex int
	var LastIncludedTerm int

	if err := d.Decode(&LastIncludedIndex); err != nil {
		DPrintf("server readSnapShot decode error LastIncludedIndex error: %v", err)
		return
	}

	if err := d.Decode(&LastIncludedTerm); err != nil {
		DPrintf("server readSnapShot decode error LastIncludedTerm error: %v", err)
		return
	}

	if d.Decode(&kvs) != nil {
		DPrintf("readSnapShot decode error  kvs error....")
		return
	}
	if d.Decode(&lastResult) != nil {
		DPrintf("readSnapShot decode error lastResult error....")
		return
	}

	kv.mu.Lock()
	log.Printf("acuqire s.readSnapShot success....")
	//kv.rf.LastIncludedTerm = LastIncludedTerm
	//kv.rf.LastIncludedIndex = LastIncludedIndex
	kv.kvs = kvs
	kv.lastResult = lastResult
	kv.mu.Unlock()
	DPrintf("%v finish readSnapShot...., kv.kvs:%v", kv.me,
		kvs)
	log.Printf("realse s.readSnapShot success....")
	//kv.mu.Lock()
	//defer kv.mu.Lock()
}

func (kv *KVServer) applyOP(msg raft.ApplyMsg) bool {
	op, ok := msg.Command.(Op)
	DPrintf("msg 内容：%v", msg)
	if !ok {
		log.Printf("转换出错, 内容：%v", msg)
		return false
	}
	commandType := op.CommandType
	var notifyMsg ApplyNotifyMsg
	kv.mu.Lock()
	log.Printf("acuqire s.applyOP success....")
	DPrintf("%v begin, apply: %v", kv.me, op)
	if maxSeq, ok := kv.lastResult[op.CliId]; ok && op.Seq <= maxSeq {
		if commandType == GET {
			notifyMsg.value = kv.kvs[op.Key]
		}
		DPrintf("%v repeat request, key: %v, type: %v", kv.me, op.Key, commandType)
		notifyMsg.err = OK
	} else {
		switch commandType {
		case GET:
			notifyMsg.value = kv.kvs[op.Key]
			DPrintf("%v apply, get: %v", kv.me, notifyMsg.value)
		case APPEND:
			if val, exists := kv.kvs[op.Key]; exists {
				kv.kvs[op.Key] = val + op.Value
				DPrintf("%v apply, append: %v", kv.me, kv.kvs[op.Key])
			} else {
				kv.kvs[op.Key] = op.Value
				DPrintf("%v apply, append: %v", kv.me, kv.kvs[op.Key])
			}
		case PUT:
			kv.kvs[op.Key] = op.Value
			DPrintf("%v apply, put: %v", kv.me, kv.kvs[op.Key])
		}
		kv.lastResult[op.CliId] = op.Seq
		//// 更新 lastApplied
		//kv.rf.UpdateApplyId(msg.CommandIndex)
	}
	notifyMsg.err = OK
	currentTerm, _ := kv.rf.GetState()
	notifyMsg.term = currentTerm
	ch, ex := kv.indexChan[msg.CommandIndex]
	if !ex {
		DPrintf("%v chan don't exist", msg)
		kv.mu.Unlock()
		log.Printf("realse s.applyOP success....")
		return false
	} else {
		kv.mu.Unlock()
		log.Printf("realse s.applyOP success....")
		// 通知等待的RPC处理程序
		DPrintf("%v sendindexChan, msg: %v", kv.me, notifyMsg)
		ch <- notifyMsg
		log.Printf("server send notifymsg success....")
		return true
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.lastResult = make(map[int]int)
	kv.indexChan = make(map[int]chan ApplyNotifyMsg)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//kv.snapSizeCond = sync.NewCond(&kv.mu)
	//kv.snapSizeFlag = false
	go func() {
		for !kv.killed() {
			msg := <-kv.applyCh
			log.Printf("server recv msg....")
			if msg.CommandValid {
				// kv应用
				kv.applyOP(msg)
				//if !ok {
				//	continue
				//}
				// 每次快照实际内容的更新其实在应用appId上，所有在这更新，而不是每次log增加的时候
				// 存在一种情况，假如 cut 在kv应用前会出现 bug，applyId 被更新了，然后执行 cut，但是更新的那个 kv 还没有进入快照，
				// 同时也不存在 log 里，假如这时候 raft 挂了，那个kv就找不回来了，要注意顺序，最好是按现实生活中逻辑发生的顺序编程

				// 还有一种情况，由于 applyId 是一下先更新的，applyId 更新到 265，消息一条条发送过来，假如到第 260的时候恰好触发了
				// cut，但是快照只存了 260，就会出现发出去的快照只包含 260，但是让接收者的 lastIncluded 更新到 265，进而 nextIndex 到了 265
				// 下次 leader 再发的时候中间 260到 265的所有东西都没了
				if maxraftstate != -1 && persister.RaftStateSize() > 0 &&
					float64(persister.RaftStateSize())/float64(maxraftstate) >= 0.9 {
					DPrintf("%v maxraftstate:%v,persister.RaftStateSize():%v", kv.me, float64(maxraftstate),
						float64(persister.RaftStateSize()))
					kv.rf.CutLogExceedMaxSizeAndSaveSnapShot(kv.kvs, kv.lastResult, msg.CommandIndex)
				}
			} else {
				log.Printf("server receve snapShot success")
				// 快照
				//kv.readSnapShot(msg.Command.([]byte))
				kv.readSnapShot(msg.Command.([]byte))
			}
		}
	}()
	// You may need initialization code here.
	//if maxraftstate != -1 {
	//	go func() {
	//		for !kv.killed() {
	//			if AbsInt(persister.RaftStateSize()-maxraftstate) <= 100 {
	//				kv.rf.CupLogExceedMaxSizeAndSaveSnapShot(kv.kvs)
	//			}
	//			time.Sleep(200 * time.Millisecond)
	//		}
	//	}()
	//}
	DPrintf("server 奔溃恢复")
	kv.readSnapShot(persister.ReadSnapshot())
	return kv
}
