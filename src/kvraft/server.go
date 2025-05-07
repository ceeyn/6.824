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

const Debug = 1
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
		DPrintf("get repeat request, key: %v", args.Key)
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
		DPrintf("get indexChan: %v", res)
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		DPrintf("get timeOut in %v", index)
	}
	kv.mu.Lock()
	delete(kv.indexChan, index)
	DPrintf("delete chan %v", index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cliId := args.CliId
	kv.mu.Lock()
	if maxSeq, ok := kv.lastResult[cliId]; ok && args.Seq <= maxSeq {
		reply.Err = OK
		DPrintf("PutAppend repeat request, key: %v", args.Key)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{CommandType: CommandType(args.Op), Key: args.Key, Value: args.Value, Seq: args.Seq, CliId: cliId}
	DPrintf("begin start putappend: %v", op)
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
			DPrintf("PutAppend indexChan: %v", res)
		}
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		DPrintf("PutAppend timeOut in %v", index)
	}
	kv.mu.Lock()
	delete(kv.indexChan, index)
	DPrintf("delete chan %v", index)
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

func (kv *KVServer) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	DPrintf("begin readSnapShot....")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvs map[string]string
	if d.Decode(&kvs) != nil {
		DPrintf("readSnapShot decode error....")
	} else {
		kv.mu.Lock()
		kv.kvs = kvs
		kv.mu.Unlock()
		DPrintf("finish readSnapShot....kv.me:%v , kv.kvs:%v", kv.me,
			kvs)
	}
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
	}
	notifyMsg.err = OK
	currentTerm, _ := kv.rf.GetState()
	notifyMsg.term = currentTerm
	ch, ex := kv.indexChan[msg.CommandIndex]
	if !ex {
		DPrintf("%v chan don't exist", msg)
		kv.mu.Unlock()
		return false
	} else {
		kv.mu.Unlock()
		// 通知等待的RPC处理程序
		DPrintf("%v sendindexChan, msg: %v", kv.me, notifyMsg)
		ch <- notifyMsg
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

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//kv.snapSizeCond = sync.NewCond(&kv.mu)
	//kv.snapSizeFlag = false
	go func() {
		for !kv.killed() {
			msg := <-kv.applyCh
			if msg.CommandValid {
				// 每次快照实际内容的更新其实在应用appId上，所有在这更新，而不是每次log增加的时候
				if maxraftstate != -1 && persister.RaftStateSize() > 0 &&
					float64(persister.RaftStateSize())/float64(maxraftstate) >= 0.95 {
					DPrintf("maxraftstate:%v,persister.RaftStateSize():%v", float64(maxraftstate), float64(persister.RaftStateSize()))
					kv.rf.CupLogExceedMaxSizeAndSaveSnapShot(kv.kvs)
				}
				// kv应用
				ok := kv.applyOP(msg)
				if !ok {
					continue
				}
			} else {
				DPrintf("receve snapShot")
				// 快照
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
	kv.readSnapShot(persister.ReadSnapshot())
	return kv
}
