package shardkv

// import "../shardmaster"
import (
	"../labrpc"
	"../shardmaster"
	"bytes"
	"log"
	"time"
)
import "../raft"
import "sync"
import "../labgob"

var debug int = 0

type OpType string

const (
	Get           OpType = "Get"
	Put           OpType = "Put"
	Append        OpType = "Append"
	Config        OpType = "Config"
	ShardsPulling OpType = "ShardsPulling"
	ShardsDelete  OpType = "ShardsDelete"
)

type ShardStatus int

const (
	Illegal ShardStatus = iota
	Serving
	Pulling
	Pushing
	GC
)
const RaftTimeout = 300 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// 1. Key， Val 2.类型 put，append，get，Config 3.ClId， 4.SeqId， 5.
	Key                string
	Val                string
	OpType             OpType
	ClId               int
	SeqId              int
	Config             shardmaster.Config
	ShardsUpdateKvs    map[string]string
	LastResultUpdate   map[int]int
	UpdateStatusShards []int
}

type ApplyNotifyMsg struct {
	value string
	err   Err
	term  int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	// 1.indexChan 2. map:Id curConfig 3.
	curConfig shardmaster.Config
	// 1.分片移动的时候，也应该把这个移动过去，实现跨分片的客户端最多一次
	lastResult map[int]int
	kvs        map[string]string
	// 每个 index 对应的通道
	indexChan map[int]chan ApplyNotifyMsg
	// 为了主动和 master 索要配置
	mck         *shardmaster.Clerk
	shardStatus map[int]ShardStatus // 每个 shard 当前状态
	lastConfig  shardmaster.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cliId := args.CliId
	seqId := args.SeqId
	kv.mu.Lock()
	// 分片不在当前组上
	if kv.curConfig.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if lastId, ok := kv.lastResult[cliId]; ok && seqId < lastId {
		reply.Value = kv.kvs[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// 1.不是 目标 group 的处理
	op := Op{Key: args.Key, OpType: Get, ClId: cliId, SeqId: seqId}
	DPrintf("%v begin get,op:%v", kv.me, op)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
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
			return
		} else if res.err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
			return
		} else {
			log.Printf("get res:%v", res)
			reply.Err = OK
			reply.Value = res.value
		}
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		return
	}
	kv.mu.Lock()
	delete(kv.indexChan, index)
	//DPrintf("%v delete chan %v", kv.me, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cliId := args.CliId
	seqId := args.SeqId
	kv.mu.Lock()
	DPrintf("server %v begin putAppend, kv.curConfig:%v key2shard(args.Key):%v, kv.gid:%v",
		kv.me, kv.curConfig, key2shard(args.Key), kv.gid)
	// 分片不在当前组上
	if kv.curConfig.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if lastId, ok := kv.lastResult[cliId]; ok && lastId >= seqId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{Key: args.Key, Val: args.Value, OpType: OpType(args.Op), ClId: cliId, SeqId: seqId}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	log.Printf("%v begin putAppend,op:%v", kv.me, op)
	kv.indexChan[index] = make(chan ApplyNotifyMsg, 1)
	ch := kv.indexChan[index]
	kv.mu.Unlock()
	select {
	case res := <-ch:
		if res.term != term {
			reply.Err = ErrWrongLeader
			return
		} else if res.err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
			return
		} else {
			reply.Err = OK
		}
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		return
	}
	kv.mu.Lock()
	delete(kv.indexChan, index)
	//DPrintf("%v delete chan %v", kv.me, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	//debug = 0
}

//// MasterConfigHandler 处理 master 发来的配置,错误：应该是 server 问master 要，而不是 master 主动给 server
//func (kv *ShardKV) MasterConfigHandler(args *shardmaster.ConfigUpdateArgs, reply *shardmaster.ConfigUpdateReply) {
//	kv.mu.Lock()
//	argsConfig := args.Config
//	curConfig := kv.curConfig
//	kv.mu.Unlock()
//	// 落后于本地配置
//	if argsConfig.Num <= curConfig.Num {
//		return
//	}
//	index, term, isLeader := kv.rf.Start(Op{OpType: Config, Config: argsConfig})
//	if !isLeader {
//		reply.Err = ErrWrongLeader
//		return
//	}
//	kv.mu.Lock()
//	DPrintf("%v is leader index: %v", kv.me, index)
//	kv.indexChan[index] = make(chan ApplyNotifyMsg, 1)
//	ch := kv.indexChan[index]
//	kv.mu.Unlock()
//	select {
//	case res := <-ch:
//		if res.term != term {
//			reply.Err = ErrWrongLeader
//			return
//		} else {
//			//// 假如有新移入的 shard，让目标组的 leader 进行移动, 应该在 applyLoop 中触发 `AskMoveShardsRPC`，而不是在 handler
//			//kv.AskMoveShardsRPC(&curConfig, &argsConfig, curGid)
//		}
//	case <-time.After(RaftTimeout):
//		reply.Err = ErrTimeOut
//		return
//	}
//	kv.mu.Lock()
//	delete(kv.indexChan, index)
//	//DPrintf("%v delete chan %v", kv.me, index)
//	kv.mu.Unlock()
//}

//// AskMoveShardsRPC 问目标的ShardKV要这次配置变更新增的 shard
//func (kv *ShardKV) AskMoveShardsRPC(oldConfig *shardmaster.Config, newConfig *shardmaster.Config) {
//	DPrintf("%v begin AskMoveShardsRPC oldConfig:%v, newConfig:%v", kv.me, oldConfig, newConfig)
//	// 需要的 gid 到 shards 的集合
//	needGidToShardsMap := make(map[int][]int)
//	needAddKvs := make(map[string]string)
//	lastResultsUpdate := make(map[int]int)
//	curGid := kv.gid
//	for i := 0; i < shardmaster.NShards; i++ {
//		// 如果是移动到当前组的shard，就问原来组要
//		if newConfig.Shards[i] != oldConfig.Shards[i] && newConfig.Shards[i] == curGid {
//			oldGid := oldConfig.Shards[i]
//			needGidToShardsMap[oldGid] = append(needGidToShardsMap[oldGid], i)
//		}
//	}
//	// 代表没有需要移动到这里的
//	if len(needGidToShardsMap) == 0 {
//		kv.curConfig = *newConfig
//		return
//	}
//	var wg sync.WaitGroup
//	for k, v := range needGidToShardsMap {
//		// todo 可以用多线程并发请求多个目标 group
//		if servers, ok := oldConfig.Groups[k]; ok {
//			for _, server := range servers {
//				wg.Add(1)
//				srv := kv.make_end(server)
//				args := &AskMoveShardsArgs{v, newConfig.Num}
//				reply := &AskMoveShardsReply{}
//				ok := srv.Call("ShardKV.AskMoveShardsRPCHandler", &args, &reply)
//				// 这样设计有问题，如果向有的节点请求的shard失败了，一起更新就会缺失
//				if ok {
//					// 对组内所有 follower 执行新的 shard 的更新[集中批量的更新，把需要收到的所有 group 的 shard 的更新一次性发给 follower]
//					for k, v := range reply.Kvs {
//						needAddKvs[k] = v
//					}
//					lastResultsUpdate = reply.LastResult
//				}
//			}
//		}
//	}
//	// todo 失败重试, 异步
//	// 把需要收到的所有 group 的 shard 的更新一次性发给 follower
//	go func() {
//		kv.ShardsUpdateHandler(needAddKvs, map[string]string{}, lastResultsUpdate)
//	}()
//	DPrintf("%v config update,kv.curConfig:%v, newConfig:%v", kv.me, kv.curConfig, newConfig)
//	kv.curConfig = *newConfig
//	DPrintf("%v config update after,kv.curConfig:%v, newConfig:%v", kv.me, kv.curConfig, newConfig)
//}

func (kv *ShardKV) AskMoveShardsRPC() {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	kv.mu.Lock()
	// 需要拉取的shards， gid-》shard
	needGidToShardsMap := make(map[int][]int)
	for k, v := range kv.shardStatus {
		if v == Pulling {
			needGidToShardsMap[kv.lastConfig.Shards[k]] = append(needGidToShardsMap[kv.lastConfig.Shards[k]], k)
		}
	}
	//log.Printf("%v AskMoveShardsRPC needGidToShardsMap:%v", kv.me, needGidToShardsMap)
	var wg sync.WaitGroup
	for gid, shard := range needGidToShardsMap {
		wg.Add(1)
		go func(gid int, shard []int) {
			defer wg.Done()
			for _, server := range kv.curConfig.Groups[gid] {
				srv := kv.make_end(server)
				args := AskMoveShardsArgs{Shards: shard, ConfigNum: kv.curConfig.Num}
				reply := AskMoveShardsReply{}
				ok := srv.Call("ShardKV.AskMoveShardsRPCHandler", &args, &reply)
				if !ok || reply.Err == ErrWrongLeader {
					//log.Printf("%v AskMoveShardsRPC ErrWrongLeader:%v", kv.me, reply)
					continue
				}
				if reply.Err == OK {
					// 向follower就刚才更新内容达成一致
					if kv.gid == 101 {
						log.Printf("%v gid:%v AskMoveShardsRPC success:%v", kv.me, kv.gid, reply)
					}
					//
					kv.rf.Start(Op{OpType: ShardsPulling, ShardsUpdateKvs: reply.Kvs, LastResultUpdate: reply.LastResult, UpdateStatusShards: args.Shards})
					break
				}
			}
		}(gid, shard)
	}
	kv.mu.Unlock()
	wg.Wait()
}

// AskMoveShardsRPCHandler 要这次配置变更新增的 shard 的 rpc handler
func (kv *ShardKV) AskMoveShardsRPCHandler(args *AskMoveShardsArgs, reply *AskMoveShardsReply) {
	//log.Printf("%v begin AskMoveShardsRPCHandler, args:%v", kv.me, args)
	_, isLeader := kv.rf.GetState()
	shardsMap := make(map[int]bool)
	for _, shard := range args.Shards {
		shardsMap[shard] = true
	}
	kv.mu.Lock()
	//curConfigNum := kv.curConfig.Num
	// 有可能请求迁移的server的配置比当前新，说明当前落后，这个时候不可以迁移
	if !isLeader || args.ConfigNum != kv.curConfig.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// 为跨分片移动的客户端请求提供最多一次语义（重复检测）
	lastResult := make(map[int]int)
	for k, v := range kv.lastResult {
		lastResult[k] = v
	}
	reply.Err = OK
	reply.LastResult = lastResult
	reply.Kvs = KeysBelongingToShard(shardsMap, kv.kvs, kv.curConfig, kv.gid)
	//log.Printf("%v finish AskMoveShardsRPCHandler, reply:%v", kv.me, reply)
	kv.mu.Unlock()
	//// 发送shards成功以后删除当前的 shards
	//kv.ShardsUpdateHandler(map[string]string{}, reply.Kvs)
}

//	func KeysBelongingToShard(shards map[int]bool, kvs map[string]string) map[string]string {
//		result := make(map[string]string)
//		for key, val := range kvs {
//			if key == "0" {
//				log.Printf("key:0, value:%v", val)
//			}
//			if _, ok := shards[key2shard(key)]; ok {
//				result[key] = val
//			}
//		}
//		return result
//	}
func KeysBelongingToShard(shards map[int]bool, kvs map[string]string, config shardmaster.Config, gid int) map[string]string {
	log.Printf("gid %v KeysBelongingToShard, shards:%v, kvs:%v, config:%v", gid, shards, kvs, config)
	result := make(map[string]string)
	for key, val := range kvs {
		shard := key2shard(key)
		if shards[shard] {
			result[key] = val
		}
	}
	return result
}

// ShardsUpdateHandler leader对组内所有的 follower 进行 shards 的更新，例如新加入一些 shard 的kv， 删除一些 shard 的 kv[needDeleteKeys 不为空]
//func (kv *ShardKV) ShardsUpdateHandler(NeedAddKvs map[string]string, NeedDeleteKeys map[string]string, lastResultUpdate map[int]int) bool {
//	DPrintf("%v begin ShardsUpdateHandler, NeedAddKvs:%v", kv.me, NeedAddKvs)
//	isAdd := len(NeedAddKvs) > 0
//	index, term := -1, 0
//	if isAdd {
//		index, term, _ = kv.rf.Start(Op{OpType: ShardsPulling, ShardsUpdateKvs: NeedAddKvs, LastResultUpdate: lastResultUpdate})
//	}
//	//else {
//	//	index, term, _ = kv.rf.Start(Op{OpType: ShardsDelete, ShardsUpdateKvs: NeedDeleteKeys, LastResultUpdate: lastResultUpdate})
//	//}
//	kv.mu.Lock()
//	DPrintf("%v is leader index: %v", kv.me, index)
//	kv.indexChan[index] = make(chan ApplyNotifyMsg, 1)
//	ch := kv.indexChan[index]
//	kv.mu.Unlock()
//	select {
//	case res := <-ch:
//		if res.term != term {
//			return false
//		}
//	case <-time.After(RaftTimeout):
//		return false
//	}
//	kv.mu.Lock()
//	delete(kv.indexChan, index)
//	kv.mu.Unlock()
//	return true
//}

func (kv *ShardKV) applyOP(msg raft.ApplyMsg) {
	op, ok := msg.Command.(Op)
	DPrintf("%v apply receive msg:%v", kv.me, msg)
	if !ok {
		return
	}
	var notifyMsg ApplyNotifyMsg
	kv.mu.Lock()
	if op.OpType == Config {
		newConfig := shardmaster.Config{
			Num:    op.Config.Num,
			Shards: op.Config.Shards,       // 值拷贝数组
			Groups: make(map[int][]string), // 必须深拷贝 map
		}
		// 深拷贝 Groups（关键！避免修改影响原始 Config）
		for gid, servers := range op.Config.Groups {
			newConfig.Groups[gid] = append([]string{}, servers...)
		}
		log.Printf("%v gid %v newConfig.Num: %v, curConfig.Num:%v", kv.me, kv.gid, newConfig, kv.curConfig)
		if newConfig.Num > kv.curConfig.Num {
			// 深拷贝 curConfig 到 lastConfig
			kv.lastConfig = shardmaster.Config{
				Num:    kv.curConfig.Num,
				Shards: kv.curConfig.Shards,
				Groups: make(map[int][]string),
			}
			for gid, servers := range kv.curConfig.Groups {
				kv.lastConfig.Groups[gid] = append([]string{}, servers...)
			}
			kv.curConfig = newConfig
			log.Printf("config update.... old:%v, new:%v", kv.lastConfig, kv.curConfig)
			kv.becomePulling()
		}
		kv.mu.Unlock()
	} else if op.OpType == ShardsPulling {
		kv.doShardsPulling(op)
		kv.mu.Unlock()
	} else {
		ch, ex := kv.indexChan[msg.CommandIndex]
		lastId, ok := kv.lastResult[op.ClId]
		isNotUpdated := false
		isWrongGroup := false
		if kv.curConfig.Shards[key2shard(op.Key)] != kv.gid {
			isWrongGroup = true
			kv.mu.Unlock()
		} else {
			if ok && lastId >= op.SeqId {
				log.Printf("%v repeat msg:%v", kv.me, msg)
				if kv.shardStatus[key2shard(op.Key)] != Serving {
					isNotUpdated = true
				}
				if op.OpType == Get && kv.shardStatus[key2shard(op.Key)] == Serving {
					notifyMsg.value = kv.kvs[op.Key]
				}
				kv.mu.Unlock()
			} else {
				switch op.OpType {
				case Get:
					if kv.shardStatus[key2shard(op.Key)] != Serving {
						isNotUpdated = true
						log.Printf("%v gid:%v op.Key:%v Get isNotUpdated", kv.me, kv.gid, op.Key)
					} else {
						log.Printf("%v gid:%v op.Key:%v Get isUpdated %v", kv.me, kv.gid, op.Key, kv.shardStatus[key2shard(op.Key)])
						notifyMsg.value = kv.kvs[op.Key]
					}
				case Put:
					if kv.shardStatus[key2shard(op.Key)] != Serving {
						isNotUpdated = true
						log.Printf("%v gid:%v op.Key:%v Put isNotUpdated", kv.me, kv.gid, op.Key)
					} else {
						kv.kvs[op.Key] = op.Val
						log.Printf("%v gid:%v op.Key:%v Put success", kv.me, kv.gid, op.Key)
					}
				case Append:
					if kv.shardStatus[key2shard(op.Key)] != Serving {
						isNotUpdated = true
						log.Printf("%v gid:%v op.Key:%v Append isNotUpdated", kv.me, kv.gid, op.Key)
					} else {
						kv.kvs[op.Key] = kv.kvs[op.Key] + op.Val
					}
				}
				kv.lastResult[op.ClId] = op.SeqId
				kv.mu.Unlock()
			}
		}
		currentTerm, _ := kv.rf.GetState()
		if isNotUpdated {
			notifyMsg.err = ErrWrongLeader
		} else if isWrongGroup {
			notifyMsg.err = ErrWrongGroup
		} else {
			notifyMsg.err = OK
		}
		notifyMsg.term = currentTerm
		if ex {
			ch <- notifyMsg
		}
	}
	//if op.OpType != Config && op.OpType != ShardsPulling && (ok && lastId >= op.SeqId) {
	//	log.Printf("%v repeat msg:%v", kv.me, msg)
	//	if kv.shardStatus[key2shard(op.Key)] != Serving {
	//		isNotUpdated = true
	//	}
	//	if op.OpType == Get && kv.shardStatus[key2shard(op.Key)] == Serving {
	//		notifyMsg.value = kv.kvs[op.Key]
	//	}
	//	kv.mu.Unlock()
	//} else {
	//	switch op.OpType {
	//	case Get:
	//		if kv.shardStatus[key2shard(op.Key)] != Serving {
	//			isNotUpdated = true
	//			log.Printf("%v gid:%v op.Key:%v Get isNotUpdated", kv.me, kv.gid, op.Key)
	//		} else {
	//			log.Printf("%v gid:%v op.Key:%v Get isUpdated %v", kv.me, kv.gid, op.Key, kv.shardStatus[key2shard(op.Key)])
	//			notifyMsg.value = kv.kvs[op.Key]
	//		}
	//	case Put:
	//		if kv.shardStatus[key2shard(op.Key)] != Serving {
	//			isNotUpdated = true
	//			log.Printf("%v gid:%v op.Key:%v Put isNotUpdated", kv.me, kv.gid, op.Key)
	//		} else {
	//			kv.kvs[op.Key] = op.Val
	//			log.Printf("%v gid:%v op.Key:%v Put success", kv.me, kv.gid, op.Key)
	//		}
	//	case Append:
	//		if kv.shardStatus[key2shard(op.Key)] != Serving {
	//			isNotUpdated = true
	//			log.Printf("%v gid:%v op.Key:%v Append isNotUpdated", kv.me, kv.gid, op.Key)
	//		} else {
	//			kv.kvs[op.Key] = kv.kvs[op.Key] + op.Val
	//		}
	//	case Config:
	//		newConfig := shardmaster.Config{
	//			Num:    op.Config.Num,
	//			Shards: op.Config.Shards,       // 值拷贝数组
	//			Groups: make(map[int][]string), // 必须深拷贝 map
	//		}
	//
	//		// 深拷贝 Groups（关键！避免修改影响原始 Config）
	//		for gid, servers := range op.Config.Groups {
	//			newConfig.Groups[gid] = append([]string{}, servers...)
	//		}
	//
	//		log.Printf("%v gid %v newConfig.Num: %v, curConfig.Num:%v", kv.me, kv.gid, newConfig, kv.curConfig)
	//		if newConfig.Num > kv.curConfig.Num {
	//			// 深拷贝 curConfig 到 lastConfig
	//			kv.lastConfig = shardmaster.Config{
	//				Num:    kv.curConfig.Num,
	//				Shards: kv.curConfig.Shards,
	//				Groups: make(map[int][]string),
	//			}
	//			for gid, servers := range kv.curConfig.Groups {
	//				kv.lastConfig.Groups[gid] = append([]string{}, servers...)
	//			}
	//			log.Printf("config update.... old:%v, new:%v", kv.lastConfig, kv.curConfig)
	//			kv.curConfig = newConfig
	//			kv.becomePulling()
	//		}
	//	case ShardsPulling:
	//		kv.doShardsPulling(op)
	//		//case ShardsDelete:
	//		//	for k, _ := range op.ShardsUpdateKvs {
	//		//		delete(kv.kvs, k)
	//		//	}
	//	}
	//	kv.lastResult[op.ClId] = op.SeqId
	//	kv.mu.Unlock()
	//}
	//currentTerm, _ := kv.rf.GetState()
	//if isNotUpdated {
	//	notifyMsg.err = ErrWrongGroup
	//} else {
	//	notifyMsg.err = OK
	//}
	//notifyMsg.term = currentTerm
	//if ex {
	//	ch <- notifyMsg
	//}
}

func (kv *ShardKV) doShardsPulling(op Op) {
	for k, v := range op.ShardsUpdateKvs {
		kv.kvs[k] = v
		//shard := key2shard(k)
		//kv.shardStatus[shard] = Serving
		if kv.gid == 101 {
			log.Printf("doShardsPulling...key:%v, val:%v", k, v)
		}
	}
	for _, shard := range op.UpdateStatusShards {
		kv.shardStatus[shard] = Serving
		if kv.gid == 101 {
			log.Printf("%v gid:%v  shard %v become Serving", kv.me, kv.gid, shard)
		}
	}
	log.Printf("%v gid:%v after pulling:%v", kv.me, kv.gid, kv.kvs)
	for k, v := range op.LastResultUpdate {
		if kv.lastResult[k] < v {
			kv.lastResult[k] = v
		}
	}
	// 垃圾回收rpc
}

func (kv *ShardKV) readSnapShot(data []byte) {
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
	var LastConfig shardmaster.Config
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
	if d.Decode(&LastConfig) != nil {
		DPrintf("readSnapShot decode error LastConfig error....")
		return
	}

	kv.mu.Lock()
	//kv.rf.LastIncludedTerm = LastIncludedTerm
	//kv.rf.LastIncludedIndex = LastIncludedIndex
	kv.kvs = kvs
	kv.lastResult = lastResult
	kv.mu.Unlock()
	DPrintf("%v finish readSnapShot...., kv.kvs:%v", kv.me,
		kvs)
	//kv.mu.Lock()
	//defer kv.mu.Lock()
}

func (kv *ShardKV) checkAllShardServing() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, status := range kv.shardStatus {
		if status != Serving {
			return false
		}
	}
	return true
}

func (kv *ShardKV) becomePushing() {
	for shard, gid := range kv.curConfig.Shards {
		// 原来存在 现在没了
		if _, ok := kv.shardStatus[shard]; ok {
			if gid != kv.gid {
				kv.shardStatus[shard] = Pushing
			}
		}
	}
}

//	func (kv *ShardKV) becomePulling() {
//		kv.shardStatus = make(map[int]ShardStatus)
//		for shard, gid := range kv.curConfig.Shards {
//			if gid == kv.gid {
//				if kv.lastConfig.Shards[shard] != 0 && kv.lastConfig.Shards[shard] != kv.gid {
//					// 原来没有 现在有了
//					//log.Printf("%v become pulling key: %v", kv.gid, shard)
//					kv.shardStatus[shard] = Pulling
//				} else {
//					kv.shardStatus[shard] = Serving
//				}
//			}
//		}
//		//for k, v := range kv.shardStatus {
//		if kv.gid == 101 {
//			log.Printf("%v gid: %v, shardStatus:%v", kv.me, kv.gid, kv.shardStatus)
//			log.Printf("%v gid: %v, curConfig:%v", kv.me, kv.gid, kv.curConfig)
//		}
//	}
func (kv *ShardKV) becomePulling() {
	kv.shardStatus = make(map[int]ShardStatus)
	for shard := 0; shard < shardmaster.NShards; shard++ {
		curGid := kv.curConfig.Shards[shard]
		lastGid := kv.lastConfig.Shards[shard]
		if curGid == kv.gid {
			if lastGid != kv.gid && lastGid != 0 {
				// 原来属于别人，现在属于我 → Pulling
				kv.shardStatus[shard] = Pulling
				log.Printf("%v gid: %v, %v become pulling", kv.me, kv.gid, shard)
			} else {
				kv.shardStatus[shard] = Serving
			}
		}
	}

	if kv.gid == 101 {
		log.Printf("%v gid: %v, shardStatus:%v", kv.me, kv.gid, kv.shardStatus)
		log.Printf("%v gid: %v, curConfig:%v", kv.me, kv.gid, kv.curConfig)
	}
}

//
//func (kv *ShardKV) checkShardIsPulled(key string) {
//	if kv.shardStatus[key2shard()]
//}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.lastResult = make(map[int]int)
	kv.kvs = make(map[string]string)
	kv.indexChan = make(map[int]chan ApplyNotifyMsg)
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters) todo ?
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.curConfig = shardmaster.Config{}
	DPrintf("%v kv.curConfig:%v", kv.me, kv.curConfig)
	go func() {
		for msg := range kv.applyCh {
			if msg.CommandValid {
				kv.applyOP(msg)
				if maxraftstate != -1 && persister.RaftStateSize() > 0 &&
					float64(persister.RaftStateSize())/float64(maxraftstate) >= 0.9 {
					DPrintf("%v maxraftstate:%v,persister.RaftStateSize():%v", kv.me, float64(maxraftstate),
						float64(persister.RaftStateSize()))
					kv.rf.CutLogExceedMaxSizeAndSaveSnapShot(kv.kvs, kv.lastResult, msg.CommandIndex)
				}
			} else {
				kv.readSnapShot(msg.Command.([]byte))
			}
		}
	}()

	go func() {
		for {
			if _, isLeader := kv.rf.GetState(); isLeader {
				allServing := kv.checkAllShardServing()
				if allServing {
					// 1.所有组的服务器定时从 master 获取最新配置，不是 leader 也要获取，等到 leader 挂了，自身成为 leader
					newConfig := kv.mck.Query(-1)
					//DPrintf("push from master...%v", newConfig)
					kv.mu.Lock()
					if newConfig.Num <= kv.curConfig.Num {
						DPrintf("%v recieve err Config...kv.curConfig:%v, newConfig:%v", kv.me, kv.curConfig, newConfig)
						kv.mu.Unlock()
						continue
					}
					log.Printf("%v gid %v recieve newConfig...kv.curConfig:%v, newConfig:%v", kv.me, kv.gid, kv.curConfig, newConfig)
					kv.mu.Unlock()
					kv.rf.Start(Op{OpType: Config, Config: newConfig})
				}
			}
			// 2.apply后只有 leader 可以执行 config 中的变更
			time.Sleep(100 * time.Millisecond)
		}
	}()
	// 负责转移shard的协程
	go func() {
		for {
			kv.AskMoveShardsRPC()
			time.Sleep(50 * time.Millisecond)
		}
	}()
	kv.readSnapShot(persister.ReadSnapshot())
	return kv
}
