package shardmaster

import (
	"../raft"
	"bytes"

	//"lab/src/shardkv"
	"log"
	"sort"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs    []Config // indexed by config Num
	lastResult map[int]int
	indexChan  map[int]chan ApplyNotifyMsg
	make_end   func(string) *labrpc.ClientEnd
}
type ApplyNotifyMsg struct {
	value Config
	err   Err
	term  int
}

type OpType string

const RaftTimeout = 500 * time.Millisecond

const (
	Join  OpType = "Join"
	Leave OpType = "Leave"
	Move  OpType = "Move"
	Query OpType = "Query"
)

type Op struct {
	// Your data here.
	OpType OpType
	//Config Config
	SeqId         int
	CliId         int
	Num           int // 方便 query 查询
	JoinArgs      map[int][]string
	LeaveArgs     []int
	MoveArgsShard int
	MoveArgsGID   int
}

// 1。总共10 个 5， 5 新增一个组 平均 3 个  4 3 3  1 2
// 1. 排序 gid， 2. 找出每个的 tarCount 3. 找出 exceedShard 4. 对于不足的gid 分配
func (sm *ShardMaster) rebalanced(oldConfig *Config) Config {
	StructDPrintf("%v before rebalanced...%v", sm.me, oldConfig)
	var gIds []int
	// 变动后的 config 不存在任何 server
	if len(oldConfig.Groups) == 0 {
		for k, _ := range oldConfig.Shards {
			oldConfig.Shards[k] = 0
		}
		return *oldConfig
	}
	for k, _ := range oldConfig.Groups {
		gIds = append(gIds, k)
	}
	sort.Ints(gIds)
	tarCount := make(map[int]int)
	avgCount := NShards / len(gIds)
	extra := NShards % len(gIds)
	for i, gId := range gIds {
		tarCount[gId] = avgCount
		if i < extra {
			tarCount[gId]++
		}
	}
	// tarCount 4 3 3
	shardCountEveryGroup := make(map[int]int)
	exceedShards := []int{}
	for shard, gId := range oldConfig.Shards {
		shardCountEveryGroup[gId]++
		if shardCountEveryGroup[gId] > tarCount[gId] {
			exceedShards = append(exceedShards, shard)
		}
	}
	// 5, 5   1 + 2
	//
	for _, gId := range gIds {
		for shardCountEveryGroup[gId] < tarCount[gId] {
			shard := exceedShards[len(exceedShards)-1]
			exceedShards = exceedShards[:len(exceedShards)-1]
			oldConfig.Shards[shard] = gId
			shardCountEveryGroup[gId]++
		}
	}
	StructDPrintf("%v after rebalanced...%v", sm.me, oldConfig)
	return *oldConfig
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()
	if lastId, ok := sm.lastResult[args.CliId]; ok && lastId >= args.SeqId {
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	//prevConfig := sm.configs[len(sm.configs)-1]
	//newConfig := Config{
	//	Num:    prevConfig.Num + 1,
	//	Groups: make(map[int][]string),
	//	Shards: prevConfig.Shards, // shards 可以先整体复制，稍后rebalance
	//}
	//// 深拷贝旧的 Groups
	//for gid, servers := range prevConfig.Groups {
	//	newConfig.Groups[gid] = append([]string{}, servers...)
	//}
	//sm.mu.Unlock()
	//// 加入新Servers
	//for gid, servers := range args.Servers {
	//	newConfig.Groups[gid] = append([]string{}, servers...)
	//}
	//newConfig = sm.rebalanced(&newConfig)
	//check1(&newConfig, &sm.configs[len(sm.configs)-1], args)
	//DPrintf("%v join newConfig:%v", sm.me, newConfig)
	op := Op{OpType: Join, CliId: args.CliId, SeqId: args.SeqId, JoinArgs: args.Servers}
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		DPrintf("join WrongLeader")
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	sm.indexChan[index] = make(chan ApplyNotifyMsg, 1)
	ch := sm.indexChan[index]
	sm.mu.Unlock()
	select {
	case res := <-ch:
		if res.term != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return
		} else {
			reply.Err = OK
		}
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		return
	}
	sm.mu.Lock()
	delete(sm.indexChan, index)
	//DPrintf("%v delete chan %v", kv.me, index)
	sm.mu.Unlock()
}

func check1(newConfig *Config, oldConfig *Config, args *JoinArgs) {
	// 1. 检查配置编号是否自增
	if newConfig.Num != oldConfig.Num+1 {
		log.Fatalf("[CHECK FAIL] Config.Num should increment: old=%v, new=%v", oldConfig.Num, newConfig.Num)
	}

	// 2. 检查旧的 Groups 是否完整保留
	for gid, oldServers := range oldConfig.Groups {
		newServers, ok := newConfig.Groups[gid]
		if !ok {
			log.Fatalf("[CHECK FAIL] GID %v missing in newConfig.Groups", gid)
		}
		if !equalStringSlice(oldServers, newServers) {
			log.Fatalf("[CHECK FAIL] Servers mismatch for GID %v: old=%v, new=%v", gid, oldServers, newServers)
		}
	}

	// 3. 检查 args.Servers 中的 GID 是否被正确加入
	for gid, argServers := range args.Servers {
		newServers, ok := newConfig.Groups[gid]
		if !ok {
			log.Fatalf("[CHECK FAIL] GID %v from args not found in newConfig.Groups", gid)
		}
		if !equalStringSlice(argServers, newServers) {
			log.Fatalf("[CHECK FAIL] Servers mismatch for GID %v from args: args=%v, new=%v", gid, argServers, newServers)
		}
	}

	// 4. 检查 shards 长度是否有效
	if len(newConfig.Shards) != NShards {
		log.Fatalf("[CHECK FAIL] Shards length mismatch, expected %v, got %v", NShards, len(newConfig.Shards))
	}
	for _, gid := range newConfig.Shards {
		if gid != 0 && newConfig.Groups[gid] == nil {
			log.Fatalf("[CHECK FAIL] Shard assigned to GID %v not found in Groups", gid)
		}
	}

	log.Printf("[CHECK OK] Config validated successfully")
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()
	if lastId, ok := sm.lastResult[args.CliId]; ok && lastId >= args.SeqId {
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	//prevConfig := sm.configs[len(sm.configs)-1]
	//StructDPrintf("leave...prevConfig:%v", prevConfig)
	//newConfig := Config{
	//	Num:    prevConfig.Num + 1,
	//	Groups: make(map[int][]string),
	//	Shards: prevConfig.Shards, // shards 可以先整体复制，稍后rebalance
	//}
	//// 深拷贝旧的 Groups
	//for gid, servers := range prevConfig.Groups {
	//	newConfig.Groups[gid] = append([]string{}, servers...)
	//}
	//// 如果在获取完prevConfig后就进行解锁，对prevConfig的结构体等属性访问的时候其实就是对sm.configs[len(sm.configs)-1]的属性进行访问
	//// 会出现并发问题，如果深拷贝出来则不会出现
	//sm.mu.Unlock()
	//for _, gid := range args.GIDs {
	//	delete(newConfig.Groups, gid)
	//}
	//newConfig = sm.rebalanced(&newConfig)
	//checkLeave(&newConfig, &sm.configs[len(sm.configs)-1], args)
	op := Op{OpType: Leave, CliId: args.CliId, SeqId: args.SeqId, LeaveArgs: args.GIDs}
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	sm.indexChan[index] = make(chan ApplyNotifyMsg, 1)
	ch := sm.indexChan[index]
	sm.mu.Unlock()
	select {
	case res := <-ch:
		if res.term != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return
		} else {
			reply.Err = OK
		}
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		return
	}
	sm.mu.Lock()
	delete(sm.indexChan, index)
	//DPrintf("%v delete chan %v", kv.me, index)
	sm.mu.Unlock()
}

func checkLeave(newConfig *Config, oldConfig *Config, args *LeaveArgs) {
	// 检查编号递增
	if newConfig.Num != oldConfig.Num+1 {
		log.Fatalf("[CHECK FAIL][Leave] Config.Num should increment: old=%v, new=%v", oldConfig.Num, newConfig.Num)
	}

	// 创建一个 map 快速检查被删除的 GIDs
	leaveGIDs := make(map[int]bool)
	for _, gid := range args.GIDs {
		leaveGIDs[gid] = true
	}

	// 检查被删除的 gid 已经从 newConfig.Groups 中消失
	for _, gid := range args.GIDs {
		if _, exists := newConfig.Groups[gid]; exists {
			log.Fatalf("[CHECK FAIL][Leave] GID %v should have been deleted, but still exists in newConfig.Groups", gid)
		}
	}

	// 检查其他 gid 未受影响，且 servers 保持一致
	for gid, servers := range oldConfig.Groups {
		if leaveGIDs[gid] {
			continue // 跳过被删除的 gid
		}
		newServers, exists := newConfig.Groups[gid]
		if !exists {
			log.Fatalf("[CHECK FAIL][Leave] Existing GID %v is missing in newConfig.Groups", gid)
		}
		if !equalStringSlice(servers, newServers) {
			log.Fatalf("[CHECK FAIL][Leave] Servers mismatch for GID %v: old=%v, new=%v", gid, servers, newServers)
		}
	}

	// 检查 shards 的合法性
	if len(newConfig.Shards) != NShards {
		log.Fatalf("[CHECK FAIL][Leave] Shards length mismatch, expected %v, got %v", NShards, len(newConfig.Shards))
	}

	for shard, gid := range newConfig.Shards {
		if gid != 0 && newConfig.Groups[gid] == nil {
			log.Fatalf("[CHECK FAIL][Leave] Shard %v assigned to non-existent GID %v", shard, gid)
		}
		if leaveGIDs[gid] {
			log.Fatalf("[CHECK FAIL][Leave] Shard %v still assigned to deleted GID %v", shard, gid)
		}
	}

	log.Printf("[CHECK OK][Leave] Config validated successfully")
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	if lastId, ok := sm.lastResult[args.CliId]; ok && lastId >= args.SeqId {
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	//prevConfig := sm.configs[len(sm.configs)-1]
	//newConfig := Config{
	//	Num:    prevConfig.Num + 1,
	//	Groups: make(map[int][]string),
	//	Shards: prevConfig.Shards, // shards 可以先整体复制，稍后rebalance
	//}
	//// 深拷贝旧的 Groups
	//for gid, servers := range prevConfig.Groups {
	//	newConfig.Groups[gid] = append([]string{}, servers...)
	//}
	//sm.mu.Unlock()
	//newConfig.Shards[args.Shard] = args.GID
	//newConfig = sm.rebalanced(&newConfig)
	op := Op{OpType: Move, CliId: args.CliId, SeqId: args.SeqId, MoveArgsGID: args.GID, MoveArgsShard: args.Shard}
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	sm.indexChan[index] = make(chan ApplyNotifyMsg, 1)
	ch := sm.indexChan[index]
	sm.mu.Unlock()
	select {
	case res := <-ch:
		if res.term != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return
		} else {
			reply.Err = OK
		}
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		return
	}
	sm.mu.Lock()
	delete(sm.indexChan, index)
	//DPrintf("%v delete chan %v", kv.me, index)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	//if lastId, ok := sm.lastResult[args.CliId]; ok && lastId >= args.SeqId {
	//	return
	//}
	DPrintf("begin server Query, args: %v", args)
	op := Op{OpType: Query, CliId: args.CliId, SeqId: args.SeqId, Num: args.Num}
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	sm.indexChan[index] = make(chan ApplyNotifyMsg, 1)
	ch := sm.indexChan[index]
	sm.mu.Unlock()
	select {
	case res := <-ch:
		if res.term != term {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return
		} else {
			reply.Err = OK
			reply.Config = res.value
		}
	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeOut
		return
	}
	sm.mu.Lock()
	delete(sm.indexChan, index)
	//DPrintf("%v delete chan %v", kv.me, index)
	sm.mu.Unlock()
	DPrintf("finish server Query, reply: %v", reply)
}

//// SendConfigRPC 发送config 到目标 KV 在 MIT 6.824 的 Lab 4 中，ShardMaster 并不会主动将新配置推送给所有的 ShardKV 服务器。
//相反，每个 ShardKV 服务器会定期向 ShardMaster 查询最新的配置，并根据需要进行相应的调整
//func (sm *ShardMaster) SendConfigRPC(args *ConfigUpdateArgs, reply *ConfigUpdateReply) {
//	for _, servers := range args.Config.Groups {
//		for _, server := range servers {
//			srv := sm.make_end(server)
//			reply = &ConfigUpdateReply{} // 每次都新建一个reply
//			srv.Call("ShardKV.MasterConfigHandler", args, reply)
//		}
//	}
//}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.

}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf

}

func (sm *ShardMaster) apply(msg raft.ApplyMsg) {
	op, ok := msg.Command.(Op)
	if !ok {
		log.Printf("转换出错, 内容：%v", msg)
	}
	var notifyMsg ApplyNotifyMsg
	sm.mu.Lock()
	ch, ex := sm.indexChan[msg.CommandIndex]
	if lastId, ok := sm.lastResult[op.CliId]; ok && lastId >= op.SeqId {
		if op.OpType == Query {
			queryId := 0
			if op.Num > sm.configs[len(sm.configs)-1].Num || op.Num == -1 {
				queryId = sm.configs[len(sm.configs)-1].Num
			} else {
				queryId = op.Num
			}
			notifyMsg.value = sm.configs[queryId]
		}
		sm.mu.Unlock()
	} else {
		switch op.OpType {
		case Join:
			//DPrintf("before join apply sm.configs:%v", sm.configs)
			DPrintf("%v before join apply sm.configs:%v", sm.me, len(sm.configs))
			//sm.configs = append(sm.configs, op.Config)
			sm.doJoin(op.JoinArgs)
			//DPrintf("%v after join apply sm.configs:%v, groups:%v", sm.me, len(sm.configs), len(op.Config.Groups))
			//args := &ConfigUpdateArgs{Config: op.Config}
			//reply := &ConfigUpdateReply{}
			////sm.SendConfigRPC(args, reply)
		case Leave:
			DPrintf("%v before leave apply sm.configs:%v", sm.me, len(sm.configs))
			//sm.configs = append(sm.configs, op.Config)
			sm.doLeave(op.LeaveArgs)
			DPrintf("%v after leave apply sm.configs:%v", sm.me, len(sm.configs))
			//args := &ConfigUpdateArgs{Config: op.Config}
			//reply := &ConfigUpdateReply{}
			//sm.SendConfigRPC(args, reply)
		case Move:
			DPrintf("%v before move apply sm.configs:%v", sm.me, len(sm.configs))
			//sm.configs = append(sm.configs, op.Config)
			sm.doMove(op.MoveArgsGID, op.MoveArgsShard)
			DPrintf("%v after move apply sm.configs:%v", sm.me, len(sm.configs))
			//args := &ConfigUpdateArgs{Config: op.Config}
			//reply := &ConfigUpdateReply{}
			//sm.SendConfigRPC(args, reply)
		case Query:
			sm.doQuery(op, &notifyMsg)
		}
		sm.lastResult[op.CliId] = op.SeqId
		sm.mu.Unlock()
	}
	notifyMsg.err = OK
	currentTerm, _ := sm.rf.GetState()
	notifyMsg.term = currentTerm
	if ex {
		ch <- notifyMsg
	}
}

func (sm *ShardMaster) doQuery(op Op, notifyMsg *ApplyNotifyMsg) {
	queryId := 0
	if op.Num > sm.configs[len(sm.configs)-1].Num || op.Num == -1 {
		queryId = sm.configs[len(sm.configs)-1].Num
	} else {
		queryId = op.Num
	}
	notifyMsg.value = sm.configs[queryId]
}

func (sm *ShardMaster) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var configs []Config
	// 如果不保存这个，就会出现刚开始日志提交了两个重复的 append，假如 server 挂了重新恢复的时候，维护的每个 cli最后一个的值没了，
	// 这个时候重新执行就会执行成功。
	var lastResult map[int]int
	var LastIncludedIndex int
	var LastIncludedTerm int

	if err := d.Decode(&LastIncludedIndex); err != nil {
		return
	}

	if err := d.Decode(&LastIncludedTerm); err != nil {
		return
	}

	if d.Decode(&configs) != nil {
		return
	}
	if d.Decode(&lastResult) != nil {
		return
	}

	sm.mu.Lock()
	log.Printf("acuqire s.readSnapShot success....")
	//kv.rf.LastIncludedTerm = LastIncludedTerm
	//kv.rf.LastIncludedIndex = LastIncludedIndex
	sm.configs = configs
	sm.lastResult = lastResult
	sm.mu.Unlock()
	log.Printf("realse s.readSnapShot success....")
}

func (sm *ShardMaster) doJoin(args map[int][]string) {
	prevConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    prevConfig.Num + 1,
		Groups: make(map[int][]string),
		Shards: prevConfig.Shards, // shards 可以先整体复制，稍后rebalance
	}
	// 深拷贝旧的 Groups
	for gid, servers := range prevConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	// 加入新Servers
	for gid, servers := range args {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	newConfig = sm.rebalanced(&newConfig)
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) doLeave(args []int) {
	prevConfig := sm.configs[len(sm.configs)-1]
	StructDPrintf("leave...prevConfig:%v", prevConfig)
	newConfig := Config{
		Num:    prevConfig.Num + 1,
		Groups: make(map[int][]string),
		Shards: prevConfig.Shards, // shards 可以先整体复制，稍后rebalance
	}
	// 深拷贝旧的 Groups
	for gid, servers := range prevConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	// 如果在获取完prevConfig后就进行解锁，对prevConfig的结构体等属性访问的时候其实就是对sm.configs[len(sm.configs)-1]的属性进行访问
	// 会出现并发问题，如果深拷贝出来则不会出现
	for _, gid := range args {
		delete(newConfig.Groups, gid)
	}
	newConfig = sm.rebalanced(&newConfig)
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) doMove(gid int, shard int) {
	prevConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    prevConfig.Num + 1,
		Groups: make(map[int][]string),
		Shards: prevConfig.Shards, // shards 可以先整体复制，稍后rebalance
	}
	// 深拷贝旧的 Groups
	for gid, servers := range prevConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	newConfig.Shards[shard] = gid
	newConfig = sm.rebalanced(&newConfig)
	sm.configs = append(sm.configs, newConfig)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.configs[0].Num = 0
	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.indexChan = make(map[int]chan ApplyNotifyMsg)
	sm.lastResult = make(map[int]int)
	// Your code here.
	go func() {
		for {
			for msg := range sm.applyCh {
				if msg.CommandValid {
					sm.apply(msg)
				}
			}
		}
	}()

	return sm
}
