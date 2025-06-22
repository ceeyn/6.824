package shardkv

//
// Sharded Key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CliId int
	SeqId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// CliId
	CliId int
	SeqId int
}

type GetReply struct {
	Err   Err
	Value string
}

// AskMoveShardsArgs 问目标 shardKV 要需要的 shards, 1.需要的 shards列表
type AskMoveShardsArgs struct {
	Shards    []int
	ConfigNum int
}

// AskMoveShardsReply 回复相应的 KVs，
type AskMoveShardsReply struct {
	//Ok Err
	// 如果您的某个 RPC 处理程序在其回复中包含一个属于服务器状态的映射（例如，键/值映射），则可能会因竞争而出现错误。RPC 系统必须读取该映射
	//才能将其发送给调用者，但它并未持有覆盖该映射的锁。然而，您的服务器可能会在 RPC 系统读取该映射时继续修改它。解决方案是让 RPC 处理程序
	//在回复中包含该映射的副本。
	Kvs        map[string]string
	LastResult map[int]int
	Err        Err
}
