package kvraft

import (
	"../labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	// 最新的日志，防止重复访问，每个客户端递增
	lastLogId int
	mu        sync.Mutex
	cliId     int
}

// 随机选一个server
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.lastLogId = 0
	ck.cliId = int(nrand())
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//func (ck *Clerk) Get(key string) string {
//	// You will have to modify this function.
//	ck.mu.Lock()
//	args := &GetArgs{Key: key, Seq: ck.lastLogId, CliId: ck.cliId}
//	ck.lastLogId++
//	leaderId := ck.leaderId
//	ok := false
//	serverId := int64(-1)
//	ck.mu.Unlock()
//	for {
//		reply := &GetReply{}
//		if leaderId != -1 {
//			ok = ck.servers[leaderId].Call("KVServer.Get", args, reply)
//			DPrintf("cli, leader get,key:%v, val:%v", args.Key, reply.Value)
//		} else {
//			n := int64(len(ck.servers))
//			serverId = nrand() % n
//			ok = ck.servers[serverId].Call("KVServer.Get", args, reply)
//		}
//		if !ok {
//			continue
//		}
//		ck.mu.Lock()
//		if reply.Err == OK {
//			if serverId != -1 {
//				ck.leaderId = int(serverId)
//			}
//			ck.mu.Unlock()
//			return reply.Value
//		} else if reply.Err == ErrWrongLeader {
//			ck.leaderId = -1
//			DPrintf("cli, getWrongLeader")
//			ck.mu.Unlock()
//			continue
//		} else if reply.Err == ErrNoKey {
//			if serverId != -1 {
//				ck.leaderId = int(serverId)
//			}
//			DPrintf("cli, getErrNoKey")
//			ck.mu.Unlock()
//			return ""
//		} else {
//			ck.mu.Unlock()
//			continue
//		}
//	}
//}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	args := &GetArgs{Key: key, Seq: ck.lastLogId, CliId: ck.cliId}
	ck.lastLogId++
	serverId := ck.leaderId
	n := len(ck.servers)
	ck.mu.Unlock()

	for ; ; serverId = (serverId + 1) % n {
		reply := &GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			continue
		}
		ck.mu.Lock()
		ck.leaderId = serverId
		ck.mu.Unlock()
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
	//for {
	//	reply := &GetReply{}
	//	ok := false
	//
	//	if leaderId != -1 {
	//		ok = ck.servers[leaderId].Call("KVServer.Get", args, reply)
	//		serverId = leaderId
	//		DPrintf("cli, leader get, key:%v, val:%v", args.Key, reply.Value)
	//	} else {
	//		n := len(ck.servers)
	//		ok = ck.servers[serverId].Call("KVServer.Get", args, reply)
	//		serverId = (serverId + 1) % n
	//	}
	//
	//	if !ok {
	//		time.Sleep(10 * time.Millisecond)
	//		continue
	//	}
	//
	//	ck.mu.Lock()
	//	if reply.Err == OK {
	//		ck.leaderId = serverId
	//		ck.mu.Unlock()
	//		return reply.Value
	//	} else if reply.Err == ErrWrongLeader {
	//		ck.leaderId = -1
	//		ck.mu.Unlock()
	//		DPrintf("cli, getWrongLeader")
	//		time.Sleep(10 * time.Millisecond)
	//		continue
	//	} else if reply.Err == ErrNoKey {
	//		ck.leaderId = -1
	//		ck.mu.Unlock()
	//		DPrintf("cli, getErrNoKey")
	//		return ""
	//	} else {
	//		ck.leaderId = -1
	//		ck.mu.Unlock()
	//		time.Sleep(10 * time.Millisecond)
	//		continue
	//	}
	//}
}
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	args := &PutAppendArgs{Key: key, Value: value, Op: op, Seq: ck.lastLogId, CliId: ck.cliId}
	ck.lastLogId++
	serverId := ck.leaderId
	n := len(ck.servers)
	ck.mu.Unlock()
	for ; ; serverId = (serverId + 1) % n {
		reply := &PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			continue
		}
		ck.mu.Lock()
		ck.leaderId = serverId
		ck.mu.Unlock()
		return
	}
	//
	//	reply := &PutAppendReply{}
	//	ok := false
	//	if leaderId != -1 {
	//		ok = ck.servers[leaderId].Call("KVServer.PutAppend", args, reply)
	//		serverId = leaderId
	//		DPrintf("cli, leader PutAppend, key:%v", args.Key)
	//	} else {
	//		n := len(ck.servers)
	//		ok = ck.servers[serverId].Call("KVServer.PutAppend", args, reply)
	//		serverId = (serverId + 1) % n
	//	}
	//
	//	if !ok {
	//		time.Sleep(10 * time.Millisecond)
	//		continue
	//	}
	//
	//	ck.mu.Lock()
	//	if reply.Err == OK {
	//		ck.leaderId = serverId
	//		ck.mu.Unlock()
	//		return
	//	} else if reply.Err == ErrWrongLeader {
	//		ck.leaderId = -1
	//		ck.mu.Unlock()
	//		DPrintf("cli, PutAppendErrWrongLeader")
	//		time.Sleep(10 * time.Millisecond)
	//		continue
	//	} else {
	//		ck.leaderId = -1
	//		ck.mu.Unlock()
	//		time.Sleep(10 * time.Millisecond)
	//		continue
	//	}
	//}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//func (ck *Clerk) PutAppend(key string, value string, op string) {
//	// You will have to modify this function.
//	ck.mu.Lock()
//	args := &PutAppendArgs{Key: key, Value: value, Op: op, Seq: ck.lastLogId, CliId: ck.cliId}
//	ck.lastLogId++
//	leaderId := ck.leaderId
//	ok := false
//	serverId := int64(-1)
//	ck.mu.Unlock()
//	for {
//		reply := &PutAppendReply{}
//		if leaderId != -1 {
//			ok = ck.servers[leaderId].Call("KVServer.PutAppend", args, reply)
//			DPrintf("cli, leader PutAppend,key:%v", args.Key)
//		} else {
//			n := int64(len(ck.servers))
//			serverId = nrand() % n
//			ok = ck.servers[serverId].Call("KVServer.PutAppend", args, reply)
//		}
//		if !ok {
//			continue
//		}
//		ck.mu.Lock()
//		if reply.Err == OK {
//			if serverId != -1 {
//				ck.leaderId = int(serverId)
//			}
//			ck.mu.Unlock()
//			return
//		} else if reply.Err == ErrWrongLeader {
//			ck.leaderId = -1
//			ck.mu.Unlock()
//			DPrintf("cli, PutAppendErrWrongLeader")
//			continue
//		} else {
//			ck.mu.Unlock()
//			continue
//		}
//	}
//}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
