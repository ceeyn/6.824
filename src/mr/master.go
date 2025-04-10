package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	Timestamp int64
	// 对于map 是file名称， 对于 reduce是 任务 id
	MapContent string
	// 0 map
	MapId    int
	ReduceId int
}

var GlobalMaster *Master

type Master struct {
	// Your definitions here.
	// 要执行的文件列表
	fileNames []string
	nReduce   int
	// 未完成的任务,key mapid/ reduceId
	unFinishedMapTask    map[int]Task
	unFinishedReduceTask map[int]Task
	mapIsFinished        bool
	curReduce            int
	lock                 sync.Mutex
	cond                 sync.Cond
	XSize                int
	RetryReduceTask      []Task
	RetryMapTask         []Task
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskForMapTask(args *ExampleArgs, reply *TaskReply) error {
	//log.Printf("begin AskForMapTask: %v", m.fileNames)
	m.lock.Lock()
	defer m.lock.Unlock()
	// map 未完成
	// map 任务都下发完毕
	// 重新分配的任务
	reply.Status = 1
	if len(m.RetryMapTask) != 0 {
		task := m.RetryMapTask[len(m.RetryMapTask)-1]
		m.RetryMapTask = m.RetryMapTask[:len(m.RetryMapTask)-1]
		task.Timestamp = time.Now().UnixMilli()
		m.unFinishedMapTask[task.MapId] = task
		reply.Task = task
		reply.Status = 1
		log.Printf("after retryMapTask: %v", task)
		return nil
	}
	if len(m.fileNames) == 0 && len(m.unFinishedMapTask) != 0 {
		reply.Status = 0
		return nil
	} else if len(m.fileNames) == 0 && len(m.unFinishedMapTask) == 0 {
		reply.Status = -1
		return nil
	}
	n := 8
	curFileName := m.fileNames[len(m.fileNames)-1]
	m.fileNames = m.fileNames[:len(m.fileNames)-1]
	log.Printf("ask map fileName len: %v", len(m.fileNames))
	task := Task{Timestamp: time.Now().UnixMilli(),
		MapContent: curFileName, MapId: n - len(m.fileNames) - 1}
	m.unFinishedMapTask[task.MapId] = task
	reply.Task = task
	log.Printf("after AskForMapTask: %v", task)
	return nil
}

func (m *Master) AskForReduceTask(args *ExampleArgs, reply *TaskReply) error {
	//log.Printf("begin AskForReduceTask: %v", args)
	m.lock.Lock()
	defer m.lock.Unlock()
	reply.Status = 1
	// reduce任务只能在所有 map 结束后开始
	for len(m.fileNames) > 0 || (len(m.fileNames) == 0 && len(m.unFinishedMapTask) != 0) {
		m.cond.Wait()
	}
	// 重新分配的任务
	if len(m.RetryReduceTask) != 0 {
		task := m.RetryReduceTask[len(m.RetryReduceTask)-1]
		m.RetryReduceTask = m.RetryReduceTask[:len(m.RetryReduceTask)-1]
		task.Timestamp = time.Now().UnixMilli()
		m.unFinishedReduceTask[task.ReduceId] = task
		reply.Task = task
		log.Printf("after retryReduceTask: %v", task)
		return nil
	}
	// 某一次可能来的时候m.RetryReduceTask刚好没有放进去，这个时候就会出现错误,所以用二次检查
	if m.curReduce == m.nReduce && len(m.unFinishedReduceTask) != 0 {
		reply.Status = 0
		return nil
	} else if m.curReduce == m.nReduce && len(m.unFinishedReduceTask) == 0 {
		reply.Status = -1
		return nil
	}
	task := Task{Timestamp: time.Now().UnixMilli(),
		MapContent: strconv.Itoa(m.curReduce), ReduceId: m.curReduce}
	m.unFinishedReduceTask[task.ReduceId] = task
	m.curReduce++
	reply.Task = task
	log.Printf("after AskForReduceTask: %v", task)
	return nil
}

// MapWorkFinished 工作线程完成任务后通过此函数告知 master，从unFinishedTask []Task移除任务
func (m *Master) MapWorkFinished(args *ExampleArgs, reply *TaskReply) error {
	log.Printf("begin mapWorkFinished: %v", m.unFinishedMapTask)
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.unFinishedMapTask, args.X)
	if len(m.fileNames) == 0 && len(m.unFinishedMapTask) == 0 && len(m.RetryMapTask) == 0 {
		m.cond.Broadcast()
	}
	//log.Printf("after mapWorkFinished: %v", m.unFinishedMapTask)
	return nil
}

// ReduceWorkFinished 工作线程完成任务后通过此函数告知 master，从unFinishedTask []Task移除任务
func (m *Master) ReduceWorkFinished(args *ExampleArgs, reply *TaskReply) error {
	log.Printf("begin reduceWorkFinished: %v", m.unFinishedReduceTask)
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.unFinishedReduceTask, args.X)
	//log.Printf("after reduceWorkFinished: %v", m.unFinishedReduceTask)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	log.Printf("begin Done(): %v, %v, %v", m.fileNames, m.unFinishedMapTask, m.unFinishedReduceTask)
	log.Printf("begin Done retry: %v, %v", m.RetryMapTask, m.RetryReduceTask)
	ret := false
	// Your code here.
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.fileNames) == 0 && len(m.unFinishedReduceTask) == 0 &&
		len(m.unFinishedMapTask) == 0 && len(m.RetryMapTask) == 0 && len(m.RetryReduceTask) == 0 {
		log.Printf("所有都完成了")
		return true
	}
	if len(m.fileNames) == 0 && len(m.unFinishedMapTask) == 0 && len(m.RetryMapTask) == 0 {
		// reduce阶段
		for k, task := range m.unFinishedReduceTask {
			if time.Now().UnixMilli()-task.Timestamp >= 10_000 {
				delete(m.unFinishedReduceTask, k)
				task = Task{Timestamp: 0, MapContent: task.MapContent, MapId: 0, ReduceId: task.ReduceId}
				// 重跑这个任务
				m.RetryReduceTask = append(m.RetryReduceTask, task)
				log.Printf("重跑 reduceTask: %v", task)
			}
		}
		if len(m.unFinishedReduceTask) == 0 && len(m.RetryReduceTask) == 0 {
			log.Printf("reduce 阶段所有都完成了")
			return true
		}
	} else {
		// map 阶段
		for k, task := range m.unFinishedMapTask {
			if time.Now().UnixMilli()-task.Timestamp >= 10_000 {
				delete(m.unFinishedMapTask, k)
				task = Task{Timestamp: 0, MapContent: task.MapContent, MapId: task.MapId, ReduceId: 0}
				// 重跑这个任务
				m.RetryMapTask = append(m.RetryMapTask, task)
				log.Printf("重跑 RetryMapTask: %v", task)
			}
		}
		if len(m.fileNames) == 0 && len(m.unFinishedMapTask) == 0 && len(m.RetryMapTask) == 0 {
			// m.cond.BroadCast()
			m.cond.Broadcast()
		}
	}
	return ret
}

// todo 心跳链接

// MakeMaster create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := &Master{
		fileNames:            files,
		nReduce:              nReduce,
		unFinishedMapTask:    make(map[int]Task),
		unFinishedReduceTask: make(map[int]Task),
		mapIsFinished:        false,
		curReduce:            0,
		XSize:                len(files),
		RetryReduceTask:      make([]Task, 0),
		RetryMapTask:         make([]Task, 0),
	}
	log.Printf("master: %v", m)
	m.cond = *sync.NewCond(&m.lock)
	GlobalMaster = m
	// Your code here.
	m.server()
	return m
}
