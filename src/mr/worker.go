package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)

import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	//log.Printf("woker begin call")
	for {
		r1 := CallMap(mapf)
		if r1 == -1 {
			// time.Sleep(time.Second)
			// r1 = CallReduce(reducef)
			if r1 == -1 {
				break
			}
		}
	}

	log.Printf("worker map finish......")

	for {
		r2 := CallReduce(reducef)
		if r2 == -1 {
			//time.Sleep(time.Second)
			//r2 = CallReduce(reducef)
			if r2 == -1 {
				break
			}
		}
	}

	log.Printf("worker finish......")

}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallMap(mapf func(string, string) []KeyValue) int {
	reply := TaskReply{}
	args := ExampleArgs{}
	call("Master.AskForMapTask", &args, &reply)
	if reply.Status == -1 {
		return -1
	} else if reply.Status == 0 {
		//time.Sleep(5 * time.Second)
		return 0
	}
	log.Printf("callMap: %v", reply)
	task := reply.Task
	fileName := task.MapContent
	x := task.MapId
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kvs := mapf(fileName, string(content))
	// 排序后写入本地
	Shuffle(kvs, x)
	args.X = task.MapId
	call("Master.MapWorkFinished", &args, &reply)
	return 0
}

func Shuffle(kvs []KeyValue, x int) {
	log.Printf("begin Shuffle: %v", x)
	if len(kvs) == 0 {
		return
	}
	sort.Sort(ByKey(kvs))

	buckets := make(map[int][]KeyValue)
	for _, kv := range kvs {
		y := ihash(kv.Key) % 10
		buckets[y] = append(buckets[y], kv)
	}
	// 改成 range map 就错了
	for y := 0; y < 10; y++ {
		// 1. 写临时文件
		tempName := fmt.Sprintf("mr-%d-%d-tmp", x, y)
		file, err := os.Create(tempName)
		if err != nil {
			log.Fatalf("创建临时文件失败: %v", err)
		}
		enc := json.NewEncoder(file)
		for _, kv := range buckets[y] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("写入临时文件失败: %v", err)
			}
		}
		file.Close()

		// 2. 重命名为正式文件
		finalName := fmt.Sprintf("mr-%d%d", x, y)
		err = os.Rename(tempName, finalName)
		if err != nil {
			log.Fatalf("重命名文件失败: %v", err)
		}
		log.Printf("写入中间文件: %s", finalName)
	}
}
func CallReduce(reducef func(string, []string) string) int {
	reply := TaskReply{}
	args := ExampleArgs{}
	//args1 := ExampleArgs{}
	//reply1 := ExampleReply{}
	call("Master.AskForReduceTask", &args, &reply)
	if reply.Status == -1 {
		return -1
	} else if reply.Status == 0 {
		//time.Sleep(5 * time.Second)
		return 0
	}
	log.Printf("callReduce: %v", reply)
	task := reply.Task
	y := task.ReduceId
	//if GlobalMaster == nil {
	//	log.Fatal("GlobalMaster is not initialized!")
	//}
	xSize := 8
	fmt.Println("XSize:", xSize)
	resList := []KeyValue{}
	log.Printf("XSize: %v", xSize)
	// 找到每个 y 文件， 拼接在一起
	for i := 0; i < xSize; i++ {
		fileName := "mr" + "-" + strconv.Itoa(i) + strconv.Itoa(y)
		log.Printf("fileName: %v", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file) // 创建解码器绑定文件流
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil { // 尝试解码单个对象
				break // 遇到错误终止循环
			}
			resList = append(resList, kv) // 将解码结果存入切片
		}
	}
	// 排序后 按照 k 分割，输入到 reduce
	sort.Sort(ByKey(resList))
	//tempFile, _ := os.Create("tempSort1")
	//defer tempFile.Close()
	//for _, kv := range resList {
	//	fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value)
	//}
	i := 0
	ofile, _ := os.Create("mr-out-" + strconv.Itoa(y))
	//values := []string{}
	//for i < len(resList) {
	//	// todo 心跳链接
	//	//args1.X = 0
	//	//reply1.Y = 0
	//	//call("Master.Example", &args1, &reply1)
	//	//if reply1.Y == 0 {
	//	//	log.Printf("心跳断开连接。。。。")
	//	//	break
	//	//}
	//	j := i
	//	for j < len(resList) && resList[j].Key == resList[i].Key {
	//		values = append(values, resList[j].Value)
	//		j++
	//	}
	//	output := reducef(resList[i].Key, values)
	//	time.Sleep(time.Second)
	//	//log.Printf("key: %v, val: %v", resList[i].Key, output)
	//	// 输出到最终输出文件中 y
	//	fmt.Fprintf(ofile, "%v %v\n", resList[i].Key, output)
	//	// 下一个 key
	//	if j < len(resList) {
	//		i = j
	//		//log.Printf("i: %v", i)
	//		values = []string{}
	//	} else {
	//		break
	//	}
	//	//log.Printf("死循环了。。。。 i:%v, j:%v， size:%v", i, j, len(resList))
	//}
	for i < len(resList) {
		j := i + 1
		for j < len(resList) && resList[j].Key == resList[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, resList[k].Value)
		}
		output := reducef(resList[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", resList[i].Key, output)

		i = j
	}
	ofile.Close()
	//err := os.Rename("mr-out-tmp"+strconv.Itoa(y), "mr-out-"+strconv.Itoa(y))
	//if err != nil {
	//	log.Fatalf("重命名文件失败: %v", err)
	//}
	args.X = task.ReduceId
	log.Printf("callReduceWorkFinished: %v", args.X)
	call("Master.ReduceWorkFinished", &args, &reply)
	return 0
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
