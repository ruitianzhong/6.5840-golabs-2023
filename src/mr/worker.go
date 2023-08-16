package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

type MRKey []KeyValue

func (a MRKey) Len() int           { return len(a) }
func (a MRKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a MRKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	var reply TaskReply
	ok := false
	if reply, ok = CallGetTask(TaskReply{}, true); !ok {
		return
	}
	for {
		switch reply.TaskType {
		case MAP:
			handleMap(reply, mapf)
		case REDUCE:
			handleReduce(reply, reducef)
		case NONE:
			return
		}

		reply, ok = CallGetTask(reply, false)
		if !ok {
			return
		}

	}

}

func handleMap(reply TaskReply, mapf func(string, string) []KeyValue) {
	filename := reply.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	reduce := make([][]KeyValue, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		reduce[i] = []KeyValue{}
	}
	for _, v := range kva {
		rid := ihash(v.Key) % reply.NReduce
		reduce[rid] = append(reduce[rid], v)
	}
	for i := 0; i < reply.NReduce; i++ {
		writeIntermediate(reply, reduce[i], i)
	}
}

func writeIntermediate(reply TaskReply, kva []KeyValue, reduceSeqNum int) {
	file, err := ioutil.TempFile("", "temp-map-*")
	if err != nil {
		log.Fatalf("cannot create temp file %v", err)
	}
	enc := json.NewEncoder(file)
	for _, v := range kva {
		enc.Encode(&v)
	}
	newName := fmt.Sprintf("mr-%v-%v", reply.MapSeqNumber, reduceSeqNum)
	err = os.Rename(file.Name(), newName)
	if err != nil {
		log.Fatalf("%v rename fail \n", newName)
	}

}

func readIntermediate(reply TaskReply) []KeyValue {
	kva := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, reply.ReduceSeqNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

func handleReduce(reply TaskReply, reducef func(string, []string) string) {
	kva := readIntermediate(reply)
	sort.Sort(MRKey(kva))

	file, err := ioutil.TempFile("", "temp-out-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

		i = j
	}
	oname := fmt.Sprintf("./mr-out-%v", reply.ReduceSeqNumber)
	os.Rename(file.Name(), oname)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	// if err == nil {
	// 	return true
	// }

	// fmt.Println(err)
	return err == nil
}

func CallGetTask(previousReply TaskReply, isInitial bool) (TaskReply, bool) {

	args := TaskArgs{}
	reply := TaskReply{}
	if isInitial {
		args.ArgsType = INIT
		ok := call("Coordinator.GetTask", &args, &reply)
		return reply, ok
	}
	args.ArgsType = DONE
	args.TaskType = previousReply.TaskType
	switch args.TaskType {
	case MAP:
		args.MapSeqNumber = previousReply.MapSeqNumber
	case REDUCE:
		args.ReduceSeqNumber = previousReply.ReduceSeqNumber
	}
	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}
