package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nMap, nReduce, nMapDone, nReduceDone int
	mapState, reduceState                []TaskState
	lock                                 sync.Locker
	files                                []string
}
type TaskState struct {
	allocated, done bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	handleDone(c, args)
	for {
		c.lock.Lock()
		var taskType TaskType
		pos := 0
		ok := false
		if c.nMapDone < c.nMap {
			ok = findMapTask(c, reply)
			taskType = MAP
			pos = reply.MapSeqNumber
		} else if c.nReduceDone < c.nReduce {
			ok = findReduceTask(c, reply)
			taskType = REDUCE
			pos = reply.ReduceSeqNumber
		} else {
			taskType = NONE
		}
		c.lock.Unlock()
		if ok {
			go setTimeOut(c, taskType, pos)
			return nil
		}

	}

}

func setTimeOut(c *Coordinator, t TaskType, pos int) {
	time.Sleep(10 * time.Second)
	switch t {
	case MAP:
		c.lock.Lock()
		if !c.mapState[pos].done && c.mapState[pos].allocated {
			c.mapState[pos].allocated = false
		}
		c.lock.Unlock()
	case REDUCE:
		c.lock.Lock()
		if !c.reduceState[pos].done && c.reduceState[pos].allocated {
			c.reduceState[pos].allocated = false
		}
		c.lock.Unlock()
	default:
		return
	}
}

func handleDone(c *Coordinator, args *TaskArgs) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.ArgsType == DONE {

		switch args.TaskType {
		case MAP:
			if !c.mapState[args.MapSeqNumber].done {
				c.nMapDone += 1
				c.mapState[args.MapSeqNumber].done = true
			}
		case REDUCE:
			if !c.reduceState[args.ReduceSeqNumber].done {
				c.nReduceDone += 1
				c.reduceState[args.ReduceSeqNumber].done = true
			}
		}

	}
}

func findReduceTask(c *Coordinator, reply *TaskReply) bool {
	for i, v := range c.reduceState {
		if !v.done && !v.allocated {
			reply.ReduceSeqNumber = i
			reply.TaskType = REDUCE
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			c.reduceState[i].allocated = true
			return true
		}
	}
	return false
}

func findMapTask(c *Coordinator, reply *TaskReply) bool {
	for i, v := range c.mapState {
		if !v.done && !v.allocated {
			reply.FileName = c.files[i]
			reply.MapSeqNumber = i
			reply.NMap = c.nMap
			reply.TaskType = MAP
			reply.NReduce = c.nReduce
			c.mapState[i].allocated = true
			return true
		}
	}
	return false
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.nReduceDone == c.nReduce

	// Your code here.

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nMap: len(files), nReduce: nReduce, lock: &sync.Mutex{}}
	c.mapState = make([]TaskState, c.nMap)
	c.reduceState = make([]TaskState, nReduce)
	c.files = files

	// Your code here.

	c.server()
	return &c
}
