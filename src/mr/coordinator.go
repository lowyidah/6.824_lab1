package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// import "fmt"

type Task struct {
	id                 int
	file               string
	time_of_start_exec time.Time
}

var muCoordinator sync.Mutex

type Coordinator struct {
	// Your definitions here.
	// Either R or F
	map_tasks       map[int]Task
	executing_tasks map[int]Task
	reduce_tasks    map[int]Task
	nMap            int
	nReduce         int
	start_reduce    bool
}

func (c *Coordinator) HeartBeatChecker() error {
	muCoordinator.Lock()
	// fmt.Println("HeartBeatCheckerLock")
	for !(len(c.map_tasks) == 0 && len(c.reduce_tasks) == 0 && len(c.executing_tasks) == 0) {
		for task_id, task_struct := range c.executing_tasks {
			// case where worker died
			if time.Now().Sub(task_struct.time_of_start_exec) > 10*time.Second {
				if c.start_reduce {
					c.reduce_tasks[task_id] = task_struct
				} else {
					c.map_tasks[task_id] = task_struct
				}
			}
		}
		// fmt.Println("HeartBeatCheckerUnlock")
		muCoordinator.Unlock()
		time.Sleep(time.Second)
		muCoordinator.Lock()
		// fmt.Println("HeartBeatCheckerLock")

	}
	muCoordinator.Unlock()
	return nil
}

// Your code here -- RPC handlers for the worker to call.
// TODO: remove from struct when task is done
func (c *Coordinator) ReadyDone(args *ReadyDoneArgs, reply *ReadyDoneReply) error {
	muCoordinator.Lock()
	_, ok_task := c.executing_tasks[args.TASK_ID]
	_, ok_reduce := c.reduce_tasks[args.TASK_ID]
	_, ok_map := c.map_tasks[args.TASK_ID]
	if ok_task {
		delete(c.executing_tasks, args.TASK_ID)
		if c.start_reduce && ok_reduce {
			delete(c.reduce_tasks, args.TASK_ID)
		} else if ok_map {
			delete(c.map_tasks, args.TASK_ID)
		}
	}
	for !(len(c.map_tasks) == 0 && len(c.reduce_tasks) == 0 && len(c.executing_tasks) == 0) {
		if len(c.map_tasks) > 0 { // case where still have map task
			// Get first TASK_ID in map_tasks
			for key, _ := range c.map_tasks {
				reply.TASK_ID = key
				break
			}
			reply.TASK_TYPE = "m"
			reply.NMR = c.nReduce
			reply.FILE = c.map_tasks[reply.TASK_ID].file
			task := Task{}
			task.id = reply.TASK_ID
			task.time_of_start_exec = time.Now()
			task.file = reply.FILE
			c.executing_tasks[reply.TASK_ID] = task
			delete(c.map_tasks, reply.TASK_ID)
			muCoordinator.Unlock()
			return nil
		} else { // case where all map task completed, do reduce
			if len(c.executing_tasks) == 0 {
				c.start_reduce = true
				fmt.Println(c.map_tasks)
				fmt.Println(c.executing_tasks)
				fmt.Println(c.reduce_tasks)
			}
			if c.start_reduce && len(c.reduce_tasks) > 0 {
				reply.TASK_TYPE = "r"
				reply.NMR = c.nMap
				for key, _ := range c.reduce_tasks {
					reply.TASK_ID = key
					break
				}

				task := Task{}
				task.id = reply.TASK_ID
				task.time_of_start_exec = time.Now()
				c.executing_tasks[reply.TASK_ID] = task
				delete(c.reduce_tasks, reply.TASK_ID)
				muCoordinator.Unlock()
				return nil
			}
		}
		// fmt.Println("unlock readydone")
		muCoordinator.Unlock()
		time.Sleep(time.Second)
		muCoordinator.Lock()
		// fmt.Println("ReadyDoneLock")
	}
	muCoordinator.Unlock()
	reply.TASK_TYPE = "d"
	return nil
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
	muCoordinator.Lock()
	ret := len(c.map_tasks) == 0 && len(c.reduce_tasks) == 0 && len(c.executing_tasks) == 0
	muCoordinator.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.map_tasks = make(map[int]Task)
	c.reduce_tasks = make(map[int]Task)
	c.executing_tasks = make(map[int]Task)

	// populate c.map_tasks
	for i := 0; i < len(files); i++ {
		c.map_tasks[i] = Task{i, files[i], time.Now()}
	}
	// populate c.reduce_tasks
	for i := 0; i < nReduce; i++ {
		c.reduce_tasks[i] = Task{i, "", time.Now()}
	}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.start_reduce = false

	c.server()
	go c.HeartBeatChecker()

	return &c
}
