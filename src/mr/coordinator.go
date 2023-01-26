package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
// import "fmt"
import "sync"

type Task struct {
	id int
	file string
}

var muCoordinator sync.Mutex

type Coordinator struct {
	// Your definitions here.
	// Either R or F
	worker_status map[int]string 
	map_tasks_id_to_task_struct map[int]Task
	worker_to_task map[int]Task
	reduce_tasks []int
	nMap int
	nReduce int
	start_reduce bool
	worker_to_last_heartbeat map[int]time.Time
	last_worker_id int
}

func(c *Coordinator) HeartBeatRecieve(args *HeartBeatArgs, reply *HeartBeatReply) error {
	// fmt.Println("Receiving heartbeat")
	muCoordinator.Lock()
	// fmt.Println("HeartBeatRecieveLock")
	if args.WORKER_ID == -1 || c.worker_status[args.WORKER_ID] == "f" {
		reply.WORKER_ID = c.last_worker_id
		c.last_worker_id++
		c.worker_status[reply.WORKER_ID] = "r"
	} else {
		reply.WORKER_ID = args.WORKER_ID
	}
	c.worker_to_last_heartbeat[reply.WORKER_ID] = time.Now()
	// fmt.Println("HeartBeatRecieveUnlock")
	muCoordinator.Unlock()
	return nil
}

func(c *Coordinator) HeartBeatChecker() error {
	muCoordinator.Lock()
	// fmt.Println("HeartBeatCheckerLock")
	for !(len(c.map_tasks_id_to_task_struct) == 0 && len(c.reduce_tasks) == 0 && len(c.worker_to_task) == 0) {
		for worker_id, last_heartbeat := range c.worker_to_last_heartbeat {
			// case where worker died
			if (time.Now().Sub(last_heartbeat) > 10 * time.Second) {
				task_struct, ok := c.worker_to_task[worker_id]
				if ok {
					if c.start_reduce {
						c.reduce_tasks = append(c.reduce_tasks, task_struct.id)
					} else {
						c.map_tasks_id_to_task_struct[task_id] = task_struct
					}
					delete(c.worker_to_task, worker_id)
				}
				c.worker_status[worker_id] = "f"
				delete(c.worker_to_last_heartbeat, worker_id)
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
func(c *Coordinator) ReadyDone(args *ReadyDoneArgs, reply *ReadyDoneReply) error {
	muCoordinator.Lock()
	// fmt.Println("ReadyDoneLock")
	_, ok := c.worker_to_task[args.WORKER_ID]
	if c.worker_status[args.WORKER_ID] != "f" && args.TASK_ID != -1 && ok {
		delete(c.worker_to_task, args.WORKER_ID)
	}
	for !(len(c.map_tasks_id_to_task_struct) == 0 && len(c.reduce_tasks) == 0 && len(c.worker_to_task) == 0) {
		// if worker dead, just send empty task and exit from func
		if c.worker_status[args.WORKER_ID] == "f" {
			reply.TASK_ID = -1
			muCoordinator.Unlock()
			return nil
		}
		if len(c.map_tasks_id_to_task_struct) > 0 {	// case where still have map task
			// Get first TASK_ID in map_tasks_id_to_task_struct
			for key, _ := range c.map_tasks_id_to_task_struct {
				reply.TASK_ID = key
				break
			}
			reply.TASK_TYPE = "m"
			reply.NMR = c.nReduce
			reply.FILE = c.map_tasks_id_to_task_struct[reply.TASK_ID].file
			c.worker_to_task[args.WORKER_ID] = c.map_tasks_id_to_task_struct[reply.TASK_ID]
			delete(c.map_tasks_id_to_task_struct, reply.TASK_ID)
			muCoordinator.Unlock()
			return nil
		} else {	// case where all map task completed, do reduce
			if len(c.worker_to_task) == 0 {
				c.start_reduce = true
			}
			if c.start_reduce && len(c.reduce_tasks) > 0 {
				reply.TASK_TYPE = "r"
				reply.NMR = c.nMap
				reply.TASK_ID = c.reduce_tasks[len(c.reduce_tasks)-1]
				// Removing the task from the slice
				c.reduce_tasks = c.reduce_tasks[:len(c.reduce_tasks) - 1]
				task := Task{}
				task.id = reply.TASK_ID
				c.worker_to_task[args.WORKER_ID] = task
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
	ret := len(c.map_tasks_id_to_task_struct) == 0 && len(c.reduce_tasks) == 0 && len(c.worker_to_task) == 0
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
	c.map_tasks_id_to_task_struct = make(map[int]Task)
	c.worker_status = make(map[int]string)
	c.worker_to_task = make(map[int]Task)
	c.worker_to_last_heartbeat = make(map[int]time.Time)
	// populate c.map_tasks_id_to_task_struct
	for i := 0; i < len(files); i++ {
		
		c.map_tasks_id_to_task_struct[i] = Task{i, files[i]}
	}
	// populate c.reduce_tasks
	for i:=0; i < nReduce; i++ {
		c.reduce_tasks = append(c.reduce_tasks, i)
	}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.start_reduce = false
	c.last_worker_id = 0

	c.server()
	go c.HeartBeatChecker()

	return &c
}
