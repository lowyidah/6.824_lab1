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
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var task_id int
var task_type string

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
	if err == nil {
		return true
	}
	return false
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
	task_id = -1
	CallReadyDone(mapf, reducef)
}

func CallReadyDone(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for task_type != "d" {
		args := ReadyDoneArgs{}
		args.TASK_ID = task_id
		args.TASK_TYPE = task_type
		reply := ReadyDoneReply{}
		ok := call("Coordinator.ReadyDone", &args, &reply)
		if ok {
			task_type = reply.TASK_TYPE
			task_id = reply.TASK_ID
			mydir, _ := os.Getwd()
			if reply.TASK_TYPE == "m" {
				file, err := os.Open(reply.FILE)
				if err != nil {
					log.Fatalf("cannot open %v", reply.FILE)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.FILE)
				}
				file.Close()
				fmt.Println("Before map")
				kva := mapf(reply.FILE, string(content))
				fmt.Println("After map")
				reduce_task_to_tmp_file_name := make(map[int]*os.File)
				encoders := make(map[int]*json.Encoder)
				for i := 0; i < reply.NMR; i++ {
					reduce_file_name := "m[" + strconv.Itoa(reply.TASK_ID) + "]-r[" + strconv.Itoa(i) + "]"
					tmpfile, _ := ioutil.TempFile(mydir, reduce_file_name)
					reduce_task_to_tmp_file_name[i] = tmpfile
					encoders[i] = json.NewEncoder(tmpfile)
				}
				for _, key_value := range kva {
					reduce_task := ihash(key_value.Key) % reply.NMR
					encoders[reduce_task].Encode(&key_value)
				}
				for i := 0; i < reply.NMR; i++ {
					reduce_file_name := "m[" + strconv.Itoa(reply.TASK_ID) + "]-r[" + strconv.Itoa(i) + "]"
					os.Rename(reduce_task_to_tmp_file_name[i].Name(), reduce_file_name)
					reduce_task_to_tmp_file_name[i].Close()
					fmt.Println("Finished: " + reduce_file_name)
				}

			} else if reply.TASK_TYPE == "r" {
				// intermediate file name format: "m[x]-r[y]", where 0 <= x <= nMap, 0 <= y <= nReduce
				// fmt.Println("Reached Reduce")
				intermediate := []KeyValue{}
				for i := 0; i < reply.NMR; i++ {
					reduce_file_name := "m[" + strconv.Itoa(i) + "]-r[" + strconv.Itoa(reply.TASK_ID) + "]"
					reduce_file, _ := os.Open(reduce_file_name)
					dec := json.NewDecoder(reduce_file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
				}
				fmt.Println("before sort")
				sort.Sort(ByKey(intermediate))
				fmt.Println("after sort")

				oname := "mr-out-" + strconv.Itoa(reply.TASK_ID)
				ofile, _ := ioutil.TempFile(mydir, oname)

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				os.Rename(ofile.Name(), oname)
				ofile.Close()
				fmt.Println("Finished: " + ofile.Name())
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}
	// fmt.Println("Worker Done")
}
