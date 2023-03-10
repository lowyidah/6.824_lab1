package kvraft

import (
	// "fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OPERATION string // either "Put", "Append" or "Get"
	KEY       string
	VALUE     string
	CMDID     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	table         map[string]string
	cmdId_to_cv   map[int]*sync.Cond
	cmdId_to_done map[int]bool
}

// Called with lock held
func (kv *KVServer) makeCond(cmdId int) *sync.Cond {
	kv.cmdId_to_cv[cmdId] = sync.NewCond(&kv.mu)
	kv.cmdId_to_done[cmdId] = false
	return kv.cmdId_to_cv[cmdId]

}

// Called with lock held
func (kv *KVServer) cleanMaps(cmdId int) {
	delete(kv.cmdId_to_cv, cmdId)
	delete(kv.cmdId_to_done, cmdId)
}

func (kv *KVServer) conditionalBroadcast(cmdId int) {
	cv, hasCv := kv.cmdId_to_cv[cmdId]
	if hasCv {
		// fmt.Println("broadcasted")
		cv.Broadcast()
		kv.cmdId_to_done[cmdId] = true
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{}
	op.KEY = args.Key
	op.CMDID = args.CmdId
	op.OPERATION = "Get"
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	cv := kv.makeCond(args.CmdId)
	for kv.cmdId_to_done[args.CmdId] == false {
		cv.Wait()
	}
	val, valOk := kv.table[args.Key]
	if valOk {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	kv.cleanMaps(args.CmdId)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{}
	op.KEY = args.Key
	op.VALUE = args.Value
	op.CMDID = args.CmdId
	op.OPERATION = args.Op
	// fmt.Println("PutAppend before calling Start for server: " + strconv.Itoa(kv.me))
	_, _, isLeader := kv.rf.Start(op)
	// fmt.Println("PutAppend after calling Start for server: " + strconv.Itoa(kv.me))
	if !isLeader {
		// fmt.Println("Server code, server: " + strconv.Itoa(kv.me) + " thinks its not leader " + "term: " + strconv.Itoa(term))
		reply.Err = ErrWrongLeader
		return
	}
	cv := kv.makeCond(args.CmdId)
	for kv.cmdId_to_done[args.CmdId] == false {
		cv.Wait()
	}
	// fmt.Println("Server: " + strconv.Itoa(kv.me) + "returned from")
	reply.Err = OK
	kv.cleanMaps(args.CmdId)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// Keeps running as a go routine until killed, continuously tries to apply stuff from the applyCh to the database
func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		// fmt.Print("m from applyCh before lock: ")
		// fmt.Println(m)
		kv.mu.Lock()
		// fmt.Print("m from applyCh after lock: ")
		// fmt.Println(m)
		if m.CommandValid {
			operationCmd, operationOk := m.Command.(Op)
			if operationOk {
				// In the case that this server is the leader
				if operationCmd.OPERATION == "Put" {
					kv.table[operationCmd.KEY] = operationCmd.VALUE
				} else if operationCmd.OPERATION == "Append" {
					_, hasKey := kv.table[operationCmd.KEY]
					if hasKey {
						kv.table[operationCmd.KEY] += operationCmd.VALUE
					} else {
						kv.table[operationCmd.KEY] = operationCmd.VALUE
					}
				}
				kv.conditionalBroadcast(operationCmd.CMDID)
				// TODO: signal relevant RPC handler cv
			} else {
				// fmt.Println("Invalid operation")
			}
		} else if m.SnapshotValid {
			// TODO: snapshotting
		} else {
			// fmt.Println("Error: unknown commited log entry type")
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.cmdId_to_cv = make(map[int]*sync.Cond)
	kv.cmdId_to_done = make(map[int]bool)
	kv.table = make(map[string]string)

	// You may need initialization code here.

	go kv.applier()

	return kv
}
