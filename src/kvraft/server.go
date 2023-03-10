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
	CLIENTID  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	table            map[string]string
	clientId_to_cv   map[int64]*sync.Cond
	clientId_to_done map[int64]bool
	index_to_cmdId   map[int]int

	// client_to_currentCmdId -> to track if got same cmdId to apply more than once through applyCh. cmdId increases monotonically
	// clientId_to_cv	(to identify each rpc) -> could have multiple same index??. change to clientId_to_cv
	// if already have clientId_to_cv -> broadcast previous cvs associated to clientId and cleanmaps, send error message
	// index_to_op -> to check if applyMsg index matches cmdId (if leader lost status)
}

// Called with lock held
func (kv *KVServer) makeCond(clientId int64) *sync.Cond {
	kv.clientId_to_cv[clientId] = sync.NewCond(&kv.mu)
	kv.clientId_to_done[clientId] = false
	return kv.clientId_to_cv[clientId]

}

// Called with lock held
func (kv *KVServer) cleanMaps(clientId int64) {
	delete(kv.clientId_to_cv, clientId)
	delete(kv.clientId_to_done, clientId)
}

func (kv *KVServer) conditionalBroadcast(clientId int64) {
	cv, hasCv := kv.clientId_to_cv[clientId]
	if hasCv {
		// fmt.Println("broadcasted")
		cv.Broadcast()
		kv.clientId_to_done[clientId] = true
	}

	// // case where the op that this server called Start on did not end up at the supposed index (server lost leader status)
	// addedCmdId, hasAddedCmdId := kv.index_to_cmdId[commandIndex]
	// if hasAddedCmdId {
	// 	if cmdId != addedCmdId {

	// 	}
	// }
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{}
	op.KEY = args.Key
	op.CMDID = args.CmdId
	op.OPERATION = "Get"
	op.CLIENTID = args.ClientId
	_, _, isLeader := kv.rf.Start(op)
	// kv.index_to_cmdId[index] = args.CmdId
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	cv := kv.makeCond(args.ClientId)
	for kv.clientId_to_done[args.ClientId] == false && !kv.killed() {
		cv.Wait()
	}
	val, valOk := kv.table[args.Key]
	if valOk {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	kv.cleanMaps(args.ClientId)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{}
	op.KEY = args.Key
	op.VALUE = args.Value
	op.CMDID = args.CmdId
	op.CLIENTID = args.ClientId
	op.OPERATION = args.Op
	// fmt.Println("PutAppend before calling Start for server: " + strconv.Itoa(kv.me))
	_, _, isLeader := kv.rf.Start(op)
	// kv.index_to_cmdId[index] = args.CmdId
	// fmt.Println("PutAppend after calling Start for server: " + strconv.Itoa(kv.me))
	if !isLeader {
		// fmt.Println("Server code, server: " + strconv.Itoa(kv.me) + " thinks its not leader " + "term: " + strconv.Itoa(term))
		reply.Err = ErrWrongLeader
		return
	}
	cv := kv.makeCond(args.ClientId)
	for kv.clientId_to_done[args.ClientId] == false && !kv.killed() {
		cv.Wait()
	}
	// fmt.Println("Server: " + strconv.Itoa(kv.me) + "returned from")
	reply.Err = OK
	kv.cleanMaps(args.ClientId)
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
		if kv.killed() {
			return
		}
		// fmt.Print("m from applyCh before lock: ")
		// fmt.Println(m)
		kv.mu.Lock()
		// fmt.Print("m from applyCh after lock: ")
		// fmt.Println(m)
		if m.CommandValid {
			operationCmd, operationOk := m.Command.(Op)
			if operationOk {
				// case where the op that this server called Start on did not end up at the supposed index (server lost leader status)
				// addedCmdId, hasAddedCmdId := kv.index_to_cmdId[m.CommandIndex]
				// if hasAddedCmdId {
				// 	if operationCmd.CMDID != addedCmdId {

				// 	}
				// }
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
				kv.conditionalBroadcast(operationCmd.CLIENTID)
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
	kv.clientId_to_cv = make(map[int64]*sync.Cond)
	kv.clientId_to_done = make(map[int64]bool)
	kv.table = make(map[string]string)

	// You may need initialization code here.

	go kv.applier()

	return kv
}
