package kvraft

import (
	"crypto/rand"
	// "fmt"
	"math"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	cmdId    int
	clientId int64
}

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
	ck.leader = 0
	ck.cmdId = 0
	ck.clientId = nrand()
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// fmt.Println("Get key: " + key)
	args := GetArgs{}
	args.Key = key
	args.CmdId = ck.cmdId
	args.ClientId = ck.clientId
	ck.cmdId += 1
	for i := ck.leader; i < math.MaxInt32; i++ {
		reply := GetReply{}
		// fmt.Println("Trying Get for server: " + strconv.Itoa(i))
		ok := ck.servers[i%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err != ErrWrongLeader {
				ck.leader = i
				if reply.Err == ErrNoKey {
					return ""
				} else {
					return reply.Value
				}
			} else if i != ck.leader && (i-ck.leader)%len(ck.servers) == 0 { // case where went one loop and did not find a leader, wait for election to complete
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{}
	args.Op = op
	args.Key = key
	args.Value = value
	args.CmdId = ck.cmdId
	args.ClientId = ck.clientId
	ck.cmdId += 1
	for i := ck.leader; i < math.MaxInt32; i++ {
		reply := PutAppendReply{}
		// fmt.Println("Trying PutAppend for server: " + strconv.Itoa(i%len(ck.servers)))
		ok := ck.servers[i%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			// fmt.Println("reply.Err: " + reply.Err)
			if reply.Err != ErrWrongLeader {
				ck.leader = i
				return
			} else if i != ck.leader && (i-ck.leader)%len(ck.servers) == 0 { // case where went one loop and did not find a leader, wait for election to complete
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	// fmt.Println("Put key: " + key + " val: " + value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	// fmt.Println("Append key: " + key + " val: " + value)

	ck.PutAppend(key, value, "Append")
}
