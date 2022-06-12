package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "sync"


type Clerk struct {
	servers []*labrpc.ClientEnd

	mu sync.Mutex
	clientId  int64
	requestId int
	leaderId  int

	// You will have to modify this struct.
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
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0
	DPrintf("create a Client %v",ck.clientId)
	// You'll have to add code here.
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
	ck.mu.Lock()
	ck.requestId += 1
	// DPrintf("%v", ck.requestId)
	args := GetArgs{key, ck.clientId, ck.requestId}
	oldLeaderId := ck.leaderId
	pLen := len(ck.servers)
	ck.mu.Unlock()

	// ck.FindLeader()
	for ;; oldLeaderId = (oldLeaderId + 1) % pLen {
		reply := GetReply{}
		ok := ck.servers[oldLeaderId].Call("KVServer.Get", &args, &reply)
		
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			// if !ok {
			// 	DPrintf("GET  client %v request %v fail RPC to server %v with lost RPC.",
			// 		ck.clientId, ck.requestId, oldLeaderId)
			// } else {
			// 	DPrintf("GET  client %v request %v fail RPC to server %v with %v.",
			// 		ck.clientId, ck.requestId, oldLeaderId, reply.Err)
			// }
			continue 
		}

		ck.mu.Lock()
		ck.leaderId = oldLeaderId
		ck.mu.Unlock()
		// DPrintf("%v (%v) (%v)", "GET", key,reply.Value)
		if reply.Err == OK {
			// DPrintf("GET  client %v request %v Accept RPC to server %v with %v.",
			// 	ck.clientId, ck.requestId, oldLeaderId, reply.Err)
			return reply.Value
		}

		if reply.Err == ErrNoKey {
			// DPrintf("GET  client %v request %v Accept RPC to server %v with %v.",
			// 	ck.clientId, ck.requestId, oldLeaderId, reply.Err)
			return ""
		}
	}

	// You will have to modify this function.
	// return ""
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
	ck.mu.Lock()
	ck.requestId += 1
	// DPrintf("%v", ck.requestId)
	args := PutAppendArgs{key, value, op, ck.clientId, ck.requestId}
	oldLeaderId := ck.leaderId
	pLen := len(ck.servers)
	ck.mu.Unlock()

	// DPrintf("%v (%v) (%v)", op, key,value)
	// ck.FindLeader()
	for ;; oldLeaderId = (oldLeaderId + 1) % pLen {
		reply := PutAppendReply{}
		ok := ck.servers[oldLeaderId].Call("KVServer.PutAppend", &args, &reply)
		
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			// if !ok {
			// 	DPrintf("PUT  client %v request %v fail RPC to server %v with lost RPC.",
			// 		ck.clientId, ck.requestId, oldLeaderId)
			// } else {
			// 	DPrintf("PUT  client %v request %v fail RPC to server %v with %v.",
			// 		ck.clientId, ck.requestId, oldLeaderId, reply.Err)
			// }
			continue 
		}

		ck.mu.Lock()
		ck.leaderId = oldLeaderId
		ck.mu.Unlock()

		if reply.Err == OK {
			// DPrintf("PUT  client %v request %v Accept RPC to server %v with %v.",
			// 	ck.clientId, ck.requestId, oldLeaderId, reply.Err)
			return 
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
