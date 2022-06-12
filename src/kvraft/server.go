package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Opt       string
	Key       string
	Value     string
	ClientId  int64
	RequestId int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVdb struct {
	mu  sync.Mutex
	kvp map[string]string
}

func (kvdb *KVdb) Get(Key string) string {
	kvdb.mu.Lock()
	Value, ok := kvdb.kvp[Key]
	if ok == false {
		Value = ""
	}
	kvdb.mu.Unlock()
	return Value
}

func (kvdb *KVdb) Put(Key, Value string) {
	kvdb.mu.Lock()
	kvdb.kvp[Key] = Value
	kvdb.mu.Unlock()
}

func (kvdb *KVdb) Append(Key, Value string) {
	// oldV := kvdb.Get(Key)
	// kvdb.Put(Key, oldV + Value)
	kvdb.mu.Lock()
	kvdb.kvp[Key] += Value
	kvdb.mu.Unlock()
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	DB KVdb
	doneLog map[int64]int
	backChan map[int]chan Op

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}


func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	ch, have := kv.backChan[index]
	if have == true {
		close(ch)
		delete(kv.backChan, index)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	opt := Op {
		Opt : GetOpt,
		Key : args.Key,
		ClientId : args.ClientId,
		RequestId : args.RequestId,
	}

	kv.mu.Lock()
	index,_,isLeader := kv.rf.Start(opt)
	kv.mu.Unlock()

	if isLeader == false {
		reply.Err = "ErrWrongLeader"
		// DPrintf("ser %v this last logs is : %v", kv.me, kv.rf.GetRaftStateSize())
		// DPrintf("%v not leader", kv.me)
		return 
	}
	// DPrintf("%v %v %v\n op: %v in %v,", kv.me, "is leader > ", isLeader, opt, kv.me)
	// kv.CheckLogsLen(index)

	kv.mu.Lock()
	ch, have := kv.backChan[index]
	if have == false {
		ch = make(chan Op, 1)
		kv.backChan[index] = ch
	}
	kv.mu.Unlock()

	select {
	case x := <- ch :
		// DPrintf("GET + %v \n %v, in %v", x,opt, kv.me)
		if x.Opt != GetOpt || x.Key != opt.Key || x.ClientId != opt.ClientId || x.RequestId != opt.RequestId {
			reply.Err = ErrWrongLeader
			// DPrintf("this")
			kv.CloseChan(index)
			return 
		}
		
		reply.Err = OK
		reply.Value = x.Value
		kv.CloseChan(index)
		return 
	case <- time.After(TimeOut * time.Millisecond) :
		reply.Err = ErrTimeOut
		kv.CloseChan(index)
		return 
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	opt := Op {
		Opt : args.Op,
		Key : args.Key,
		Value : args.Value,
		ClientId : args.ClientId,
		RequestId : args.RequestId,
	}

	kv.mu.Lock()
	index,_,isLeader := kv.rf.Start(opt)
	kv.mu.Unlock()

	if isLeader == false {
		// DPrintf("%v wrong in %v",index, term)
	// DPrintf("ser %v this last logs is : %v", kv.me, kv.rf.GetRaftStateSize())
		reply.Err = ErrWrongLeader
		return 
	}

	// kv.CheckLogsLen(index)

	kv.mu.Lock()
	ch, have := kv.backChan[index]
	if have == false {
		ch = make(chan Op, 1)
		kv.backChan[index] = ch
	}
	kv.mu.Unlock()

	select {
	case x := <- ch :
		// DPrintf("%v \n %v", x,opt)
		if x.Opt != opt.Opt || x.Key != opt.Key || x.Value != opt.Value || x.ClientId != opt.ClientId || x.RequestId != opt.RequestId {
			reply.Err = ErrWrongLeader
			kv.CloseChan(index)
			return 
		}
		
		reply.Err = OK
		kv.CloseChan(index)
		return 
	case <- time.After(TimeOut * time.Millisecond) :
		reply.Err = ErrTimeOut
		kv.CloseChan(index)
		return 
	}
}

func (kv *KVServer) LiveApply() {
	for {
		// DPrintf("Start!")
		select {
		case backAM := <- kv.applyCh :
			if backAM.CommandValid == true {
				opt := backAM.Command.(Op)
				kv.mu.Lock()
				x,ok := kv.doneLog[opt.ClientId]
				if ok == true && x >= opt.RequestId {
					kv.mu.Unlock()
					if opt.Opt == GetOpt {
						opt.Value = kv.DB.Get(opt.Key)
					}
					// continue 
				} else {
					kv.doneLog[opt.ClientId] = opt.RequestId
					kv.mu.Unlock()

					if opt.Opt == GetOpt {
						opt.Value = kv.DB.Get(opt.Key)
					}
					if opt.Opt == PutOpt {
						kv.DB.Put(opt.Key, opt.Value)
					}
					if opt.Opt == AppendOpt {
						kv.DB.Append(opt.Key, opt.Value)
					}
				}

				kv.mu.Lock()
				_,isLeader := kv.rf.GetState()
				if isLeader == true {
					// DPrintf("liveapply leader : %v", kv.me)
					ch, have := kv.backChan[backAM.CommandIndex]
					if have == true {
						ch <- opt
					}
					kv.CheckLogsLen(backAM.CommandIndex)
					// DPrintf("doing %v",opt)
				}
				kv.mu.Unlock()
			} else if backAM.SnapshotValid == true {
				// snapshot := backAM.Snapshot
				kv.mu.Lock()
				ok := kv.rf.CondInstallSnapshot(
						backAM.SnapshotTerm, backAM.SnapshotIndex, backAM.Snapshot)
				
				if ok == true {
					data := backAM.Snapshot
					r := bytes.NewBuffer(data)
					d := labgob.NewDecoder(r)
					var kvMap map[string]string
					var crMap map[int64]int
					if d.Decode(&kvMap) != nil || 
						d.Decode(&crMap) != nil {
							// log.
					} else {
						kv.DB.mu.Lock()
						kv.DB.kvp = kvMap
						kv.DB.mu.Unlock()
						kv.doneLog = crMap
					}
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) CheckLogsLen(index int) {
	if kv.maxraftstate == -1  || kv.maxraftstate > kv.rf.GetRaftStateSize() {
		return 
	}

	// kv.mu.Lock() 
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.DB.kvp)
	e.Encode(kv.doneLog)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
	// kv.mu.Unlock()
	// DPrintf("snapshot from %v, and last logs : %v",index, kv.rf.GetRaftStateSize())
}

func (kv *KVServer) ReadKVSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return 
	}
	
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvMap map[string]string
	var crMap map[int64]int
	if d.Decode(&kvMap) != nil || 
		d.Decode(&crMap) != nil {
			// log.
	} else {
		kv.DB.mu.Lock()
		kv.DB.kvp = kvMap
		kv.DB.mu.Unlock()
		kv.doneLog = crMap
	}
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

	kv.DB = KVdb{}
	kv.DB.kvp = make(map[string]string)
	kv.doneLog = make(map[int64]int)
	kv.backChan = make(map[int]chan Op)

	kv.ReadKVSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.LiveApply()
	// You may need initialization code here.

	return kv
}
