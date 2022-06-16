package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "time"
import "sort"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	doneLog map[int64]int
	backChan map[int]chan Op

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	Optype string
	Opcont interface{}
}

func deepCopy(x Config) Config {
	y := Config{Num:x.Num, Groups:make(map[int][]string,0)}
	for i,arr := range x.Shards {
		y.Shards[i] = arr
	}

	for gid,group := range x.Groups {
		y.Groups[gid] = make([]string, 0)
		for _,s := range group {
			y.Groups[gid] = append(y.Groups[gid], s)
		}
	}

	return y
}

func SameStruct(x,y Op) bool {
	if x.Optype != y.Optype {
		return false
	}

	switch x.Optype {
	case Join :
		xx := x.Opcont.(JoinArgs)
		yy := y.Opcont.(JoinArgs)
		return xx.Cid == yy.Cid && xx.Rid == yy.Rid
	case Leave :
		xx := x.Opcont.(LeaveArgs)
		yy := y.Opcont.(LeaveArgs)
		return xx.Cid == yy.Cid && xx.Rid == yy.Rid
	case Move :
		xx := x.Opcont.(MoveArgs)
		yy := y.Opcont.(MoveArgs)
		return xx.Cid == yy.Cid && xx.Rid == yy.Rid
	case Query :
		xx := x.Opcont.(QueryArgs)
		yy := y.Opcont.(QueryArgs)
		return xx.Num == yy.Num
	}

	return false
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// DPrintf("Get a Join request %v.", args)
	opt := Op{Optype:Join, Opcont:*args}
	reply.WrongLeader = !sc.SendLog(opt)
	return 
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// DPrintf("Get a Leave request %v.", args)
	// Your code here.
	opt := Op{Optype:Leave, Opcont:*args}
	reply.WrongLeader = !sc.SendLog(opt)
	return 
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// DPrintf("Get a Move request %v.", args)
	// Your code here.
	opt := Op{Optype:Move, Opcont:*args}
	reply.WrongLeader = !sc.SendLog(opt)
	return 
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// DPrintf("Get a Query request %v.", args)
	// Your code here.
	sc.mu.Lock()
	if args.Num == -1 {
		args.Num = len(sc.configs) - 1
	}
	sc.mu.Unlock()
	opt := Op{Optype:Query, Opcont:*args}
	reply.WrongLeader = !sc.SendLog(opt)
	if reply.WrongLeader == false {
		sc.mu.Lock()
		if args.Num >= len(sc.configs) || args.Num < 0 {
			reply.Err = UndefinedNum
		} else {
			reply.Config = sc.configs[args.Num]
			// DPrintf("++ %v",reply.Config)
		}
		sc.mu.Unlock()
	}
	return 
}

// func (sc *ShardCtrler) ReComm(opt Op) bool {
// 	sc.mu.Lock()
// 	if sc.DoneRequ[]
// }

func (sc *ShardCtrler) SendLog(opt Op) bool {
	sc.mu.Lock()
	index,_,isLeader := sc.rf.Start(opt)
	sc.mu.Unlock()
	if isLeader == false {
		return false
	}
	// DPrintf("get a leader")

	sc.mu.Lock()
	ch, have := sc.backChan[index]
	if have == false {
		ch = make(chan Op, 1)
		sc.backChan[index] = ch
	}
	sc.mu.Unlock()

	select {
	case x := <- ch :
		// DPrintf("GET  %v", x)
		if !SameStruct(x, opt) {
			sc.CloseChan(index)
			return false
		}
		
		sc.CloseChan(index)
		return true
	case <- time.After(TimeOut * time.Millisecond) :
		sc.CloseChan(index)
		return false
	}
}

func (sc *ShardCtrler) CloseChan(index int) {
	sc.mu.Lock()
	ch, have := sc.backChan[index]
	if have == true {
		close(ch)
		delete(sc.backChan, index)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) LiveApply() {
	for {
		// DPrintf("Start!")
		select {
		case backAM := <- sc.applyCh :
			if backAM.CommandValid == true {
				opt := backAM.Command.(Op)
				switch opt.Optype {
				case Join:
					sc.DoJoin(opt.Opcont.(JoinArgs))
				case Leave :
					sc.DoLeave(opt.Opcont.(LeaveArgs))
				case Move :
					sc.DoMove(opt.Opcont.(MoveArgs))
				}

				sc.mu.Lock()
				_,isLeader := sc.rf.GetState()
				if isLeader == true {
					// DPrintf("liveapply leader : %v", kv.me)
					ch, have := sc.backChan[backAM.CommandIndex]
					if have == true {
						ch <- opt
					}
					// DPrintf("doing %v",opt)
				}
				sc.mu.Unlock()
			} 
		}
	}
}

func (sc *ShardCtrler) DoJoin(opt JoinArgs) {
	sc.mu.Lock()
	if sc.doneLog[opt.Cid] >= opt.Rid {
		sc.mu.Unlock()
		return 
	}

	conf := deepCopy(sc.configs[len(sc.configs)-1])
	conf.Num = len(sc.configs)
	for gid, group := range opt.Servers {
		conf.Groups[gid] = append(make([]string,0), group...)
	}
	// DPrintf("- %v",conf)
	sc.ReBalance(&conf)
	// DPrintf("+ %v",conf)
	sc.configs = append(sc.configs, conf)
	sc.doneLog[opt.Cid] = opt.Rid
	sc.mu.Unlock()
}

func (sc *ShardCtrler) DoLeave(opt LeaveArgs) {
	sc.mu.Lock()
	if sc.doneLog[opt.Cid] >= opt.Rid {
		sc.mu.Unlock()
		return 
	}

	conf := deepCopy(sc.configs[len(sc.configs)-1])
	conf.Num = len(sc.configs)
	for _,gid := range opt.GIDs {
		delete(conf.Groups, gid)
	}
	
	// DPrintf("- %v",conf)
	sc.ReBalance(&conf)
	// DPrintf("+ %v",conf)
	sc.configs = append(sc.configs, conf)
	sc.doneLog[opt.Cid] = opt.Rid
	sc.mu.Unlock()
}

func (sc *ShardCtrler) DoMove(opt MoveArgs) {
	sc.mu.Lock()
	if sc.doneLog[opt.Cid] >= opt.Rid {
		sc.mu.Unlock()
		return 
	}

	conf := deepCopy(sc.configs[len(sc.configs)-1])
	conf.Num = len(sc.configs)
	conf.Shards[opt.Shard] = opt.GID
	sc.configs = append(sc.configs, conf)
	sc.doneLog[opt.Cid] = opt.Rid
	sc.mu.Unlock()
}

func (sc *ShardCtrler) ReBalance(conf *Config) {
	// conf := sc.configs[len(sc.configs)-1]
	gLen := 0
	Snum := make(map[int]int)
	needMove := make([]int,0)
	Gids := make([]int,0)

	for gid,_ := range conf.Groups {
		gLen += 1
		Snum[gid] = 0
		Gids = append(Gids, gid)
	}

	if gLen == 0 {
		for i,_ := range conf.Shards {
			conf.Shards[i] = 0
		}
		return 
	}
	
	eachSum := NShards / gLen

	for i,gid := range conf.Shards {
		_,ok := conf.Groups[gid]
		if ok == false || eachSum == Snum[gid] {
			needMove = append(needMove, i)
		} else {
			Snum[gid] += 1
		}
	}

	sort.Ints(Gids)

	index := len(needMove) - 1
	// for gid,_ := range conf.Groups {
	for _,gid := range Gids {
		for eachSum > Snum[gid]  && index >= 0 {
			conf.Shards[needMove[index]] = gid
			Snum[gid] += 1
			index -= 1
		}
	}

	// for gid,_ := range conf.Groups {
	for _,gid := range Gids {
		if index >= 0 {
			conf.Shards[needMove[index]] = gid
			Snum[gid] += 1
			index -= 1
		}
	}
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.doneLog = make(map[int64]int)
	sc.backChan = make(map[int]chan Op)
	go sc.LiveApply()

	return sc
}
