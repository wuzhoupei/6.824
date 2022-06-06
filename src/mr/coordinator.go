package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Coordinator struct {
	// Your definitions here.
	m sync.Mutex

	files []string
	fileSum int
	fileNumber map[string]int
	NReduce int

	// map :
	MapJobList chan string
	MapFinished int
	MapJobState map[string]int // 0 dont; 1 doing; 2 done

	// reduce : 
	ReduceJobList chan int
	ReduceFinished int
	ReduceJobState map[int]int // 0 dont; 1 doing; 2 done
}

func (c *Coordinator) PushJob() {
	for _,filename := range c.files {
		c.MapJobList <- filename
	}
	for i := 0; i < c.NReduce; i ++  {
		c.ReduceJobList <- i
	}
}

func (c *Coordinator) Timer(mapJob string, reduceJob int) {
	time.Sleep(time.Second * 10)

	if reduceJob == -1 {
		c.m.Lock()
		ok := c.MapJobState[mapJob]
		c.m.Unlock()
		if ok == 1 {
			c.MapJobList <- mapJob
		}
	} else {
		c.m.Lock()
		ok := c.ReduceJobState[reduceJob]
		c.m.Unlock()
		if ok == 1 {
			c.ReduceJobList <- reduceJob
		}	
	}
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


// func (c *Coordinator) Call(args *JobArgs, reply *JobReply) error {
// 	log.Printf("get a new worker need job\n")
// 	return c.Job2Worker(args, reply)
// }

func (c *Coordinator) Job2Worker(args *JobArgs, reply *JobReply) error {
	c.m.Lock()
	if args.JobType != -1 {
		if args.JobType == 0 {
			switch c.MapJobState[args.Filename] {
			case 0:
				log.Printf("An unassigned task was executed!")
			case 1:
				// if c.MapJobState[args.Filename] == 1 {
					c.MapJobState[args.Filename] = 2
					c.MapFinished += 1
					if c.MapFinished == c.fileSum {
						close(c.MapJobList)
					}
				// }
				// log.Printf("NUMBER : %v; %v",c.MapFinished,c.fileSum)
			case 2:
				log.Printf("A job has been repeated.")
			default:
				log.Printf("Undefiend state!")
			}
		}

		if args.JobType == 1 {
			switch c.ReduceJobState[args.NReduce] {
			case 0:
				log.Printf("An unassigned task was executed!")
			case 1:
				// if c.ReduceJobState[args.NReduce] == 1 {
					c.ReduceJobState[args.NReduce] = 2
					c.ReduceFinished += 1
					if c.ReduceFinished == c.NReduce {
						close(c.ReduceJobList)
					}
				// }
				// log.Printf("NUMBER : %v; %v",c.ReduceFinished,c.NReduce)
			case 2:
				log.Printf("A job has been repeated.")
			default:
				log.Printf("Undefiend state!")
			}
		}
	}
	c.m.Unlock()
	
	
	mapJob,ok := <- c.MapJobList
	// log.Printf("get a new worker need job and ok : %v\n",ok)
	if ok == true {
		c.m.Lock()
		reply.JobType = 0
		reply.Filename = mapJob
		reply.FileNo_ = c.fileNumber[mapJob]
		reply.NReduce = c.NReduce
		c.MapJobState[mapJob] = 1
		c.m.Unlock()
		// log.Printf("A map job.")
		go c.Timer(mapJob, -1)
		return nil
	}

	c.m.Lock()
	if c.MapFinished < c.fileSum {
		c.m.Unlock()
		reply.JobType = -1
		return nil
	}
	c.m.Unlock()

	reduceJob,ok1 := <- c.ReduceJobList
	// log.Printf("get a new worker need job and ok1 : %v\n",ok1)
	if ok1 == true {
		c.m.Lock()
		reply.JobType = 1
		reply.FileNo_ = c.fileSum
		reply.NReduce = reduceJob
		c.ReduceJobState[reduceJob] = 1
		c.m.Unlock()
		// log.Printf("A reduce job.")
		go c.Timer("", reduceJob)
		return nil
	}

	c.m.Lock()
	if c.ReduceFinished < c.NReduce {
		c.m.Unlock()
		reply.JobType = -1
		return nil
	}
	c.m.Unlock()

	reply.JobType = -2
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
	ret := false

	// Your code here.
	if c.ReduceFinished == c.NReduce && c.MapFinished == c.fileSum {
		ret = true
	}

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
	c.files = files
	c.fileSum = len(files)
	c.NReduce = nReduce

	c.MapJobState = make(map[string]int)
	c.fileNumber = make(map[string]int)
	for i,filename := range c.files {
		c.MapJobState[filename] = 0
		c.fileNumber[filename] = i
	}
	c.ReduceJobState = make(map[int]int)
	for i := 0; i < c.NReduce; i ++  {
		c.ReduceJobState[i] = 0
	}

	c.MapJobList = make(chan string)
	c.ReduceJobList = make(chan int)
	go c.PushJob()

	c.server()
	return &c
}
