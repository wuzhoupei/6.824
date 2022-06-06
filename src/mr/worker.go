package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "strconv"
import "sort"
import "strings"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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


type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	jobNow := JobReply{}
	jobNow.JobType = -1

	for {
		// log.Printf("this is pre jobType : %v",jobNow.JobType)
		jobNow = JobRequest(jobNow)
		// log.Printf("this is jobType : %v",jobNow.JobType)
		if jobNow.JobType == -2 {
			break
		}

		if jobNow.JobType == -1 {
			time.Sleep(time.Millisecond * 5)
			continue 
		}

		// log.Printf("worker get a job.")
		// map : 
		if jobNow.JobType == 0 {
			// intermediate := []KeyValue{}
			file, err := os.Open(jobNow.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", jobNow.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", jobNow.Filename)
			}
			file.Close()
			kva := mapf(jobNow.Filename, string(content))
			// intermediate = append(intermediate, kva...)

			midFile := make([]*os.File, jobNow.NReduce)
			for i := 0; i < jobNow.NReduce; i ++ {
				name := "mr-mid-" + strconv.Itoa(jobNow.FileNo_) + 
						"-" + strconv.Itoa(i)
				midFile[i],_ = os.Create(name)
			}

			for i,kv := range kva {
				i = ihash(kv.Key) % jobNow.NReduce
				fmt.Fprintf(midFile[i], "%v %v\n", kv.Key, kv.Value)
			}

			for i := 0; i < jobNow.NReduce; i ++ {
				midFile[i].Close()
			}
		}

		// reduce :
		if jobNow.JobType == 1 {
			intermediate := []KeyValue{}
			midFile := make([]*os.File, jobNow.FileNo_)
			for i := 0; i < jobNow.FileNo_; i ++ {
				name := "mr-mid-" + strconv.Itoa(i) + 
						"-" + strconv.Itoa(jobNow.NReduce)
				
				var err error
				midFile[i], err = os.Open(name)
				defer midFile[i].Close()
				if err != nil {
					log.Printf("%v dont exit!", name)
					continue 
				}

				content, err := ioutil.ReadAll(midFile[i])
				if err != nil {
					log.Fatalf("cannot read %v", name)
				}
				kvp := strings.Split((string(content)), "\n")

				for _,kv := range kvp {
					kv0 := strings.Split(kv, " ")
					if len(kv0) < 2 {
						continue 
					}
					intermediate = append(intermediate, KeyValue{kv0[0],kv0[1]})
				}
			}

			sort.Sort(ByKey(intermediate))

			oname := "mr-out-" + strconv.Itoa(jobNow.NReduce)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

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

			// for i := 0; i < jobNow.FileNo_; i ++ {
			// 	name := "mr-mid-" + strconv.Itoa(i) + 
			// 			"-" + strconv.Itoa(jobNow.NReduce)

			// 	err := os.Remove(name)
			// 	if err != nil {
			// 		log.Printf("%v fail to remove!",name)
			// 	}
			// }
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func JobRequest(jobNow JobReply) JobReply {
	args := JobArgs{}

	args.JobType = jobNow.JobType
	args.Filename = jobNow.Filename
	args.NReduce = jobNow.NReduce
	
	reply := JobReply{}

	call("Coordinator.Job2Worker", &args, &reply)

	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

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

	fmt.Println(err)
	return false
}
