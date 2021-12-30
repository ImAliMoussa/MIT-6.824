package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		response := AskForWork()
		log.Println(response)
		if response.CommandType == MAP {
			mapFile(response.FileToMap)
		} else if response.CommandType == REDUCE {
			reduceFile(response.ReducePos)
		} else if response.CommandType == WAIT {
			time.Sleep(100 * time.Millisecond)
		} else if response.CommandType == EXIT {
			break
		} else {
			panic("Wrong code")
		}
	}
}

func mapFile(fileToMap string) {
	log.Println("Map file : ", fileToMap)
}

func reduceFile(reducePos int) {
	log.Printf("Reducing %d\n", reducePos)
}

func AskForWork() WorkerResponse {
	args := WorkerRequest{}
	reply := WorkerResponse{}
	call("Coordinator.Work", &args, &reply)
	log.Printf("Coordinator gave worker job %d\n", reply.CommandType)
	return reply
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
