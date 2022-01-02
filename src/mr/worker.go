package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			mapFile(response.NReduce, response.MapTask, response.FileToMap, mapf)
			ReportWorkDone(MAP, -1, response.FileToMap)
		} else if response.CommandType == REDUCE {
			reduceFile(response.ReducePos, response.NReduce, reducef)
			ReportWorkDone(REDUCE, response.ReducePos, "")
		} else if response.CommandType == WAIT {
			time.Sleep(100 * time.Millisecond)
		} else if response.CommandType == EXIT {
			break
		} else {
			panic("Wrong Map-Reduce Command")
		}
		if response.CommandType == MAP || response.CommandType == REDUCE {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func mapFile(nReduce, mapTask int, fileToMap string, mapf func(string, string) []KeyValue) {
	pid := os.Getpid()
	log.Printf("Worker %d: Mapping file : %s\n", pid, fileToMap)

	file, err := os.Open(fileToMap)
	if err != nil {
		log.Fatalf("cannot open %v", fileToMap)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileToMap)
	}
	file.Close()

	log.Printf("Worker %d started to map %d\n", pid, mapTask)
	kva := mapf(fileToMap, string(content))
	log.Printf("Worker %d has finished mapping %d\n", pid, mapTask)

	encoderMap := make(map[int]*json.Encoder)
	intermediateFiles := make(map[int]*os.File)

	for _, keyValue := range kva {
		taskNumber := ihash(keyValue.Key) % nReduce
		_, exists := encoderMap[taskNumber]
		if !exists {
			tmpFile, err := ioutil.TempFile("", "*.json")
			if err != nil {
				panic(err)
			}
			intermediateFiles[taskNumber] = tmpFile
			enc := json.NewEncoder(tmpFile)
			encoderMap[taskNumber] = enc
		}
		encoderMap[taskNumber].Encode(&keyValue)
	}
	for i, file := range intermediateFiles {
		filename := file.Name()
		err := file.Close()
		if err != nil {
			panic(err)
		}
		newFilename := fmt.Sprintf("mr-%d-%d", mapTask, i+1)
		os.Rename(filename, newFilename)
	}
}

func reduceFile(reducePos, nReduce int, reducef func(string, []string) string) {
	log.Printf("Reducing %d\n", reducePos)
	intermediate := []KeyValue{}
	for i := 1; i <= nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reducePos)
		file, err := os.OpenFile(filename, os.O_RDONLY, 0400)
		if os.IsNotExist(err) {
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reducePos)
	ofile, _ := os.Create(oname)

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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func AskForWork() WorkerResponse {
	pid := os.Getpid()
	log.Printf("Worker %d is asking for work\n", pid)
	args := WorkerRequest{}
	reply := WorkerResponse{}
	call("Coordinator.Work", &args, &reply)
	log.Printf("Worker %d has received work\n", pid)
	return reply
}

func ReportWorkDone(command MRCommand, reduceTask int, mapFile string) {
	pid := os.Getpid()
	log.Printf("Worker %d is going to report finishing work\n", pid)

	args := WorkerDoneRequest{
		CommandType:        command,
		MappedFile:         mapFile,
		ReduceTaskFinished: reduceTask,
	}
	reply := WorkerDoneResponse{}
	call("Coordinator.ReportWorkDone", &args, &reply)
	log.Printf("Worker %d has finished his work\n", pid)
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
