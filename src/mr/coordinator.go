package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	filesMap map[string]bool
	nReduce  int
}

func (c *Coordinator) Work(args *WorkerRequest, reply *WorkerResponse) error {
	reply.CommandType = MAP
	reply.ReducePos = 2
	reply.FileToMap = "hello.txt"
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
	log.Println("Done was called")

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{filesMap: make(map[string]bool), nReduce: nReduce}

	// Your code here.

	log.Println("Inside MakeCoordinator", files, nReduce)

	c.nReduce = nReduce
	for _, file := range files {
		c.filesMap[file] = false
	}

	c.server()
	return &c
}
