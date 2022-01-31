package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapState struct {
	allFiles         []string
	readyFiles       map[string]bool
	inProgressFiles  map[string]bool
	mapPhaseFinished bool
}

type ReduceState struct {
	nReduce             int
	readyTasks          map[int]bool
	inProgressTasks     map[int]bool
	reducePhaseFinished bool
}

type Coordinator struct {
	// Your definitions here.
	mu     sync.Mutex
	mState MapState
	rState ReduceState
}

func (c *Coordinator) Work(args *WorkerRequest, reply *WorkerResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.rState.reducePhaseFinished {
		reply.CommandType = EXIT
	} else if c.mState.mapPhaseFinished {
		// Reduce
		if len(c.rState.readyTasks) == 0 {
			reply.CommandType = WAIT
		} else {
			reduceTask := c.getTaskFromReduceReadyMap()

			reply.CommandType = REDUCE
			reply.ReducePos = reduceTask
			reply.NReduce = c.rState.nReduce

			if reduceTask < 1 || reduceTask > c.rState.nReduce {
				panic("Wrong reduceTask position")
			}

			go time.AfterFunc(10*time.Second, func() {
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.rState.inProgressTasks[reduceTask] {
					c.addTaskBackToReady(reduceTask)
				}
			})
		}
	} else {
		// Map
		if len(c.mState.readyFiles) == 0 {
			reply.CommandType = WAIT
		} else {
			readyFile := c.getFileFromMapReadyMap()

			reply.CommandType = MAP
			reply.FileToMap = readyFile
			reply.NReduce = c.rState.nReduce
			reply.MapTask = c.findPosOfFile(readyFile)

			go time.AfterFunc(10*time.Second, func() {
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.mState.inProgressFiles[readyFile] {
					log.Println("Adding", readyFile, "back to Ready because it wasn't completed")
					c.addMapFileBackToReady(readyFile)
				}
			})
		}
	}
	return nil
}

func (c *Coordinator) ReportWorkDone(args *WorkerDoneRequest, reply *WorkerDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.CommandType == MAP {
		delete(c.mState.inProgressFiles, args.MappedFile)
		remainingMaps := c.getRemainingMaps()
		log.Println("Remaining maps", remainingMaps)
		if remainingMaps == 0 {
			c.mState.mapPhaseFinished = true
		}
	} else if args.CommandType == REDUCE {
		delete(c.rState.inProgressTasks, args.ReduceTaskFinished)
		remainingTasks := c.getRemainingTasks()
		log.Println("Remaining tasks", remainingTasks)
		if remainingTasks == 0 {
			c.rState.reducePhaseFinished = true
		}
	} else {
		return errors.New("Wrong Worker Done Request")
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.rState.reducePhaseFinished
}

func (c *Coordinator) initDS() {
	for _, file := range c.mState.allFiles {
		c.mState.readyFiles[file] = true
	}

	for i := 1; i <= c.rState.nReduce; i++ {
		c.rState.readyTasks[i] = true
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mState := MapState{
		allFiles:        files,
		readyFiles:      make(map[string]bool),
		inProgressFiles: make(map[string]bool),
	}

	rState := ReduceState{
		nReduce:         nReduce,
		readyTasks:      make(map[int]bool),
		inProgressTasks: make(map[int]bool),
	}

	c := Coordinator{
		mState: mState,
		rState: rState,
	}

	c.initDS()

	log.Println("Inside MakeCoordinator", files, nReduce)

	c.server()
	return &c
}
