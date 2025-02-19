package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu           sync.Mutex
	nReduce      int
	files2number map[string]int

	// --Map
	doneMapTasks map[int]bool // keeps track if a map task is done
	// keeps track of when each map task was assigned
	// to retry
	mapTimestamps map[int]*time.Time

	// --Reduce
	doneReduceTasks map[int]bool // keeps track if a reduce task is done
	// keeps track of when each reduce task was assigned
	// to retry
	reduceTimestamps map[int]*time.Time
}

const DEBUG bool = false

type TaskType int

const (
	INVALID_TASK TaskType = iota
	MAP_TASK     TaskType = iota
	REDUCE_TASK  TaskType = iota
)

type RequestType int

const (
	INVALID_REQUEST RequestType = iota
	GET_TASK        RequestType = iota
	ACK_MAP         RequestType = iota
	ACK_REDUCE      RequestType = iota
)

// Helpers
func (c *Coordinator) getMapTask(args *Args, reply *Reply) bool {
	if args.RequestType != GET_TASK {
		log.Fatal("Wrong request. \n")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for filename, taskNumber := range c.files2number {

		// Map task is yet to be completed
		if !c.doneMapTasks[taskNumber] {
			current_time := time.Now()

			// If more than 10s have passed, try it again
			if c.mapTimestamps[taskNumber] == nil ||
				current_time.Sub(*c.mapTimestamps[taskNumber]) >= 10*time.Second {

				reply.TaskType = MAP_TASK
				reply.Filename = filename
				reply.TaskNumber = taskNumber
				reply.NReduce = c.nReduce
				reply.NMaps = len(c.files2number)
				c.mapTimestamps[taskNumber] = &current_time

				return true
			}
		}
	}

	return false
}

func (c *Coordinator) mapIsDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, taskNumber := range c.files2number {
		if !c.doneMapTasks[taskNumber] {
			return false
		}
	}
	return true
}

func (c *Coordinator) reduceIsDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for taskNumber := range c.nReduce {
		taskNumber++
		if !c.doneReduceTasks[taskNumber] {
			return false
		}
	}
	return true
}

func (c *Coordinator) getReduceTask(args *Args, reply *Reply) bool {
	if args.RequestType != GET_TASK {
		log.Fatal("Wrong request. \n")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for taskNumber := range c.nReduce {
		// avoid using 0
		taskNumber++
		// Reduce task is yet to be completed
		if !c.doneReduceTasks[taskNumber] {
			current_time := time.Now()

			// If more than 10s have passed, try it again
			if c.reduceTimestamps[taskNumber] == nil ||
				current_time.Sub(*c.reduceTimestamps[taskNumber]) >= 10*time.Second {

				reply.TaskType = REDUCE_TASK
				reply.Filename = ""
				reply.TaskNumber = taskNumber
				reply.NReduce = c.nReduce
				reply.NMaps = len(c.files2number)
				c.reduceTimestamps[taskNumber] = &current_time

				return true
			}
		}
	}
	return false
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	if args.RequestType != GET_TASK {
		log.Fatal("Wrong request. \n")
	}

	if DEBUG && c.Done() {
		return fmt.Errorf("Done with tasks")
	}

	for {

		if c.getMapTask(args, reply) {

			if DEBUG {
				fmt.Printf("gets a map task. reply = %v\n", *reply)
			}

			return nil
		}

		if c.mapIsDone() && c.getReduceTask(args, reply) {

			if DEBUG {
				fmt.Printf("gets a reduce task. reply = %v\n", *reply)
			}

			return nil
		}

		// Allow to retry request in case others are busy
		time.Sleep(time.Second)
	}

}

func (c *Coordinator) AckMapTask(args *Args, reply *Reply) error {
	if args.RequestType != ACK_MAP {
		log.Fatal("Wrong request. \n")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.doneMapTasks[args.TaskNumber] = true
	if DEBUG {
		fmt.Printf("tasks done %v\n", c.doneMapTasks)
	}

	return nil
}

func (c *Coordinator) AckReduceTask(args *Args, reply *Reply) error {
	if args.RequestType != ACK_REDUCE {
		log.Fatal("Wrong request. \n")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.doneReduceTasks[args.TaskNumber] = true

	if DEBUG {
		fmt.Printf("tasks done %v\n", c.doneReduceTasks)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.mapIsDone() && c.reduceIsDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.files2number = make(map[string]int)
	c.nReduce = nReduce
	c.doneMapTasks = make(map[int]bool)
	c.mapTimestamps = make(map[int]*time.Time)
	c.doneReduceTasks = make(map[int]bool)
	c.reduceTimestamps = make(map[int]*time.Time)

	for i := range files {
		// avoid using 0
		taskNumber := i + 1
		c.files2number[files[i]] = taskNumber
		c.doneMapTasks[taskNumber] = false
		c.mapTimestamps[taskNumber] = nil
	}

	for i := range nReduce {
		// avoid using 0
		taskNumber := i + 1
		c.doneReduceTasks[taskNumber] = false
		c.reduceTimestamps[taskNumber] = nil
	}

	c.server()
	return &c
}
