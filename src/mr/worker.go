package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Ask for a task
		args := MakeArgs()
		args.RequestType = GET_TASK
		args.TaskNumber = -1

		reply := MakeReply()

		if DEBUG {
			fmt.Println("asking for a task")
		}

		ok := call("Coordinator.GetTask", args, reply)
		if ok {

			if DEBUG {
				fmt.Printf("reply.taskType %v\n", reply.TaskType)
				fmt.Printf("reply.filename %v\n", reply.Filename)
				fmt.Printf("reply.taskNumber %v\n", reply.TaskNumber)
				fmt.Printf("reply.nReduce %v\n", reply.NReduce)
				fmt.Printf("reply.nMaps %v\n", reply.NMaps)
				fmt.Printf("reply %v\n", reply)
			}

		} else {
			fmt.Printf("call for task has failed!\n")
			break
		}

		// Map task
		if reply.TaskType == MAP_TASK {

			if DEBUG {
				fmt.Printf("beginning map task\n")
			}

			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))

			reduceTasks2file := make([]*os.File, reply.NReduce)
			for i := range reduceTasks2file {
				temp_file, err := os.CreateTemp("", "example")
				if err != nil {
					log.Fatal(err)
				}
				reduceTasks2file[i] = temp_file
			}

			// Match KV to temp file
			for _, kv := range kva {
				reduceTask := ihash(kv.Key) % reply.NReduce

				temp_file := reduceTasks2file[reduceTask]

				enc := json.NewEncoder(temp_file)
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatal(err)
				}
			}

			// Rename atomically files
			for reduceTask, temp_file := range reduceTasks2file {
				// avoid using 0
				reduceTask++

				err := os.Rename(temp_file.Name(), "mr-"+strconv.Itoa(reply.TaskNumber)+"-"+strconv.Itoa(reduceTask))
				if err != nil {
					log.Fatal(err)
				}
				temp_file.Close()
			}

			// Acknowledge end of map task
			args = MakeArgs()
			args.RequestType = ACK_MAP
			args.TaskNumber = reply.TaskNumber
			reply = MakeReply()

			ok := call("Coordinator.AckMapTask", &args, &reply)
			if !ok {
				fmt.Printf("call failed!\n")
			}

			if DEBUG {
				fmt.Printf("done with map task %v\n", args.TaskNumber)
			}

			// Reduce task
		} else if reply.TaskType == REDUCE_TASK {

			if DEBUG {
				fmt.Printf("beginning reduce task\n")
			}

			// Collect intermediate files
			intermediate := make([]KeyValue, 0)
			for mapTaskNumber := range reply.NMaps {
				mapTaskNumber++
				filename := "mr-" + strconv.Itoa(mapTaskNumber) + "-" + strconv.Itoa(reply.TaskNumber)

				// Get the current working directory.
				currentDir, err := os.Getwd()
				if err != nil {
					fmt.Println("Error getting current directory:", err)
					return
				}

				// Construct the full file path.
				filename = filepath.Join(currentDir, filename)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			// Sort intermediate files
			sort.Sort(ByKey(intermediate))

			// Reduce function
			oname := "mr-out-" + strconv.Itoa(reply.TaskNumber)
			ofile, _ := os.Create(oname)

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

			ofile.Close()

			// Acknowledge end of reduce task
			args = MakeArgs()
			args.RequestType = ACK_REDUCE
			args.TaskNumber = reply.TaskNumber
			reply = MakeReply()

			ok := call("Coordinator.AckReduceTask", &args, &reply)
			if !ok {
				fmt.Printf("call failed!\n")
			}

			if DEBUG {
				fmt.Printf("done with reduce task %v\n", args.TaskNumber)
			}

			// Invalid task
		} else {
			fmt.Printf("invalid task type!, type = %v\n", reply.TaskType)
		}
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
