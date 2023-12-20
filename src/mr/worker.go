package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the bucket
// TaskObj number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// register the worker
	log.Println("Worker started!")
	worker, err := CallRegisterWorker()
	if err != nil {
		log.Println("Fail to register worker, assume the coordinator is gone and exit")
		os.Exit(1)
	}
	log.Printf("Worker registered to the coordinator with worder id %d\n", worker.WorkerId)
	// repeatedly request for TaskObj and inform finished
	for {
		task, err := CallRequestTask(worker)
		if err != nil {
			log.Println("Fail to request a TaskObj, assume the coordinator is gone and exit")
			os.Exit(1)
		} else {
			log.Printf("Received "+task.Type.String()+" Task %d!\n", task.TaskId)
		}
		switch task.Type {
		case MAP:
			handleMapTask(task.TaskId, task.Filename, task.NumOfBuckets, mapf)
			log.Printf(task.Type.String()+" Task %d is finished!\n", task.TaskId)
		case REDUCE:
			handleReduceTask(task.TaskId, task.NumOfFiles, reducef)
			log.Printf(task.Type.String()+" Task %d is finished!\n", task.TaskId)
		case EXIT:
			log.Println("Received EXIT signal from coordinator, exit")
			os.Exit(0)
		default:
			log.Fatalf("Received tasks of unknown type: %d\n", task.Type)
		}
		err = CallTaskFinished(worker, task)
		if err != nil {
			log.Println("Fail to inform the TaskObj finished, assume the coordinator is gone and exit")
			os.Exit(1)
		}
	}
}
func handleMapTask(taskId int, filename string, numOfBuckets int, mapf func(string, string) []KeyValue) {
	// open the input files and read contents
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// map into KV array and sort by key
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	// create an array of temporary output files and their encoders
	var tmpFiles []*os.File
	var encoders []*json.Encoder
	for i := 0; i < numOfBuckets; i++ {
		tmpFile, err := ioutil.TempFile("", fmt.Sprintf("temp-%d-%d-", taskId, i))
		if err != nil {
			log.Fatal(err)
		}
		tmpFiles = append(tmpFiles, tmpFile)
		encoders = append(encoders, json.NewEncoder(tmpFile))
	}
	// encode each kv to the correct output files
	for _, kv := range kva {
		bucketId := ihash(kv.Key) % numOfBuckets
		encoders[bucketId].Encode(&kv)
	}
	// close all files and atomic renaming
	for bucketId, tmpFile := range tmpFiles {
		if err := tmpFile.Close(); err != nil {
			log.Fatal(err)
		}
		os.Rename(tmpFile.Name(), fmt.Sprintf("mr-%d-%d", taskId, bucketId))
	}
}
func handleReduceTask(taskId int, numOfFiles int, reducef func(string, []string) string) {
	// create tmp file for write
	ofile, err := ioutil.TempFile("", fmt.Sprintf("temp-out-%d-", taskId))
	if err != nil {
		log.Fatal(err)
	}
	defer ofile.Close()
	reduceMap := make(map[string][]string)
	// for each file open the file, decode the file into KeyValue pairs, collect into map
	for i := 0; i < numOfFiles; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskId)
		ifile, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// reach the end of file and exit
				break
			}
			// Compute if absent the reduce map with the key
			if _, ok := reduceMap[kv.Key]; !ok {
				reduceMap[kv.Key] = []string{}
			}
			reduceMap[kv.Key] = append(reduceMap[kv.Key], kv.Value)
		}
	}
	// print out the map content into the output file
	for key, valueArray := range reduceMap {
		output := reducef(key, valueArray)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	// Close the file
	if err := ofile.Close(); err != nil {
		log.Fatal(err)
	}
	// atomic rename
	newFileName := fmt.Sprintf("mr-out-%d", taskId)
	if err := os.Rename(ofile.Name(), newFileName); err != nil {
		log.Fatal(err)
	}
}

// RPC callers

// register the worker to receive a worker id
func CallRegisterWorker() (*WorkerInfo, error) {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		worker := new(WorkerInfo)
		worker.WorkerId = reply.WorkerId
		log.Printf("register the worker with id %d\n", reply.WorkerId)
		return worker, nil
	} else {
		log.Fatalf("fail to register the worker!\n")
		return nil, errors.New("Failure to communicate!")
	}
}

// request a task from the coordinator
func CallRequestTask(worker *WorkerInfo) (*Task, error) {
	args := RequestArgs{worker.WorkerId}
	reply := RequestReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		log.Fatalf("worker %d fail to receive a TaskObj!\n", worker.WorkerId)
		return nil, errors.New("Failure to communicate!")
	}
	log.Printf("worker %d received the TaskObj %d with type %d\n",
		worker.WorkerId, reply.TaskObj.TaskId, reply.TaskObj.Type)
	return reply.TaskObj, nil
}

// inform the coordinator the finish of a task
func CallTaskFinished(worker *WorkerInfo, task *Task) error {
	args := FinishedArgs{worker.WorkerId, task.TaskId, task.Type} // type is needed to avoid confusion from network delay
	reply := FinishedReply{}
	ok := call("Coordinator.TaskFinished", &args, &reply)
	if !ok {
		log.Fatalf("worker %d fail to inform Task %d finished!\n", worker.WorkerId, task.TaskId)
		return nil
	}
	log.Printf("The finish of Task %d has been informed to coordinator\n", task.TaskId)
	return nil
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

	log.Println(err)
	return false
}
