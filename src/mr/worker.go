package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
	"path/filepath"
)
import "log"
import "net/rpc"
import "hash/fnv"


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
	for !IsMapperDone() {
		MapperWork(mapf)
		time.Sleep(time.Second)
	}
	for !IsReducerDone() {
		ReducerWork(reducef)
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func MapperWork(mapf func(string, string) []KeyValue) {
	pid := os.Getpid()
	workerRequest := AllocateWorkerRequest{ClientPid: pid}
	workerResponse := AllocateWorkerResponse{}
	call("Master.AllocateMapper", &workerRequest, &workerResponse)
	// Do nothing when no map task is available.
	if len(workerResponse.FilePath) == 0 {
		return
	}
	inputFilePath := workerResponse.FilePath
	numReducer := workerResponse.NumReducer
	mapId := workerResponse.Id
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("cannot open %v", inputFilePath)
	}
	content, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile)
	}
	defer inputFile.Close()
	kva := mapf(inputFilePath, string(content))
	intermediateOutputMap := map[int][]KeyValue{}
	for _, kv := range kva {
		bucket := ihash(kv.Key) % numReducer
		intermediateOutputMap[bucket] = append(intermediateOutputMap[bucket], kv)
	}
	buckets := []int{}
	for bucket, keyValueList := range intermediateOutputMap {
		buckets = append(buckets, bucket)
		outputFilePath := "mr-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(bucket)
		outputFile, _ := os.Create(outputFilePath)
		enc := json.NewEncoder(outputFile)
		for _, kv := range keyValueList {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to %v", outputFile.Name())
			}
		}
	}
	mapDoneRequest := DoneRequest{FilePath: inputFilePath, ClientPid: pid, Buckets: buckets}
	mapDoneResponse := DoneResponse{}
	call("Master.MapTaskDone", &mapDoneRequest, &mapDoneResponse)
}

func ReducerWork(reducef func(string, []string) string) {
	pid := os.Getpid()
	workerRequest := AllocateWorkerRequest{ClientPid: pid}
	workerResponse := AllocateWorkerResponse{}
	call("Master.AllocateReducer", &workerRequest, &workerResponse)
	// Do nothing when no reduce task is available.
	if len(workerResponse.FilePath) == 0 {
		return
	}
	outputFilePath := workerResponse.FilePath
	reduceId := workerResponse.Id
	inputFilePaths, _ := filepath.Glob("mr-[0-9]-" + strconv.Itoa(reduceId))
	if inputFilePaths != nil {
		reducerMap := map[string][]string {}
		for _, inputFilePath := range inputFilePaths {
			inputFile, err := os.Open(inputFilePath)
			if err != nil {
				log.Fatalf("cannot open %v", outputFilePath)
			}
			defer inputFile.Close()
			dec := json.NewDecoder(inputFile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				reducerMap[kv.Key] = append(reducerMap[kv.Key], kv.Value)
			}
		}
		outputFile, _ := os.Create(outputFilePath)
		for key, value := range reducerMap {
			output := reducef(key, value)
			fmt.Fprintf(outputFile, "%v %v\n", key, output)
		}
		defer outputFile.Close()
	}
	reduceDoneRequest := DoneRequest{FilePath: outputFilePath, ClientPid: pid}
	reduceDoneResponse := DoneResponse{}
	call("Master.ReduceTaskDone", &reduceDoneRequest, &reduceDoneResponse)
}

func IsMapperDone() bool {
	args := IsDoneRequest{}
	reply := IsDoneResponse{Done: false}
	call("Master.IsMapperDone", &args, &reply)
	return reply.Done
}

func IsReducerDone() bool {
	args := IsDoneRequest{}
	reply := IsDoneResponse{}
	call("Master.IsReducerDone", &args, &reply)
	return reply.Done
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
