package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"strconv"
	"sort"
)
import "io/ioutil"
import "os"
import "encoding/json"
import "time"
import "sync"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int {
	 return len(a) 
}
func (a ByKey) Swap(i, j int) {
	 a[i], a[j] = a[j], a[i] 
}
func (a ByKey) Less(i, j int) bool { 
	return a[i].Key < a[j].Key 
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
	// My implementation is here
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
	for  {
		fetchTask := FetchTask{}
		response := RespondTask{}
		task := call("Coordinator.FetchTask", &fetchTask, &response)
		if task {

			if response.Task == Map {
				doMap(mapf, &response)
			} else if response.Task == Reduce {
				doReduce(reducef, &response)
			} else if response.Task == Wait {
				//time.Sleep(1 * time.Second)
				//println("waiting for task")
			} else {
				return
			}

		}
		time.Sleep(1 * time.Second)

	}
}


func doMap(mapf func(string, string) []KeyValue, args *RespondTask) {
	//println(0)
	file, err := os.Open(args.FileName)
	var enc []*json.Encoder

	if err != nil {
		log.Fatalf("cannot open %v", args.FileName)
	}
	lines, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", args.FileName)
	}
	file.Close()
	result := mapf(args.FileName, string(lines))

	files := make(map[int][]string)
	for i := 0; i < args.NumWork; i++ {
		file := "mr-" + strconv.Itoa(args.FileIndex) + "-" + strconv.Itoa(i)
		if _, exists := files[args.FileIndex]; !exists {
            files[args.FileIndex] = []string{}
        }
		files[args.FileIndex] = append(files[args.FileIndex], file)
		f, err := os.Create(file)
		if err != nil {
			log.Fatalf("CAN'T CREATE INTERFILE")
		}
		enc = append(enc, json.NewEncoder(f))
	}

	for _, keyvalue := range result {
		enc[ihash(keyvalue.Key)%args.NumWork].Encode(&keyvalue)
	}
	
	newArgs := RespondReport {
		Done: true,
		FileIndex: args.FileIndex,
		Task: Map,
		Files: files,
	}
	response := FetchReport{}

	call("Coordinator.Finish", &newArgs, &response)

}

func doReduce(reducef func(string, []string) string, args *RespondTask){
	//println(1)
	var myMap []KeyValue
	var lock sync.Mutex
	var group sync.WaitGroup
	for i := range args.Files {
		group.Add(1)
		go func(fileName string) {
			defer group.Done()
			entry := helper(fileName)
			lock.Lock()
			myMap = append(myMap, entry...)
			lock.Unlock()
		}(args.Files[i])
	}
	group.Wait()
	sort.Sort(ByKey(myMap))
	fileName := fmt.Sprintf("mr-out-%d", args.FileIndex)
	tmp, err := os.CreateTemp(".", fileName)
	if err != nil {
		log.Fatal(err)
	}
	i := 0
	defer func() {
		tmp.Close()
	}()
	
	for i < len(myMap) {
		j := i + 1
		for j < len(myMap) && myMap[j].Key == myMap[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, myMap[k].Value)
		}
		output := reducef(myMap[i].Key, values)
		_, _ = fmt.Fprintf(tmp, "%v %v\n", myMap[i].Key, output)

		i = j
	}
	err = os.Rename(tmp.Name(), fileName)

	if err != nil {
		log.Fatal(err)
	}

	newArgs := RespondReport {
		Done: true,
		FileIndex: args.FileIndex,
		Task: Reduce,
	}
	response := FetchReport{}

	call("Coordinator.Finish", &newArgs, &response)
}





func helper(fileName string) []KeyValue{
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	
	var res []KeyValue
	dec := json.NewDecoder(f)
	for {
		var entry KeyValue
		if err := dec.Decode(&entry); err != nil {
			break
		}
		res = append(res, entry)
	}

	err2 := f.Close()

	if err2 != nil {
		log.Fatal(err2)
	}
	return res

	
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
