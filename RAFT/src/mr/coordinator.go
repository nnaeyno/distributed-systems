package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"fmt"
)

type Stat int64

const (
	Current Stat = iota
	Done
	Waiting
)
type Chore struct {
	Status Stat
	StartTime time.Time
	Tasks []string
	Index int
}

type Coordinator struct {
	// Your definitions here.
	MapDone bool
	ReduceDone bool
	allDone bool

	NumWork int
	MapFiles []*Chore
	ReduceFiles []*Chore
	lock sync.Mutex
	//Intermediate map[int][]string
	Interm map[int]struct{}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.lock.Lock()
	ret := c.allDone
	c.lock.Unlock()
	// Your code here.
	// that's all my code 
	//exit for coordinator
	//print("RETURNING DONE", ret)
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapDone: false,
		ReduceDone: false,
		allDone: false,

		NumWork: nReduce,
		//Intermediate: make(map[int][]string),
		Interm: make(map[int]struct{}),
		// MapFiles: make(map[int]*Chore),
		// ReduceFiles:  make(map[int]*Chore),
	}

	// Your code here.
	// we should create nReduce files to pass to Reduce, so divide keys by nReduce
	leng := len(files)
	for i := 0; i < leng; i++ {
		var f []string
		f = append(f, files[i])
		c.MapFiles = append(c.MapFiles, &Chore{
			Status: Waiting,
			StartTime: time.Now(),
			Tasks: f,
			Index: i,
		})
	}
	
	for i := 0; i < c.NumWork; i++ {
		c.ReduceFiles = append(c.ReduceFiles, &Chore{
			Status: Waiting,
			StartTime: time.Now(),
			Tasks: nil,
			Index: i,
		})
	}
	length := len(c.ReduceFiles)
	fmt.Println("Checking reduces", length)
	c.server()
	return &c
}



func (c *Coordinator) FetchTask(fetchTask *FetchTask, response *RespondTask) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	response.Task = Wait
	if !c.MapDone {
		for _, chore := range c.MapFiles {
			
			timeNow := time.Now()
			
			if chore.Status == Done {
				// c.lock.Unlock()
				// continue
			}
			overdue := chore.StartTime.Add(10 * time.Second).Before(timeNow)
			if chore.Status == Current &&  overdue {
				chore.Status = Waiting
			}
			if chore.Status == Waiting {
				chore.Status = Current
				chore.StartTime = timeNow
				//c.lock.Unlock()
				response.FileIndex = chore.Index
				response.FileName = chore.Tasks[len(chore.Tasks)-1]
				response.NumWork = c.NumWork
				response.Files = nil
				response.Task = Map
				
				return nil
				
			}
			//c.lock.Unlock()
		}

	} else if c.MapDone && !c.ReduceDone{
		for _, chore := range c.ReduceFiles {
		
			timeNow := time.Now()
			//c.lock.Lock()
			if chore.Status == Done {
				// c.lock.Unlock()
				// continue
			}
			overdue := chore.StartTime.Add(10 * time.Second).Before(timeNow)
			if chore.Status == Current &&  overdue {
				chore.Status = Waiting
			}
			if chore.Status == Waiting {
				chore.Status = Current
				chore.StartTime = timeNow	
				getFiles(c, response, chore.Index)
				//c.lock.Unlock()	
				response.FileIndex = chore.Index
				response.FileName = ""
				response.NumWork = c.NumWork	
				response.Task = Reduce
				
				return nil
				
			}
			//c.lock.Unlock()
		}
		

	} else {

	}
	return nil

}

func (c *Coordinator) Finish(args *RespondReport, _ *FetchReport) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.Task == Map {
		
		c.MapFiles[args.FileIndex].Status = Done
		// for _, chore := range c.MapFiles {
		// 	if chore.Index == args.FileIndex {
		// 		chore.Status = Done
		// 		// if _, exists := c.Intermediate[chore.Index]; !exists {
		// 		// 	c.Intermediate[chore.Index] = []string{}
		// 		// }
		// 		// c.Intermediate[chore.Index] = args.Files[chore.Index]
		// 		
		// 	}
		c.Interm[args.FileIndex] = struct{
		}{}
		for _, chore := range c.MapFiles {
			if chore.Status != Done {
				return nil
			}
		}
		c.MapDone = true
	} else {
		for _, chore := range c.ReduceFiles {
			if chore.Index == args.FileIndex {
				chore.Status = Done	
			}
		}

		for _, chore := range c.ReduceFiles {
			if chore.Status != Done {
				return nil
			}
		}
		//print("ALL DONE HAPPENED")
		c.ReduceDone = true
		c.allDone = true
	}
	return nil
}

func getFiles(c *Coordinator, resp *RespondTask, index int) {
	for i := range c.Interm {
		resp.Files = append(resp.Files, fmt.Sprintf("mr-%d-%d", i, index))
	}
}


//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
//make(map[KeyType]ValueType)
