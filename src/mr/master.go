package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Status int

const (
	NotStarted Status = iota
	InProgress
	Complete
)

type Task struct {
	// Task id.
	id int
	// input file path for map task. output file path for reduce task.
	filePath string
	// task status.
	status Status
	// task duration.
	duration time.Duration
	// client process id. FilePath are and clientPid are used together to check which client is the current running one.
	clientPid int
	// Used for reducer task to indicate whether reduce key is bucketed.
	bucketed bool
}

type Master struct {
	// Your definitions here.
	// mapTasks key is input path, value indicates whether map job is done or not.
	mapTasks []Task
	// reduceTasks key is the key emitted from mapper, value indicates whether reduce job is done or not.
	reduceTasks []Task
	// maximum number of reducer.
	numReducer int
	// Lock ofr master.
	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) AllocateMapper(args *AllocateWorkerRequest, reply *AllocateWorkerResponse) error {
	m.mutex.Lock()
	for index, task := range m.mapTasks {
		if task.status == NotStarted && task.bucketed {
			reply.Id = task.id
			reply.NumReducer = m.numReducer
			reply.FilePath = task.filePath
			task.status = InProgress
			task.duration = 0
			task.clientPid = args.ClientPid
			m.mapTasks[index] = task
			break
		}
	}
	m.mutex.Unlock()
	return nil
}

func (m *Master) AllocateReducer(args *AllocateWorkerRequest, reply *AllocateWorkerResponse) error {
	m.mutex.Lock()
	for index, task := range m.reduceTasks {
		if task.status == NotStarted && task.bucketed {
			reply.Id = task.id
			reply.NumReducer = m.numReducer
			reply.FilePath = task.filePath
			task.duration = 0
			task.status = InProgress
			task.clientPid = args.ClientPid
			m.reduceTasks[index] = task
			break
		}
	}
	m.mutex.Unlock()
	return nil
}

func (m *Master) MapTaskDone(args *DoneRequest, reply *DoneResponse) error {
	for index, task := range m.mapTasks {
		if task.filePath == args.FilePath && task.clientPid == args.ClientPid {
			task.status = Complete
			m.mapTasks[index] = task
			for _, bucket := range args.Buckets {
				m.reduceTasks[bucket].bucketed = true
			}
			break
		}
	}
	return nil
}

func (m *Master) ReduceTaskDone(args *DoneRequest, reply *DoneResponse) error {
	for index, task := range m.reduceTasks {
		if task.filePath == args.FilePath && task.clientPid == args.ClientPid {
			task.status = Complete
			m.reduceTasks[index] = task
			break
		}
	}
	return nil
}

func (m *Master) IsMapperDone(args *IsDoneRequest, reply *IsDoneResponse) error {
	reply.Done = m.getStatus(m.mapTasks) == Complete
	return nil
}

func (m *Master) IsReducerDone(args *IsDoneRequest, reply *IsDoneResponse) error {
	reply.Done = m.getStatus(m.reduceTasks) == Complete
	return nil
}

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
// When both mapTasks and reduceTasks values are true, it means MR job is done.
func (m *Master) Done() bool {
	// Your code here.
	updateTasksDuration(m.mapTasks)
	updateTasksDuration(m.reduceTasks)
	mapStatus := m.getStatus(m.mapTasks)
	reduceStatus := m.getStatus(m.reduceTasks)
	return mapStatus == Complete && reduceStatus == Complete
}

// Update task duration per second. If duration is more than 10 seconds, consider the task as crashed so that client can create a new task.
func updateTasksDuration(tasks []Task) {
	for index, task := range tasks {
		if task.status == InProgress {
			task.duration = task.duration + 1
			if task.duration > 10 {
				task.duration = 0
				task.clientPid = 0
				task.status = NotStarted
			}
			tasks[index] = task
		}
	}
}

func (m *Master) getStatus(tasks []Task) Status {
	statusSet := map[Status]bool{}
	for _, task := range tasks {
		if task.bucketed {
			statusSet[task.status] = true
		}
	}
	if len(statusSet) == 1 && statusSet[NotStarted] {
		return NotStarted
	}
	if len(statusSet) == 1 && statusSet[Complete] {
		return Complete
	}
	return InProgress
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
// Initilize the instance of Master.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	for index, fileName := range files {
		mapTask := Task{id: index, filePath: fileName, status: NotStarted, duration: 0, bucketed: true}
		m.mapTasks = append(m.mapTasks, mapTask)
	}
	for i := 0; i < nReduce; i++ {
		reduceTask := Task{id: i, filePath: "mr-out-" + strconv.Itoa(i), status: NotStarted, duration: 0, bucketed: false}
		m.reduceTasks = append(m.reduceTasks, reduceTask)
	}
	m.numReducer = nReduce

	m.server()
	return &m
}
