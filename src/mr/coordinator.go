package mr

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	status      coordinatorStatus // 1:Map 2:reduce 3:done
	nReduce     int
	inputFiles  []string
	idle        map[int64]*taskInfo
	inProgress  map[int64]*taskInfo
	finish      map[int64]*taskInfo
	midFiles    []string
	outputFiles []string
	taskLock    sync.Mutex
}

type taskInfo struct {
	TaskType  TaskType
	FileName  string
	Index     int64
	StartTime int64
}

type coordinatorStatus int32

const (
	Init   coordinatorStatus = 0
	Map    coordinatorStatus = 1
	Reduce coordinatorStatus = 2
	Done   coordinatorStatus = 3
)

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

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	//fmt.Printf("coordinator :%v", c)
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	if c.status == Done {
		reply.Done = true
		return nil
	}

	//defer fmt.Printf("ask task res :%v", reply)
	if c.status == Init || len(c.idle) == 0 {
		reply.TaskType = TaskWait
		return nil
	}

	for k, v := range c.idle {
		reply.TaskType = v.TaskType
		reply.Index = k
		reply.FileName = v.FileName

		delete(c.idle, k)
		v.StartTime = time.Now().Unix()
		c.inProgress[k] = v

		break
	}

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if c.status != Reduce && c.status != Map {
		return errors.New("invalid task")
	}

	c.taskLock.Lock()
	if (c.status == Reduce && args.TaskType != TaskTypeReduce) || (c.status == Map && args.TaskType != TaskTypeMap) ||
		(args.TaskType != TaskTypeReduce && args.TaskType != TaskTypeMap) {
		return errors.New("invalid task")
	}

	if v, ok := c.inProgress[args.Index]; !ok {
		return errors.New("invalid task")
	} else {
		delete(c.inProgress, args.Index)
		c.finish[args.Index] = v
	}
	if c.status == Map {
		c.midFiles = append(c.midFiles, args.ResFileName)
	} else if c.status == Reduce {
		c.outputFiles = append(c.outputFiles, args.ResFileName)
	}

	c.taskLock.Unlock()

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
	c.taskLock.Lock()
	defer c.taskLock.Unlock()

	// Your code here.
	if c.status == Done {
		ret = true
	}

	return ret
}

func (c *Coordinator) init() {
	c.idle = make(map[int64]*taskInfo)
	c.inProgress = make(map[int64]*taskInfo)
	c.finish = make(map[int64]*taskInfo)
	for i, file := range c.inputFiles {
		c.idle[int64(i)] = &taskInfo{
			TaskType:  TaskTypeMap,
			FileName:  file,
			Index:     int64(i),
			StartTime: 0,
		}
	}

	c.status = Map

	// 异步操作
	go c.process()

	go c.statusLog()

}

func (c *Coordinator) process() {
	for {
		c.taskLock.Lock()
		// 检查是否有长期进行中的任务，即下发后一直未完成的任务
		if c.status == Map || c.status == Reduce {
			now := time.Now().Unix()
			for k, v := range c.inProgress {
				if now-v.StartTime > 5 {
					delete(c.inProgress, k)
					c.idle[k] = v
				}
			}
		}

		// 从map状态切换到reduce状态
		if c.status == Map && len(c.idle) == 0 && len(c.inProgress) == 0 {
			c.idle = make(map[int64]*taskInfo)
			c.inProgress = make(map[int64]*taskInfo)
			c.finish = make(map[int64]*taskInfo)

			c.genReduceTask()

			c.status = Reduce
		}

		// 从reduce状态切换到done
		if c.status == Reduce && len(c.idle) == 0 && len(c.inProgress) == 0 {
			c.status = Done

			//fmt.Println(c.outputFiles)
		}
		c.taskLock.Unlock()

		time.Sleep(time.Second)
	}
}

func (c *Coordinator) statusLog() {
	for {
		c.taskLock.Lock()
		fmt.Printf("status: %d idle: %d process:%d finish:%d midfile:%d outputfile:%d", c.status, len(c.idle), len(c.inProgress), len(c.finish), len(c.midFiles), len(c.outputFiles))
		c.taskLock.Unlock()

		time.Sleep(5 * time.Minute)
	}
}

const mr_reduce = "mr-reduce-%d"

func (c *Coordinator) genReduceTask() {
	m := make(map[string][]string)
	for _, f := range c.midFiles {
		content, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		cs := strings.Split(string(content), "\n")
		for _, tmp := range cs {
			kv := strings.Split(tmp, " ")
			if len(kv) != 2 {
				continue
			}
			m[kv[0]] = append(m[kv[0]], kv[1])
		}
	}

	idlFiles := make([]string, 0)
	i := 0
	for k, v := range m {
		reduceFileName := fmt.Sprintf(mr_reduce, i)
		sb := strings.Builder{}
		sb.WriteString(k + " ")
		for idx, v1 := range v {
			sb.WriteString(v1)
			if idx != len(v)-1 {
				sb.WriteString(" ")
			}
		}
		os.WriteFile(reduceFileName, []byte(sb.String()), 0666)
		idlFiles = append(idlFiles, reduceFileName)
		i++
	}
	fmt.Println(len(idlFiles))

	for i, file := range idlFiles {
		c.idle[int64(i)] = &taskInfo{
			TaskType:  TaskTypeReduce,
			FileName:  file,
			Index:     int64(i),
			StartTime: 0,
		}
	}
}

func Test() {
	c := &Coordinator{
		idle:       make(map[int64]*taskInfo),
		inProgress: make(map[int64]*taskInfo),
		finish:     make(map[int64]*taskInfo),
		midFiles: []string{
			"mr-tmp/mr-mid-0",
			"mr-tmp/mr-mid-1",
			"mr-tmp/mr-mid-2",
			"mr-tmp/mr-mid-3",
			"mr-tmp/mr-mid-4",
			"mr-tmp/mr-mid-5",
			"mr-tmp/mr-mid-6",
			"mr-tmp/mr-mid-7",
		},
		outputFiles: []string{},
		taskLock:    sync.Mutex{},
	}
	c.genReduceTask()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		status:     Init,
		nReduce:    nReduce,
		inputFiles: files,
	}

	// Your code here.
	c.init()

	c.server()
	return &c
}
