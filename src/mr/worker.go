package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
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

const (
	mr_mid_out = "mr-mid-%d"
	mr_out     = "mr-out-%d"
)

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

	fmt.Printf("worker start\n")

	for {
		reply, err := CallAskTask(&AskTaskArgs{})
		if err != nil {
			continue
		}
		if reply.Done {
			break
		}
		if reply.TaskType == TaskWait {
			time.Sleep(time.Second)
			continue
		}

		w := &worker{
			AskTaskReply: reply,
			mapf:         mapf,
			reducef:      reducef,
		}
		fileName, err := w.Do()
		if err != nil {
			fmt.Printf("worker do err = %v\n", err)
			continue
		}
		_, _ = CallFinishTask(&FinishTaskArgs{
			TaskType:    reply.TaskType,
			Index:       reply.Index,
			ResFileName: fileName,
		})

		//time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("worker done\n")
}

type worker struct {
	*AskTaskReply
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) Do() (fileName string, err error) {
	switch w.TaskType {
	case TaskTypeMap:
		return w.m()
	case TaskTypeReduce:
		return w.r()
	case TaskWait:
		time.Sleep(100 * time.Millisecond)
		return
	default:
		return "", errors.New("invalid task type")
	}
}

func (w *worker) m() (fileName string, err error) {
	content, err := os.ReadFile(w.FileName)
	if err != nil {
		return
	}

	kvs := w.mapf(w.FileName, string(content))
	sb := strings.Builder{}
	for _, kv := range kvs {
		sb.WriteString(kv.Key)
		sb.WriteString(" ")
		sb.WriteString(kv.Value)
		sb.WriteString("\n")
	}

	fileName = fmt.Sprintf(mr_mid_out, w.Index)
	os.Remove(fileName)
	err = ioutil.WriteFile(fileName, []byte(sb.String()), 0666)
	return
}

func (w *worker) r() (fileName string, err error) {
	content, err := os.ReadFile(w.FileName)
	if err != nil {
		return
	}

	words := strings.Split(string(content), " ")
	if len(words) < 1 {
		return "", errors.New("invalid file")
	}
	//fmt.Println(words)

	res := w.reducef(words[0], words[1:])
	fileName = fmt.Sprintf(mr_out, w.Index)
	os.Remove(fileName)
	err = ioutil.WriteFile(fileName, []byte(words[0]+" "+res), 0666)
	return
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

func CallAskTask(args *AskTaskArgs) (reply *AskTaskReply, err error) {
	reply = &AskTaskReply{}
	ok := call("Coordinator.AskTask", args, reply)
	if !ok {
		return nil, errors.New("call ask task error")
	}
	//fmt.Printf("call ask task res:%v\n", reply)
	return
}

func CallFinishTask(args *FinishTaskArgs) (reply *FinishTaskReply, err error) {
	reply = &FinishTaskReply{}
	ok := call("Coordinator.FinishTask", args, reply)
	if !ok {
		fmt.Printf("call finish task error")
	}
	//fmt.Printf("call ask task res:%v\n", reply)
	return
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

func Read() {
	fmt.Println(os.Getwd())
	content, err := os.ReadFile("pg-being_ernest.txt")
	fmt.Println(content, err)
}
