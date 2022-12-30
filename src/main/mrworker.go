package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"6.824/mr"
	"fmt"
	"os"
	"sort"
	"strings"
)
import "plugin"
import "log"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	mr.Worker(mapf, reducef)
	//genReduceTask()
	//mr.Test()
}

func genReduceTask() {
	//midfiles := []string{"mr-mid-0", "mr-mid-1"}
	//m := make(map[string][]string)
	//for _, f := range midfiles {
	//	content, err := os.ReadFile(f)
	//	if err != nil {
	//		continue
	//	}
	//	cs := strings.Split(string(content), "\n")
	//	for _, tmp := range cs {
	//		kv := strings.Split(tmp, " ")
	//		if len(kv) != 2 {
	//			continue
	//		}
	//		m[kv[0]] = append(m[kv[0]], kv[1])
	//	}
	//}
	//fmt.Println(m)

	//idlFiles := make([]string, 0)
	//for k, v := range m {
	//	reduceFileName := fmt.Sprintf(mr_reduce, k)
	//	sb := strings.Builder{}
	//	sb.WriteString(k)
	//	for _, v1 := range v {
	//		sb.WriteString(v1)
	//	}
	//	os.WriteFile(reduceFileName, []byte(sb.String()), 0666)
	//	idlFiles = append(idlFiles, reduceFileName)
	//}

	//for i, file := range idlFiles {
	//	c.idle[int64(i)] = &taskInfo{
	//		TaskType:  TaskTypeReduce,
	//		FileName:  file,
	//		Index:     int64(i),
	//		StartTime: 0,
	//	}
	//}

	//content, err := os.ReadFile("mr-reduce-zithers")
	//if err != nil {
	//	return
	//}
	//
	//words := strings.Split(string(content), " ")
	//fmt.Println(words)

	//res := w.reducef(words[0], words[1:])
	//fileName = fmt.Sprintf(mr_out, w.Index)
	//os.Remove(fileName)
	//err = ioutil.WriteFile(fileName, []byte(words[0]+" "+res), 0666)

	//files := []string{
	//	"mr-mid-0",
	//	"mr-mid-1",
	//	"mr-mid-2",
	//	"mr-mid-3",
	//	"mr-mid-4",
	//	"mr-mid-5",
	//	"mr-mid-6",
	//	"mr-mid-7",
	//}
	//m := make(map[string]int)
	//for _, f := range files {
	//	content, _ := os.ReadFile("mr-tmp/" + f)
	//	cs := strings.Split(string(content), "\n")
	//	for _, c := range cs {
	//		c1 := strings.Split(c, " ")
	//		if len(c1) != 2 {
	//			continue
	//		}
	//		m[c1[0]] += 1
	//	}
	//}
	//sb := strings.Builder{}
	//ks := []string{}
	//for k, _ := range m {
	//	ks = append(ks, k)
	//}
	//sort.Strings(ks)
	//for _, k := range ks {
	//	sb.WriteString(k)
	//	sb.WriteString(" ")
	//	sb.WriteString(fmt.Sprintf("%d", m[k]))
	//	sb.WriteString("\n")
	//}
	//os.WriteFile("mr-out-out.txt", []byte(sb.String()), 0666)

	files := os.Args[1:]
	fmt.Println(files)
	m := make(map[string]int)
	for _, f := range files {
		content, _ := os.ReadFile(f)
		cs := strings.Split(string(content), " ")
		m[cs[0]] = len(cs[1:]) - 1
	}
	sb := strings.Builder{}
	ks := []string{}
	for k, _ := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		sb.WriteString(k)
		sb.WriteString(" ")
		sb.WriteString(fmt.Sprintf("%d", m[k]))
		sb.WriteString("\n")
	}
	os.WriteFile("mr-out-reduce.txt", []byte(sb.String()), 0666)
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
