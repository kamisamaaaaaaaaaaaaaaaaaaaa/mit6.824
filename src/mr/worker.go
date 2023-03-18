package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the coordinator.
	for {
		Call(mapf, reducef)
	}

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

//worker调用该函数向master获取任务
func Call(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := Args{}

	reply := Reply{}

	//调用coordinator的gettask获取任务
	ok := call("Coordinator.Gettask", &args, &reply)

	if ok {

		task_type := reply.Type

		if task_type == "map_dealing" {
			// fmt.Printf("map task is dealing\n")
		} else if task_type == "reduce_dealing" {
			// fmt.Printf("reduce task is dealing\n")
		} else if task_type == "job_finished" {
			// fmt.Printf("job has finished\n")
		} else if task_type == "map" {
			//map任务
			n_reduce := reply.NReduce
			index := reply.Index
			input_file_name := reply.File

			input_file, err := os.Open(input_file_name)

			if err != nil {
				// log.Fatalf("cannot open %v", input_file_name)
			}

			content, err := ioutil.ReadAll(input_file)

			if err != nil {
				// log.Fatalf("cannot read %v", input_file_name)
			}

			input_file.Close()

			//将input_file通过map函数转成kv对
			kva := mapf(input_file_name, string(content))

			//创建n_reduce个中间文件
			intermidiate_files := make([]*json.Encoder, n_reduce)

			for i := 0; i < n_reduce; i++ {
				intermidiate_file_name := "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(i)
				intermidiate_file, err := os.Create(intermidiate_file_name)
				if err != nil {
					// log.Fatalf("Create %v Fail!\n", intermidiate_file_name)
				}
				defer intermidiate_file.Close()
				intermidiate_files[i] = json.NewEncoder(intermidiate_file)
			}

			//根据key将key_value对分配到不同文件
			for i := 0; i < len(kva); i++ {
				intermidiate_index := ihash(kva[i].Key) % n_reduce
				err := intermidiate_files[intermidiate_index].Encode(&kva[i])
				if err != nil {
					// log.Fatalf("mr-%d-%d写入失败\n", index, intermidiate_index)
				}
			}

			//成功后生成一个回应，通过调用Coordinator的Response函数告诉coordinator
			args := Args{}
			args.Type = "map"
			args.Map_finished = true
			args.Map_index = index

			reply = Reply{}

			ok := call("Coordinator.Response", &args, &reply)
			if ok {
				// fmt.Printf("response successful for map index %d\n", index)
			} else {
				// fmt.Printf("response failed for map index %d\n", index)
			}
		} else if task_type == "reduce" {
			//reduce任务
			index := reply.Index
			intermidiate_files := reply.Intermediatefiles
			//先把对应的一系列文件转化成kv对
			kva := []KeyValue{}
			for _, filename := range intermidiate_files {
				file, err := os.Open(filename)
				if err != nil {
					// log.Fatalf("cannot open %v", filename)
				}
				defer file.Close()

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			//按key排序
			sort.Sort(ByKey(kva))

			oname := "mr-out-" + strconv.Itoa(index)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				//得到(key,[val1,val2,val3,...])，输入reduce得到最终结果，然后输入ofile
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()

			//调用Coordinator的Response告诉master已经完成reduce任务
			args := Args{}
			args.Reduce_finished = true
			args.Type = "reduce"
			args.Reduce_index = index

			reply := Reply{}

			ok := call("Coordinator.Response", &args, &reply)
			if ok {
				// fmt.Printf("response successful for reduce index %d\n", index)
			} else {
				// fmt.Printf("response failed for reduce index %d\n", index)
			}
		}
	} else {

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
		// log.Fatal("dialing:", err)
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
