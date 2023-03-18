package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Args struct {
	//当前任务是否完成
	Map_finished    bool
	Reduce_finished bool

	//任务类型：map或reduce
	Type string

	//任务编号
	Map_index    int
	Reduce_index int
}

type Reply struct {
	//map任务的输入文件
	File string
	//reduce任务对应的一系列中间文件
	Intermediatefiles []string
	NReduce           int
	//任务编号
	Index int
	Type  string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
