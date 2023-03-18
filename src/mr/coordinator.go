package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// map_index map[string]int
	// files     []string

	//input_file的序号，及其到name的映射（一个mapper处理一个input_file）
	input_index_to_files map[int]string
	input_index          []int

	//中间文件对应的reduce序号，及其对应的一系列文件，假设序号为i，则对应mr-..-i，每个i对应len(input_files)个文件
	reduce_index_to_files map[int][]string
	reduce_index          []int

	//mapper和reducer对应的计时器
	map_timers    map[int]*time.Timer
	reduce_timers map[int]*time.Timer

	//记录对应序号的map或task任务是否完成
	map_finished_index    []bool
	reduce_finished_index []bool

	//map任务或reduce任务是否全部完成
	map_finished    bool
	reduce_finished bool

	//map和reduce任务完成的个数
	has_mapped_cnt  int
	has_reduced_cnt int

	//input_file的数量
	tot_input int
	nReduce   int

	//加锁保护input_index数组和reduce_index数组的操作
	m sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//用于worker调用这个函数，用来获取对应的处理文件
func (c *Coordinator) Gettask(args *Args, reply *Reply) error {
	// fmt.Println(c.files[0])
	c.m.Lock()
	if !c.map_finished && len(c.input_index) > 0 {
		reply.Type = "map"
		reply.Index = c.input_index[0]
		reply.File = c.input_index_to_files[c.input_index[0]]

		//每次获取文件都要重设对应的编号的计时器
		if !c.map_timers[c.input_index[0]].Stop() {
			select {
			case <-c.map_timers[c.input_index[0]].C:
			default:
			}
		}
		c.map_timers[c.input_index[0]].Reset(10 * time.Second)

		reply.NReduce = c.nReduce
		c.input_index = c.input_index[1:]

	} else if c.map_finished && !c.reduce_finished && len(c.reduce_index) > 0 {
		reply.Type = "reduce"
		reply.Index = c.reduce_index[0]
		reply.Intermediatefiles = c.reduce_index_to_files[c.reduce_index[0]]

		if !c.reduce_timers[c.reduce_index[0]].Stop() {
			select {
			case <-c.reduce_timers[c.reduce_index[0]].C:
			default:
			}
		}
		c.reduce_timers[c.reduce_index[0]].Reset(10 * time.Second)

		c.reduce_index = c.reduce_index[1:]

		//由于是多线程，可能会出现map对应的任务全部分配完，但还没有得到处理完，此时再有进程过来访问的情况
		//此时属于过渡阶段
	} else if !c.map_finished {
		reply.Type = "map_dealing"
	} else if c.map_finished && !c.reduce_finished {
		reply.Type = "reduce_dealing"
	} else {
		reply.Type = "job_finished"
	}
	c.m.Unlock()

	// fmt.Println(reply.File, reply.NReduce)
	return nil
}

//worker调用这个函数用来告诉master此时已完成任务
func (c *Coordinator) Response(args *Args, reply *Reply) error {
	c.m.Lock()
	if args.Type == "map" {
		//由于同一个任务可能会分配给不同worker（某个worker处理太慢，master以为宕机了，又把任务分配给其它worker）
		//此时可能会接受同一个任务的若干个处理结果，此时要第一个即可（c.map_finished_index[args.Map_index]=false）
		if !c.map_finished_index[args.Map_index] {
			if args.Map_finished {
				c.map_finished_index[args.Map_index] = true
				c.has_mapped_cnt = c.has_mapped_cnt + 1
				for i := 0; i < c.nReduce; i++ {
					mid_file_name := "mr-" + strconv.Itoa(args.Map_index) + "-" + strconv.Itoa(i)
					c.reduce_index_to_files[i] = append(c.reduce_index_to_files[i], mid_file_name)
				}
				if c.has_mapped_cnt == c.tot_input {
					//map任务完成后开启reduce计时器检查
					go c.Reduce_check_timer()
					c.map_finished = true
				}
			} else {
				//若返回来的响应是处理失败，则重新将任务加入队列，后面交给其它worker
				c.input_index = append(c.input_index, args.Map_index)
			}
		}
	} else {
		if !c.reduce_finished_index[args.Reduce_index] {
			if args.Reduce_finished {
				c.reduce_finished_index[args.Reduce_index] = true
				c.has_reduced_cnt = c.has_reduced_cnt + 1
				if c.has_reduced_cnt == c.nReduce {
					c.reduce_finished = true
				}
			} else {
				c.reduce_index = append(c.reduce_index, args.Reduce_index)
			}
		}
	}
	c.m.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	//注册c后，同一个socket的客户端就可以调用c的函数了
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

//检查timer有没有超时，超时的话会向timer.C管道中塞东西
func (c *Coordinator) check_timer(Type string, index int, timer *time.Timer) {
	select {
	//若timer.C里面有东西，就会执行case，把超时任务加回列表，待再分配
	case <-timer.C:
		c.m.Lock()
		if Type == "map" {
			c.input_index = append(c.input_index, index)
		} else {
			c.reduce_index = append(c.reduce_index, index)
		}
		c.m.Unlock()
	//若没东西，说明无超时，此时执行default，然后退出
	default:
	}
}

//检查每个file的计时器
func (c *Coordinator) Map_check_timer() {
	for {
		//如果map任务完成,break，线程退出
		if c.map_finished {
			break
		}

		//否则检查每个没有完成的file对应的计时器
		for k, v := range c.map_timers {
			if !c.map_finished_index[k] {
				go c.check_timer("map", k, v)
			}
		}
	}
}

//检查每个reduce_file的计时器
func (c *Coordinator) Reduce_check_timer() {
	for {
		if c.reduce_finished {
			break
		}

		for k, v := range c.reduce_timers {
			if !c.reduce_finished_index[k] {
				go c.check_timer("reduce", k, v)
			}
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.map_finished && c.reduce_finished

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	// c.files = files

	c.input_index_to_files = make(map[int]string)
	for i := 0; i < len(files); i++ {
		c.input_index = append(c.input_index, i)
		c.input_index_to_files[i] = files[i]
	}

	c.reduce_index_to_files = make(map[int][]string)

	c.map_finished = false
	c.reduce_finished = false

	c.has_mapped_cnt = 0
	c.has_reduced_cnt = 0

	c.tot_input = len(files)

	for i := 0; i < nReduce; i++ {
		c.reduce_index = append(c.reduce_index, i)
	}

	c.map_timers = make(map[int]*time.Timer)
	for i := 0; i < len(files); i++ {
		c.map_timers[i] = time.NewTimer(10 * time.Second)
		//计时器初始化完要先暂停，否则会因为一开始非法超时而错误将任务重新添加到队列
		c.map_timers[i].Stop()
	}

	c.reduce_timers = make(map[int]*time.Timer)
	for i := 0; i < nReduce; i++ {
		c.reduce_timers[i] = time.NewTimer(10 * time.Second)
		c.reduce_timers[i].Stop()
	}

	for i := 0; i < len(files); i++ {
		c.map_finished_index = append(c.map_finished_index, false)
	}

	for i := 0; i < nReduce; i++ {
		c.reduce_finished_index = append(c.reduce_finished_index, false)
	}

	c.server()

	//启动map计时检查器
	go c.Map_check_timer()

	return &c
}
