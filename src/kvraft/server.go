package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	Client_id int
	Op_id     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	hash                      map[string]string
	lastappliedindex          int
	lastindex                 int
	up_to_date                *sync.Cond
	has_put                   *sync.Cond
	client_id_to_last_op_id   map[int]int
	client_id_to_recent_op_id map[int]int
	// Your definitions here.
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = "Not Leader"
		// fmt.Printf("server %v is not leader\n", kv.me)
		kv.mu.Unlock()
		return
	}

	// fmt.Printf("server %v 收到Get请求，is leader\n", kv.me)

	for kv.lastappliedindex < kv.lastindex {
		kv.up_to_date.Wait()
	}

	value, ok := kv.hash[args.Key]
	if !ok {
		value = ""
	}

	reply.Value = value

	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isleader := kv.rf.GetState(); !isleader {
		reply.Err = "Not Leader"
		return
	}

	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		Client_id: args.Client_id,
		Op_id:     args.Op_id,
	}

	index, _, isleader := kv.rf.Start(op)

	if !isleader {
		reply.Err = "Not Leader"
		return
	}

	_, ok := kv.client_id_to_last_op_id[op.Client_id]
	if !ok {
		kv.client_id_to_last_op_id[op.Client_id] = -1
	}

	//fmt.Printf("server %v 收到 %v 命令,Key: %v,Value: %v\n", kv.me, op.Operation, op.Key, op.Value)
	kv.lastindex = index

	for kv.client_id_to_last_op_id[op.Client_id] < op.Op_id {
		kv.has_put.Wait()
	}

}

func (kv *KVServer) applier() {
	for {
		select {
		case op := <-kv.applyCh:
			kv.mu.Lock()

			command := op.Command.(Op)

			if command.Op_id > kv.client_id_to_last_op_id[command.Client_id] {
				if command.Operation == "Put" {
					//fmt.Printf("server:%v lastappliedindex:%v CommandIndex:%v Put Key:%v Value:%v\n", kv.me, kv.lastappliedindex, op.CommandIndex, command.Key, command.Value)
					kv.hash[command.Key] = command.Value
				} else {
					//fmt.Printf("server:%v lastappliedindex:%v CommandIndex:%v Append Key:%v Value:%v\n", kv.me, kv.lastappliedindex, op.CommandIndex, command.Key, command.Value)
					value, ok := kv.hash[command.Key]
					if !ok {
						kv.hash[command.Key] = command.Value
					} else {
						kv.hash[command.Key] = value + command.Value
					}
				}
			}

			kv.lastappliedindex = op.CommandIndex
			kv.lastindex = max(kv.lastindex, op.CommandIndex)
			kv.client_id_to_last_op_id[command.Client_id] = max(kv.client_id_to_last_op_id[command.Client_id], command.Op_id)

			kv.up_to_date.Broadcast()
			kv.has_put.Broadcast()

			kv.mu.Unlock()
		}

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.hash = make(map[string]string)
	kv.lastappliedindex = 0
	kv.lastindex = 0
	kv.up_to_date = sync.NewCond(&kv.mu)
	kv.has_put = sync.NewCond(&kv.mu)

	kv.client_id_to_last_op_id = make(map[int]int)
	kv.client_id_to_recent_op_id = make(map[int]int)

	go kv.applier()

	return kv
}
