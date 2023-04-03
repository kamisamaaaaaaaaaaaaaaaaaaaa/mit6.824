package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

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
	Client_id int64
	Op_id     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	Data                 map[string]string
	appliedindex         int
	indexfinished        map[int]chan Op
	ClientId_to_LastOpId map[int64]int
	persister            *raft.Persister
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

	op := Op{
		Operation: "Get",
		Key:       args.Key,
		Value:     "",
		Client_id: args.Client_id,
		Op_id:     args.Op_id,
	}

	index, _, isleader := kv.rf.Start(op)

	if !isleader {
		reply.Err = "Not Leader"
		reply.LeaderId = kv.rf.GetLeaderId()
		kv.mu.Unlock()
		return
	}

	kv.indexfinished[index] = make(chan Op, 1)
	ch := kv.indexfinished[index]

	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.indexfinished, index)
		kv.mu.Unlock()
	}()

	select {
	case command := <-ch:
		if command.Client_id == op.Client_id && command.Op_id == op.Op_id {
			////fmt.Printf("Get成功，Key:%v，Value:%v\n", op.Key, kv.Data[op.Key])
			kv.mu.Lock()
			reply.Value = kv.Data[op.Key]
			kv.mu.Unlock()
		} else {
			reply.Err = "Get Failed"
		}
	case <-time.After(1 * time.Second):
		reply.Err = "Get Timeout"
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
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
		reply.LeaderId = kv.rf.GetLeaderId()
		kv.mu.Unlock()
		return
	}

	kv.indexfinished[index] = make(chan Op, 1)
	ch := kv.indexfinished[index]

	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.indexfinished, index)
		kv.mu.Unlock()
	}()

	select {
	case command := <-ch:
		if command.Client_id == op.Client_id && command.Op_id == op.Op_id {
			return
		} else {
			reply.Err = "Put/Append Failed"
		}
	case <-time.After(1 * time.Second):
		reply.Err = "Put/Append Timeout"
	}
}

func (kv *KVServer) applier() {
	for {
		select {
		case command := <-kv.applyCh:
			//fmt.Printf("server :%v commandindex:%v command:%v\n", kv.me, command.CommandIndex, command)
			if command.SnapshotValid {
				kv.ReadSnapshot()
			} else {

				if command.Command != nil {

					op := command.Command.(Op)
					kv.mu.Lock()

					last_op_id, ok := kv.ClientId_to_LastOpId[op.Client_id]
					if !ok {
						last_op_id = -1
					}

					if last_op_id < op.Op_id {
						//fmt.Printf("server :%v commandindex:%v last op id : %v , op_id : %v ,oper : %v, Key: %v, Value : %v\n", kv.me, command.CommandIndex, last_op_id, op.Op_id, op.Operation, op.Key, op.Value)
						if op.Operation == "Put" {
							kv.Data[op.Key] = op.Value
						} else if op.Operation == "Append" {
							kv.Data[op.Key] += op.Value
						}
						kv.ClientId_to_LastOpId[op.Client_id] = op.Op_id
					}

					if ch, ok := kv.indexfinished[command.CommandIndex]; ok {
						_, isleader := kv.rf.GetState()
						if isleader {
							ch <- op
						} else {
							ch <- Op{
								Operation: "",
								Key:       "",
								Value:     "",
								Client_id: -1,
								Op_id:     -1,
							}
						}
						delete(kv.indexfinished, command.CommandIndex)
					}

					kv.mu.Unlock()
				}

				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					kv.Snapshot(command.CommandIndex)
				}
			}
		}
	}
}

func (kv *KVServer) Snapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	////fmt.Printf("server %v data before snapshot: %v \n", kv.me, kv.Data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.Data)
	e.Encode(kv.ClientId_to_LastOpId)

	sm_state := w.Bytes()

	kv.rf.Snapshot(index, sm_state)
}

func (kv *KVServer) ReadSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data := kv.persister.ReadSnapshot()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var Data map[string]string
	var ClientId_to_LastOpId map[int64]int

	if d.Decode(&Data) != nil || d.Decode(&ClientId_to_LastOpId) != nil {
		// //fmt.Printf("--------------------------decode failed------------------------\n")
	} else {
		kv.Data = Data
		kv.ClientId_to_LastOpId = ClientId_to_LastOpId

		//fmt.Printf("server %v data after Readsnapshot: %v \n", kv.me, kv.Data)
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
	kv.Data = make(map[string]string)

	kv.ClientId_to_LastOpId = make(map[int64]int)

	kv.persister = persister

	kv.indexfinished = make(map[int]chan Op)

	kv.appliedindex = 0

	kv.ReadSnapshot()

	go kv.applier()

	return kv
}
