package shardctrler

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	// Your data here.
	indexfinished    map[int]chan Op
	ClientId_to_OpId map[int64]int
	maxraftstate     int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Operation string
	ClientId  int64
	Op_id     int
	JoinArgs  *JoinArgs
	LeaveArgs *LeaveArgs
	MoveArgs  *MoveArgs
	QueryArgs *QueryArgs
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()

	op := Op{
		Operation: "Join",
		ClientId:  args.Client_id,
		Op_id:     args.Op_id,
		JoinArgs:  args,
	}

	index, _, isleader := sc.rf.Start(op)

	if !isleader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		sc.mu.Unlock()
		return
	}

	sc.indexfinished[index] = make(chan Op, 1)
	ch := sc.indexfinished[index]
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.indexfinished, index)
		sc.mu.Unlock()
	}()

	select {
	case command := <-ch:
		if command.ClientId == op.ClientId && command.Op_id == op.Op_id {
			reply.WrongLeader = false
			reply.Err = ""
		}
	case <-time.After(1 * time.Second):
		reply.WrongLeader = true
		reply.Err = "Join Timeout"
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()

	op := Op{
		Operation: "Leave",
		ClientId:  args.Client_id,
		Op_id:     args.Op_id,
		LeaveArgs: args,
	}

	index, _, isleader := sc.rf.Start(op)

	if !isleader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		sc.mu.Unlock()
		return
	}

	sc.indexfinished[index] = make(chan Op, 1)
	ch := sc.indexfinished[index]
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.indexfinished, index)
		sc.mu.Unlock()
	}()

	select {
	case command := <-ch:
		if command.ClientId == op.ClientId && command.Op_id == op.Op_id {
			reply.WrongLeader = false
			reply.Err = ""
		}
	case <-time.After(1 * time.Second):
		reply.WrongLeader = true
		reply.Err = "Leave Timeout"
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()

	op := Op{
		Operation: "Move",
		ClientId:  args.Client_id,
		Op_id:     args.Op_id,
		MoveArgs:  args,
	}

	index, _, isleader := sc.rf.Start(op)

	if !isleader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		sc.mu.Unlock()
		return
	}

	sc.indexfinished[index] = make(chan Op, 1)
	ch := sc.indexfinished[index]
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.indexfinished, index)
		sc.mu.Unlock()
	}()

	select {
	case command := <-ch:
		if command.ClientId == op.ClientId && command.Op_id == op.Op_id {
			reply.WrongLeader = false
			reply.Err = ""
		}
	case <-time.After(1 * time.Second):
		reply.WrongLeader = true
		reply.Err = "Move Timeout"
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()

	op := Op{
		Operation: "Query",
		ClientId:  args.Client_id,
		Op_id:     args.Op_id,
		QueryArgs: args,
	}

	index, _, isleader := sc.rf.Start(op)

	if !isleader {
		reply.WrongLeader = true
		reply.Err = "Not Leader"
		sc.mu.Unlock()
		return
	}

	sc.indexfinished[index] = make(chan Op, 1)
	ch := sc.indexfinished[index]
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.indexfinished, index)
		sc.mu.Unlock()
	}()

	select {
	case command := <-ch:
		if command.ClientId == op.ClientId && command.Op_id == op.Op_id {
			reply.WrongLeader = false
			reply.Err = ""

			sc.mu.Lock()
			if args.Num == -1 || args.Num >= len(sc.configs) {
				args.Num = len(sc.configs) - 1
			}
			reply.Config = sc.configs[args.Num]
			sc.mu.Unlock()
		}
	case <-time.After(1 * time.Second):
		reply.WrongLeader = true
		reply.Err = "Query Timeout"
	}

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) JoinOpertion(args *JoinArgs) {
	new_config := Config{}
	new_config.Groups = make(map[int][]string)

	cur_config := sc.configs[len(sc.configs)-1]

	for k, v := range cur_config.Groups {
		new_config.Groups[k] = v
	}
	for k, v := range args.Servers {
		new_config.Groups[k] = v
	}

	shards_num := len(cur_config.Shards)
	groups_num := len(new_config.Groups)

	avg := shards_num / groups_num

	group := 0
	shards_id := 0

	var gids []int
	for k := range new_config.Groups {
		gids = append(gids, k)
	}
	sort.Ints(gids)

	for _, k := range gids {
		tot := avg
		if group < shards_num%groups_num {
			tot += 1
		}

		cnt := 0
		for cnt < tot && shards_id < shards_num {
			new_config.Shards[shards_id] = k
			cnt += 1
			shards_id += 1
		}
		group += 1
	}

	new_config.Num = cur_config.Num + 1

	sc.configs = append(sc.configs, new_config)

	//fmt.Printf("Join New Config:%v\n", new_config)
}

func (sc *ShardCtrler) MoveOpertion(args *MoveArgs) {
	new_config := Config{}
	new_config.Groups = make(map[int][]string)

	cur_config := sc.configs[len(sc.configs)-1]

	for k, v := range cur_config.Groups {
		new_config.Groups[k] = v
	}

	new_config.Shards = cur_config.Shards

	new_config.Shards[args.Shard] = args.GID

	new_config.Num = cur_config.Num + 1

	sc.configs = append(sc.configs, new_config)
}

func (sc *ShardCtrler) LeaveOpertion(args *LeaveArgs) {
	new_config := Config{}
	new_config.Groups = make(map[int][]string)

	cur_config := sc.configs[len(sc.configs)-1]

	for k, v := range cur_config.Groups {
		new_config.Groups[k] = v
	}

	for i := 0; i < len(args.GIDs); i++ {
		delete(new_config.Groups, args.GIDs[i])
	}

	shards_num := len(cur_config.Shards)
	groups_num := len(new_config.Groups)

	if groups_num != 0 {

		avg := shards_num / groups_num

		group := 0
		shards_id := 0

		var gids []int
		for k := range new_config.Groups {
			gids = append(gids, k)
		}
		sort.Ints(gids)

		for _, k := range gids {
			tot := avg
			if group < shards_num%groups_num {
				tot += 1
			}

			cnt := 0
			for cnt < tot && shards_id < shards_num {
				new_config.Shards[shards_id] = k
				cnt += 1
				shards_id += 1
			}
			group += 1
		}

		new_config.Num = cur_config.Num + 1
	}

	sc.configs = append(sc.configs, new_config)

}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case command := <-sc.applyCh:
			if command.SnapshotValid {
				sc.ReadSnapshot(command.Snapshot)
			} else {
				if command.Command != nil {
					op := command.Command.(Op)

					//fmt.Printf("op:%v\n", op)

					sc.mu.Lock()

					last_id, ok := sc.ClientId_to_OpId[op.ClientId]
					if !ok {
						sc.ClientId_to_OpId[op.ClientId] = -1
						last_id = -1
					}

					if op.Op_id > last_id {
						if op.Operation == "Join" {
							sc.JoinOpertion(op.JoinArgs)
						} else if op.Operation == "Leave" {
							sc.LeaveOpertion(op.LeaveArgs)
						} else if op.Operation == "Move" {
							sc.MoveOpertion(op.MoveArgs)
						}

						sc.ClientId_to_OpId[op.ClientId] = op.Op_id
					}

					if ch, ok := sc.indexfinished[command.CommandIndex]; ok {
						_, isleader := sc.rf.GetState()
						if isleader {
							ch <- op
						} else {
							ch <- Op{
								ClientId: -1,
								Op_id:    -1,
							}
						}

						delete(sc.indexfinished, command.CommandIndex)
					}
					sc.mu.Unlock()

				}

				if sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate {
					sc.Snapshot(command.CommandIndex)
				}
			}

		}
	}
}

func (sc *ShardCtrler) Snapshot(index int) {

	sc.mu.Lock()
	defer sc.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(sc.ClientId_to_OpId)
	e.Encode(sc.configs)
	e.Encode(sc.maxraftstate)

	sm_state := w.Bytes()
	sc.rf.Snapshot(index, sm_state)
}

func (sc *ShardCtrler) ReadSnapshot(data []byte) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var ClientId_to_OpId map[int64]int
	var configs []Config
	var maxraftstate int

	if d.Decode(&ClientId_to_OpId) != nil || d.Decode(&configs) != nil || d.Decode(&maxraftstate) != nil {
		fmt.Printf("----------------------------decode fail-------------------------\n")
	} else {
		sc.ClientId_to_OpId = ClientId_to_OpId
		sc.configs = configs
		sc.maxraftstate = maxraftstate
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.indexfinished = make(map[int]chan Op)
	sc.ClientId_to_OpId = make(map[int64]int)
	sc.maxraftstate = 1
	sc.persister = persister

	sc.ReadSnapshot(sc.persister.ReadSnapshot())

	go sc.applier()

	return sc
}
