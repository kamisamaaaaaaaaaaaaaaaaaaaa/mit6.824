package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	client_id int64
	op_id     int
	leader_id int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// ////fmt.Printf("--------------------------------------------make_client-----------------------------------\n")
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.client_id = nrand()
	ck.op_id = 0
	ck.leader_id = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		Client_id: ck.client_id,
		Op_id:     ck.op_id,
	}

	ck.op_id += 1

	var value string
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leader_id].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err == "" {
			value = reply.Value
			////fmt.Printf("get success\n")
			break
			// }
		} else if reply.Err == "Not Leader" && reply.LeaderId != -1 && reply.LeaderId != ck.leader_id {
			// //fmt.Printf("leaderid原来为%v,变为%v\n", ck.leader_id, reply.LeaderId)
			ck.leader_id = reply.LeaderId
		} else {
			ck.leader_id = int(nrand()) % len(ck.servers)
		}
		// ck.leader_id = (ck.leader_id + 1) % len(ck.servers)
	}

	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		Client_id: ck.client_id,
		Op_id:     ck.op_id,
	}

	ck.op_id += 1

	for {
		reply := PutAppendReply{}

		ok := ck.servers[ck.leader_id].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == "" {
			////fmt.Printf("put/append success\n")
			break
			// }
		} else if reply.Err == "Not Leader" && reply.LeaderId != -1 && reply.LeaderId != ck.leader_id {
			// //fmt.Printf("leaderid原来为:%v 变为%v\n", ck.leader_id, reply.LeaderId)
			ck.leader_id = reply.LeaderId
		} else {
			ck.leader_id = int(nrand()) % len(ck.servers)
		}
		// ck.leader_id = (ck.leader_id + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
