package raftkv

import (
	"labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// You will probably have to modify your Clerk to remember
	// which server turned out to be the leader for the last RPC,
	// and send the next RPC to that server first.
	leader int
	id     int64
	seq    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("[Clerk.Get] clerk %d start %s with key: %s", ck.id, KVGet, key)
	defer DPrintf("[Clerk.Get] clerk %d end", ck.id)
	args := GetArgs{key, ck.id, ck.seq}
	var leaderIndex int

	for {
		leaderIndex = ck.GetLeader()
		// todo: 笔记: reply 每次都是最新的
		reply := GetReply{}
		if ok := ck.servers[leaderIndex].Call("KVServer.Get", &args, &reply); !ok || reply.WrongLeader {
			if reply.WrongLeader {
				DPrintf("[Clerk.Get] kvserver[%d] is not leader!", leaderIndex)

			}
			leaderIndex = (leaderIndex + 1) % len(ck.servers)
			ck.UpdateLeader(leaderIndex)
			continue
		}

		if reply.Err != OK {
			continue
		}
		ck.GetAndIncSeq(1)
		DPrintf("[Clerk.Get] success, clerk %d %s from kvserver %d {\"%s\": \"%s\"}", ck.id, KVGet, leaderIndex, key, reply.Value)
		return reply.Value
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("[Clerk.PutAppend] clerk %d start %s {\"%s\": \"%s\"}", ck.id, op, key, value)
	defer DPrintf("[Clerk.PutAppend] clerk %d end", ck.id)

	args := PutAppendArgs{key, value, op, ck.id, ck.GetAndIncSeq(1)}
	var leaderIndex int

	for {
		leaderIndex = ck.GetLeader()
		reply := PutAppendReply{}
		if ok := ck.servers[leaderIndex].Call("KVServer.PutAppend", &args, &reply); !ok || reply.WrongLeader {
			if !ok {
				DPrintf("[Clerk.PutAppend] call kvserver[%d] failed", leaderIndex)
			}
			if reply.WrongLeader {
				DPrintf("[Clerk.PutAppend] kvserver[%d] is not leader!", leaderIndex)
			}
			leaderIndex = (leaderIndex + 1) % len(ck.servers)
			ck.UpdateLeader(leaderIndex)
			continue
		}
		if reply.Err != OK {
			continue
		}
		DPrintf("[Clerk.PutAppend] success, client %d %s to kvserver %d {\"%s\": \"%s\"}", ck.id, op, leaderIndex, key, value)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) GetLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) UpdateLeader(newLeader int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leader = newLeader
}

func (ck *Clerk) GetAndIncSeq(diff int) int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	seq := ck.seq
	ck.seq += diff

	return seq
}
