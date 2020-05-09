package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string
	ClientId int64
	Seq      int
	Term     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database  map[string]string
	clientSeq map[int64]int
	opChDone  map[int]chan Op

	done bool
}

//	customer methods
func (kv *KVServer) getCommand(args *GetArgs) Op {

	return Op{
		Key:      args.Key,
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Op:       KVGet,
	}

}

func (kv *KVServer) putAppendCommand(args *PutAppendArgs) Op {
	return Op{
		Key:      args.Key,
		Value:    args.Value,
		Op:       args.Op,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
}

// no threadsafe
func (kv *KVServer) getOpChFromMap(index int) chan Op {
	if op, ok := kv.opChDone[index]; !ok {
		DPrintf("[getOpChFromMap] kvserver[%d] add new command at index %d", kv.me, index)
		op = make(chan Op, 1)
		kv.opChDone[index] = op
		return op
	} else {
		return op
	}

}

// original methods

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, term, isLeader := kv.rf.Start(kv.getCommand(args))
	if isLeader {
		DPrintf("[KVServer.Get] race check: try lock")
		kv.mu.Lock()
		DPrintf("[KVServer.Get] race check: lock success")
		doneCh := kv.getOpChFromMap(index)
		kv.mu.Unlock()
		select {
		case op := <-doneCh:
			reply.WrongLeader = term != op.Term
			reply.Value = op.Value
			reply.Err = OK
		case <-time.After(CommandTimeout * time.Millisecond):
		}

	} else {
		reply.WrongLeader = true
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[KVServer.PutAppend] kvserver[%d] begin", kv.me)

	DPrintf("[KVServer.PutAppend] race check: try lock")
	kv.mu.Lock()
	DPrintf("[KVServer.PutAppend] race check: lock success")
	seq, ok := kv.clientSeq[args.ClientId]
	kv.mu.Unlock()

	if ok && args.Seq < seq {
		DPrintf("[KVServer.PutAppend] kvserver[%d] has already execute the command %v", kv.me, args)
		_, isLeader := kv.rf.GetState()
		reply.WrongLeader = !isLeader
		reply.Err = OK
		return
	}

	index, term, isLeader := kv.rf.Start(kv.putAppendCommand(args))
	if !isLeader {
		DPrintf("[KVServer.PutAppend] kvserver[%d] is not leader", kv.me)
		reply.WrongLeader = true
	} else {
		DPrintf("[KVServer.PutAppend] race check: try lock")
		kv.mu.Lock()
		DPrintf("[KVServer.PutAppend] race check: lock success")
		doneCh := kv.getOpChFromMap(index)
		kv.mu.Unlock()

		select {
		case op := <-doneCh:
			DPrintf("[KVServer.PutAppend] kvserver[%d] apply command %v", kv.me, op)
			reply.WrongLeader = term != op.Term
			if reply.WrongLeader {
				DPrintf("[KVServer.PutAppend] kvserver leader has change!")
			}
			reply.Err = OK
			return
		case <-time.After(CommandTimeout * time.Millisecond):
			return
		}
	}
	DPrintf("[KVServer.PutAppend] kvserver[%d] end", kv.me)

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.clientSeq = make(map[int64]int)
	kv.opChDone = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go func() {
		for msg := range kv.applyCh {
			index := msg.CommandIndex
			op := msg.Command.(Op)
			term, _ := kv.rf.GetState()
			op.Term = term
			DPrintf("[StartKVServer] race check: try lock")
			kv.mu.Lock()
			DPrintf("[StartKVServer] race check: lock success")
			doneCh := kv.getOpChFromMap(index)
			if op.Op == KVGet {
				DPrintf("[StartKVServer] kvserver[%d] Get", kv.me)
				op.Value = kv.database[op.Key]
			} else if seq, ok := kv.clientSeq[op.ClientId]; !ok || seq < op.Seq {
				kv.clientSeq[op.ClientId] = op.Seq
				if op.Op == KVPut {
					DPrintf("[StartKVServer] kvserver[%d] Put", kv.me)
					kv.database[op.Key] = op.Value
				} else if op.Op == KVAppend {
					DPrintf("[StartKVServer] kvserver[%d] Append", kv.me)
					kv.database[op.Key] += op.Value
				}
			}
			kv.mu.Unlock()

			go func() {
				doneCh <- op
			}()
		}
	}()

	return kv
}
