package raftkv

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	CommandTimeout = 1500
	KVGet = "Get"
	KVPut = "Put"
	KVAppend = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// the client will retry the command with a new leader,
	// causing it to be executed a second time.
	// The solution is for clients to assign unique serial numbers to every command
	ClientId int64
	Seq      int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	Seq      int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
