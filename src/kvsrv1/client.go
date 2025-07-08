package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
	//add code
	clerkId string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	ck.clerkId = kvtest.RandValue(8)
	return ck
}

func (ck *Clerk) ID() string {
	return ck.clerkId
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	Args := &rpc.GetArgs{
		Key: key,
	}
	Reply := &rpc.GetReply{}
	//如何得到服务名字
	ok := ck.clnt.Call(ck.server, "KVServer.Get", Args, Reply)
	for !ok {
		time.Sleep(100 * time.Millisecond)
		ok = ck.clnt.Call(ck.server, "KVServer.Get", Args, Reply)
	}
	return Reply.Value, Reply.Version, Reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	Args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	Reply := &rpc.PutReply{}

	ok := ck.clnt.Call(ck.server, "KVServer.Put", Args, Reply)
	for !ok {
		time.Sleep(100 * time.Millisecond)
		ok = ck.clnt.Call(ck.server, "KVServer.Put", Args, Reply)
		if Reply.Err == rpc.ErrVersion {
			Reply.Err = rpc.ErrMaybe
		}

	}

	return Reply.Err
}
