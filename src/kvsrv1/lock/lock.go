package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:  ck,
		key: l,
	}
	// You may add code here
	lk.ck.Put(lk.key, " ", 0)
	// if err != rpc.OK && err != rpc.ErrNoKey {
	// 	log.Fatalf("MakeLock: initial Put failed: %v\n", err)
	// }
	return lk
}

// 要不要自旋
func (lk *Lock) Acquire() {
	// Your code here
	id := lk.ck.ID() //获得id
	for {
		log.Printf("[Client %v] Acquired lock", id)
		v, version, err := lk.ck.Get(lk.key)
		if err != rpc.OK && err != rpc.ErrNoKey {
			continue // 临时性错误，重试
		}

		if v == " " || v == id {
			// println("############")
			err := lk.ck.Put(lk.key, id, version)
			if err == rpc.OK || err == rpc.ErrMaybe {
				// OK 或者不确定（幂等重试时需要外部保证）
				return
			} else if err == rpc.ErrVersion {
				// 版本冲突，被别人抢先更新，重试
				continue
			}
		}

		time.Sleep(10 * time.Millisecond)
		log.Printf("[Client %v] Failed to acquire, retrying...", id)
	}

}

func (lk *Lock) Release() {
	id := lk.ck.ID() // 当前客户端 ID
	v, version, err := lk.ck.Get(lk.key)
	if err == rpc.ErrNoKey {
		log.Fatalf("Release: lock key %v does not exist", lk.key)
		return
	}
	if err != rpc.OK {
		log.Printf("Release: Get failed with err=%v, retrying...", err)
		return
	}

	if v == id {
		// 只有当前客户端持有锁时才能释放
		err := lk.ck.Put(lk.key, " ", version) // 清空锁状态
		if err != rpc.OK && err != rpc.ErrMaybe {
			log.Printf("Release: Put failed with err=%v", err)
			// 可根据需要进行 retry 或忽略
		} else {
			println(version)
			log.Printf("[Client %v] Release lock", id)
		}
	} else {
		// 非持有者尝试释放，一般可忽略或日志警告
		log.Printf("Release: client %v tried to release lock, but it's held by %v", id, v)
	}
}
