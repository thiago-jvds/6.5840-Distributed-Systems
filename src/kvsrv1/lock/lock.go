package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// Special key in the server for the lock
	lkey string
	// Lock Client Unique identifier
	uId string
}

const DefaultLockOwner string = ""

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lkey: l, uId: kvtest.RandValue(8)}

	// If key doesn't exist, try until it exists
	_, _, err := lk.ck.Get(lk.lkey)
	if err == rpc.ErrNoKey {
		for {
			err = lk.ck.Put(lk.lkey, DefaultLockOwner, 0)
			if err != rpc.ErrNoKey {
				break
			}
		}
	}

	return lk
}

func (lk *Lock) Acquire() {
	for {

		currentHolder, version, err := lk.ck.Get(lk.lkey)

		// fmt.Printf("Acquire, current %v, holder %v, version %v, err %v\n", lk.uId, currentHolder, version, err)

		// If currentHolder is Requester
		if currentHolder == lk.uId {
			break
		}

		// Err in getting info about lock owner, or ownwership is taken
		if err != rpc.OK || currentHolder != DefaultLockOwner {
			continue
		}

		// If need to acquire the lock
		err = lk.ck.Put(lk.lkey, lk.uId, version)
		// fmt.Printf("Acquire inside, current %v, holder %v, version %v, err %v\n", lk.uId, currentHolder, version, err)

		// If able to acquire the lock, break it off the loop
		if err == rpc.OK {
			break

			// If any error other than Maybe, retry
		} else if err != rpc.ErrMaybe {
			continue

			// If Maybe, check if any errors
		} else if err == rpc.ErrMaybe {

			for {
				holder, _, err := lk.ck.Get(lk.lkey)

				if err == rpc.OK {

					// Put was succesful
					if holder == lk.uId {
						return
					} else {
						break
					}

				}
			}

		}

	}
}

func (lk *Lock) Release() {
	for {

		_, version, err := lk.ck.Get(lk.lkey)

		// fmt.Printf("Release, current %v, holder %v, version %v, err %v\n", lk.uId, currentHolder, version, err)

		if err != rpc.OK {
			continue
		}

		err = lk.ck.Put(lk.lkey, DefaultLockOwner, version)

		// fmt.Printf("Release after, current %v, holder %v, version %v, err %v\n", lk.uId, currentHolder, version, err)

		if err == rpc.OK {
			break

			// If any error other than Maybe, retry
		} else if err != rpc.ErrMaybe {
			continue

			// If Maybe, check if any errors
		} else if err == rpc.ErrMaybe {

			for {
				holder, _, err := lk.ck.Get(lk.lkey)

				if err == rpc.OK {

					// Put was succesful
					if holder != lk.uId {
						return
					} else {
						break
					}

				}
			}

		}

	}
}
