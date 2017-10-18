package transaction

import (
	"unsafe"
	"log"
	"sync"
)

const (
	LOGSIZE   int     = 128 * 1024
	CACHELINE uintptr = 64
)

// transaction interface
type (
	TX interface {
		Begin() error
		Log(interface{}) error
		Commit() error
		Abort() error
		RLock(*sync.RWMutex)
		WLock(*sync.RWMutex)
	}
)

func Init(logArea []byte) {
	// currently only support simple undo logging transaction.
	InitUndo(logArea)
}

func Release(t TX) {
	// currently only support simple undo logging transaction.
	switch v := t.(type) {
	case *undoTx:
		releaseUndo(v)
	case *readonlyTx:
		releaseReadonly(v)
	default:
		log.Panic("Releasing unsupported transaction!")
	}
} 

// directly persist pmem range
func Persist(p unsafe.Pointer, s int)

func sfence()

func clflush(unsafe.Pointer)
