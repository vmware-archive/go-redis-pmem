package transaction

import (
	"log"
	"runtime"
	"sync"
	"unsafe"
)

const (
	LOGSIZE     int     = 4 * 1024 * 1024
	CACHELINE   uintptr = 64
	LBUFFERSIZE         = 512 * 1024
	BUFFERSIZE          = 4 * 1024
)

// transaction interface
type (
	TX interface {
		Begin() error
		Log(interface{}) error
		FakeLog(interface{})
		Commit() error
		Abort() error
		RLock(*sync.RWMutex)
		WLock(*sync.RWMutex)
		Lock(*sync.RWMutex)
	}
)

// Transaction initialization function.
func Init(gcPtr unsafe.Pointer) unsafe.Pointer {
	return InitUndo(gcPtr)
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

// Flush changes to persistent memory region at address p and size s
func Flush(p unsafe.Pointer, s int) {
	runtime.FlushRange(p, uintptr(s))
}

func sfence() {
	runtime.Fence()
}

// Persist calls into the runtime to flush changes to the persistent memory region
// and also calls a fence instruction as required
func Persist(p unsafe.Pointer, s int) {
	runtime.PersistRange(p, uintptr(s))
}
