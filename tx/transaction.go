package tx

import (
	"unsafe"
	"log"
)

const (
	LOGSIZE   int     = 4 * 1024
	CACHELINE uintptr = 64
)

// transaction interface
type Transaction interface {
	Begin() error
	Log(interface{}) error
	Commit() error
	Abort() error
}

func Init(logArea []byte) {
	// currently only support simple undo logging transaction.
	InitUndo(logArea)
}

func NewUndo() Transaction {
	return newUndo()
}

func Release(t Transaction) {
	// currently only support simple undo logging transaction.
	tt, ok := t.(*undoTx)
	if ok {
		releaseUndo(tt)
	} else {
		log.Panic("Releasing unsupported transaction!")
	}
} 

// directly persist pmem range
func Persist(p unsafe.Pointer, s int) {
	f := uintptr(p) &^ (CACHELINE - 1)
	l := (uintptr(p) + uintptr(s) - 1) &^ (CACHELINE - 1)
	for f <= l {
		//clflush(f)
		f += CACHELINE
	}
}

func sfence()

func clflush(uintptr)
