package transaction

/*
 * Simple undo implementation:
 * (1) single threaded
 * (2) linear log buffer
 * (3) no nested transaction
 * (4) layout:
 *  ------------------------------------------------------------------
 * | undoHeader | log data | entryHeader | log data | entryHeader |...|
 *  ------------------------------------------------------------------
 */

import (
	"errors"
	"log"
	"reflect"
	"runtime/debug"
	"sync"
	"unsafe"
)

type (
	undoHeader struct {
		tail int // current offset of log buffer
	}

	entryHeader struct {
		offset uintptr
		size   int
	}

	undoTx struct {
		id         int         // transaction id
		undoHdr    *undoHeader // transaction header
		undoBuf    logBuffer   // volatile wrapper for log buffer
		largeBuf   bool        // using large buffer or not
		level      int         // tx level
		undoEntry  entryHeader // volatile entry header
		entrySlice []byte      // underlying raw byte slice of undoEntry
		rlocks     []*sync.RWMutex
		wlocks     []*sync.RWMutex
	}
)

const (
	LBUFFERSIZE = 512 * 1024
	BUFFERSIZE  = 4 * 1024
)

var (
	pool    [2]chan *undoTx // pool[0] for small log buffers, pool[1] for large log buffers
	undoOff uintptr         // offset of undo area
)

func initUndo(id int, largeBuf bool, logArea []byte) *undoTx {
	t := new(undoTx)
	t.id = id
	t.undoHdr = (*undoHeader)(unsafe.Pointer(&logArea[0]))
	t.largeBuf = largeBuf

	var err error
	t.undoBuf, err = initLinearUndoBuffer(logArea[unsafe.Sizeof(*t.undoHdr):], t.undoHdr.tail)
	if err != nil {
		log.Fatal(err)
	}

	err = t.Abort()
	if err != nil {
		log.Fatal(err)
	}

	ptr := unsafe.Pointer(&t.undoEntry)
	size := unsafe.Sizeof(t.undoEntry)
	t.entrySlice = (*[BUFFERSIZE]byte)(ptr)[:size:size]
	// pre allocate some space for holding locks
	t.wlocks = make([]*sync.RWMutex, 0, 3)
	t.rlocks = make([]*sync.RWMutex, 0, 3)

	return t
}

func InitUndo(logArea []byte) {
	// init global variables
	undoOff = uintptr(unsafe.Pointer(&logArea[0]))

	initPool(false, logArea[:len(logArea)/2])
	initPool(true, logArea[len(logArea)/2:])
}

func initPool(largeBuf bool, logArea []byte) {
	bufSize := 0
	idx := 0
	if largeBuf {
		bufSize = LBUFFERSIZE
		idx = 1
	} else {
		bufSize = BUFFERSIZE
	}
	max := len(logArea) / bufSize
	if max == 0 {
		log.Fatal("Not enough log area for initializing undo log! ", len(logArea))
	}
	// init transaction pool
	pool[idx] = make(chan *undoTx, max)
	for i := 0; i < max; i++ {
		begin := len(logArea) / max * i
		end := len(logArea) / max * (i + 1)
		pool[idx] <- initUndo(i, largeBuf, logArea[begin:end])
	}
}

func NewUndo() TX {
	if pool[0] == nil {
		log.Fatal("Undo log not correctly initialized!")
	}
	t := <-pool[0]
	// log.Println("Get log ", t.id)
	return t
}

func NewLargeUndo() TX {
	if pool[1] == nil {
		log.Fatal("Undo log not correctly initialized!")
	}
	t := <-pool[1]
	// log.Println("Get log ", t.id)
	return t
}

func releaseUndo(t *undoTx) {
	t.Abort()
	// log.Println("Release log ", t.id)
	if t.largeBuf {
		pool[1] <- t
	} else {
		pool[0] <- t
	}
}

func (t *undoTx) setUndoHdr(tail int) {
	sfence()
	t.undoHdr.tail = tail // atomic update
	//Persist(unsafe.Pointer(t.undoHdr), int(unsafe.Sizeof(*t.undoHdr)))
	clflush(unsafe.Pointer(t.undoHdr))
	sfence()
}

func (t *undoTx) Log(data interface{}) error {
	// Check data type, get pointer and size of data.
	v := reflect.ValueOf(data)
	bytes := 0
	switch kind := v.Kind(); kind {
	case reflect.Slice:
		bytes = v.Len() * int(v.Type().Elem().Size())
	case reflect.Ptr:
		bytes = int(v.Elem().Type().Size())
	default:
		debug.PrintStack()
		return errors.New("tx.undo: Log data must be pointer/slice!")
	}
	ptr := unsafe.Pointer(v.Pointer())

	// Append data to undo log buffer.
	_, err := t.undoBuf.Write((*[LBUFFERSIZE]byte)(ptr)[:bytes:bytes])
	if err != nil {
		return err
	}
	// Append log header.
	t.undoEntry.offset = v.Pointer() - undoOff
	t.undoEntry.size = bytes
	_, err = t.undoBuf.Write(t.entrySlice)
	if err != nil {
		return err
	}

	// Update log offset in header.
	t.setUndoHdr(t.undoBuf.Tail())
	return nil
}

func (t *undoTx) Begin() error {
	t.level += 1
	return nil
}

func (t *undoTx) Commit() error {
	if t.level == 0 {
		return errors.New("tx.undo: no transaction to commit!")
	}
	t.level--
	if t.level == 0 {
		defer t.unLock()
		/* Need to flush current value of logged areas. */
		for t.undoBuf.Tail() > 0 {
			_, err := t.undoBuf.Read(t.entrySlice)
			if err != nil {
				return err
			}

			/* Flush change. */
			Persist(unsafe.Pointer(t.undoEntry.offset+undoOff), t.undoEntry.size)

			t.undoBuf.Rewind(t.undoEntry.size)
		}
		if t.undoBuf.Tail() != 0 {
			debug.PrintStack()
			log.Fatal("tx.undo: buffer not correctly parsed when commit!", t.undoBuf.Tail())
			return errors.New("tx.undo: buffer not correctly parsed when commit!")
		}
		t.setUndoHdr(0) // discard all logs.
	}
	return nil
}

func (t *undoTx) Abort() error {
	defer t.unLock()
	t.level = 0
	for t.undoBuf.Tail() > 0 {
		_, err := t.undoBuf.Read(t.entrySlice)
		if err != nil {
			return err
		}
		ptr := unsafe.Pointer(undoOff + t.undoEntry.offset)
		_, err = t.undoBuf.Read((*[LBUFFERSIZE]byte)(ptr)[:t.undoEntry.size:t.undoEntry.size])
		if err != nil {
			return err
		}
	}
	if t.undoBuf.Tail() != 0 {
		return errors.New("tx.undo: buffer not correctly parsed when rollback!")
	}
	t.setUndoHdr(0)
	return nil
}

func (t *undoTx) RLock(m *sync.RWMutex) {
	m.RLock()
	//log.Println("Log ", t.id, " rlocking ", m)
	t.rlocks = append(t.rlocks, m)
}

func (t *undoTx) WLock(m *sync.RWMutex) {
	m.Lock()
	//log.Println("Log ", t.id, " wlocking ", m)
	t.wlocks = append(t.wlocks, m)
}

func (t *undoTx) Lock(m *sync.RWMutex) {
	t.WLock(m)
}

func (t *undoTx) unLock() {
	for _, m := range t.wlocks {
		//log.Println("Log ", t.id, " unlocking ", m)
		m.Unlock()
	}
	t.wlocks = t.wlocks[0:0]
	for _, m := range t.rlocks {
		//log.Println("Log ", t.id, " runlocking ", m)
		m.RUnlock()
	}
	t.rlocks = t.rlocks[0:0]
}
