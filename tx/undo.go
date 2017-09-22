package tx

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
		level      int         // tx level
		undoEntry  entryHeader // volatile entry header
	    entrySlice []byte      // underlying raw byte slice of undoEntry
	}
)

const (
	max int = 1    // currently only support one outstanding transaction
)

var (
	pool       []*undoTx
	undoOff    uintptr     // offset of undo area
)

func initUndo(id int, logArea []byte) *undoTx{
	t := new(undoTx)
	t.undoHdr = (*undoHeader)(unsafe.Pointer(&logArea[0]))

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
	t.entrySlice = (*[LOGSIZE]byte)(ptr)[:size:size]

	return t
}

// currently actually only support single thread and single tx
func InitUndo(logArea []byte) {
	// init global variables
	undoOff = uintptr(unsafe.Pointer(&logArea[0]))

	// init transaction pool
	pool = make([]*undoTx, max)
	for i, _ := range pool {
		begin := len(logArea)/max*i
		end := len(logArea)/max*(i+1)
		pool[i] = initUndo(i, logArea[begin:end])
	}
}

func newUndo() *undoTx {
	var t *undoTx = nil
	if len(pool) > 0 {
		t = pool[len(pool) - 1]
		pool = pool[:len(pool) - 1]
	}
	return t
}

func releaseUndo(t *undoTx) {
	t.Abort()
	pool= append(pool, t)
}

func (t *undoTx) setUndoHdr(tail int) {
	sfence()
	t.undoHdr.tail = tail  // atomic update
	Persist(unsafe.Pointer(t.undoHdr), int(unsafe.Sizeof(*t.undoHdr)))
	sfence()
}

func (t *undoTx) Log(data interface{}) error {
	// Check data type, get pointer and size of data.
	v := reflect.ValueOf(data)
	bytes := 0
	switch kind := v.Kind(); kind {
	case reflect.Array:
		fallthrough
	case reflect.Slice:
		bytes = v.Len() * int(v.Type().Elem().Size())
	case reflect.Ptr:
		bytes = int(v.Elem().Type().Size())
	default:
		return errors.New("tx.undo: Log data must be pointer/array/slice!")
	}
	ptr := unsafe.Pointer(v.Pointer())

	// Append data to undo log buffer. 
	_, err := t.undoBuf.Write((*[LOGSIZE]byte)(ptr)[:bytes:bytes])
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
			return errors.New("tx.undo: buffer not correctly parsed when commit!")
		}
		t.setUndoHdr(0) // discard all logs.
	} 
	return nil
}

func (t *undoTx) Abort() error {
	t.level = 0
	for t.undoBuf.Tail() > 0 {
		_, err := t.undoBuf.Read(t.entrySlice)
		if err != nil {
			return err
		}
		ptr := unsafe.Pointer(undoOff + t.undoEntry.offset)
		_, err = t.undoBuf.Read((*[LOGSIZE]byte)(ptr)[:t.undoEntry.size:t.undoEntry.size])
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