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
)

var (
	undoOff    uintptr     // offset of undo area
	undoHdr    *undoHeader // transaction header
	undoBuf    logBuffer   // volatile wrapper for log buffer
	undoEntry  entryHeader // volatile entry header
	entrySlice []byte      // underlying raw byte slice of undoEntry
	level      int         // tx level
)

func initUndo(data []byte) {
	/* 1. Get offset and header. */
	undoOff = uintptr(unsafe.Pointer(&data[0]))
	undoHdr = (*undoHeader)(unsafe.Pointer(&data[0]))

	/* 2. Init buffer. */
	var err error
	undoBuf, err = initLinearUndoBuffer(data[unsafe.Sizeof(*undoHdr):], undoHdr.tail)
	if err != nil {
		log.Fatal(err)
	}

	/* 3. Rollback not committed transaction log. */
	err = rollbackUndo()
	if err != nil {
		log.Fatal(err)
	}

	/* 4. Init byte slice of volatile entryHeader (merely for ease of logging) */
	ptr := unsafe.Pointer(&undoEntry)
	size := unsafe.Sizeof(undoEntry)
	entrySlice = (*[LOGSIZE]byte)(ptr)[:size:size]
}

func setUndoHdr(tail int) {
	sfence()
	undoHdr.tail = tail
	Persist(unsafe.Pointer(undoHdr), int(unsafe.Sizeof(*undoHdr)))
	sfence()
}

func LogUndo(data interface{}) error {
	/* 1. Check data type, get pointer and size of data. */
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

	/* 2. Append data to undo log buffer. */
	_, err := undoBuf.Write((*[LOGSIZE]byte)(ptr)[:bytes:bytes])
	if err != nil {
		return err
	}
	/* 3. Append log header.*/
	undoEntry.offset = v.Pointer() - undoOff
	undoEntry.size = bytes
	_, err = undoBuf.Write(entrySlice)
	if err != nil {
		return err
	}

	/* 4. Update log offset in header. Int update should be atomic. */
	setUndoHdr(undoBuf.Tail())
	return nil
}

func beginUndo() error {
	level += 1
	return nil
}

func commitUndo() error {
	if level == 0 {
		return errors.New("tx.undo: no transaction to commit!")
	}
	level--
	if level == 0 {
		/* Need to flush current value of logged areas. */
		for undoBuf.Tail() > 0 {
			_, err := undoBuf.Read(entrySlice)
			if err != nil {
				return err
			}

			/* Flush change. */
			Persist(unsafe.Pointer(undoEntry.offset+undoOff), undoEntry.size)

			undoBuf.Rewind(undoEntry.size)
		}
		if undoBuf.Tail() != 0 {
			return errors.New("tx.undo: buffer not correctly parsed when commit!")
		}
		setUndoHdr(0) // discard all logs.
	} 
	return nil
}

func rollbackUndo() error {
	level = 0
	for undoBuf.Tail() > 0 {
		_, err := undoBuf.Read(entrySlice)
		if err != nil {
			return err
		}
		ptr := unsafe.Pointer(undoOff + undoEntry.offset)
		_, err = undoBuf.Read((*[LOGSIZE]byte)(ptr)[:undoEntry.size:undoEntry.size])
		if err != nil {
			return err
		}
	}
	if undoBuf.Tail() != 0 {
		return errors.New("tx.undo: buffer not correctly parsed when rollback!")
	}
	setUndoHdr(0)
	return nil
}

func Persist(p unsafe.Pointer, s int) {
	f := uintptr(p) &^ (CACHELINE - 1)
	l := (uintptr(p) + uintptr(s) - 1) &^ (CACHELINE - 1)
	for f <= l {
		clflush(f)
		f += CACHELINE
	}
}

func sfence()

func clflush(uintptr)
