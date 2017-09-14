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
	level      int         // nested tx level
	nest       [10]int     // nested tx info
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
	level = 1
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
	/* TODO: Need a fence here. */
	undoHdr.tail = tail
	/* TODO: Need to flush updates here. */
	/* TODO: Need another fence here. */
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
	if level > 10 {
		return errors.New("tx.undo: reached maximum nested transaction level!")
	}
	nest[level-1] = undoBuf.Tail()
	return nil
}

func commitUndo() error {
	if level <= 0 {
		return errors.New("tx.undo: no transaction to commit!")
	}
	/* Need to flush current value of logged areas. */
	for undoBuf.Tail() > nest[level-1] {
		_, err := undoBuf.Read(entrySlice)
		if err != nil {
			return err
		}

		/* Flush change. */
		// flush(undoEntry.offset, undoEntry.size)

		undoBuf.Rewind(undoEntry.size)
	}
	level -= 1
	if level == 0 { // top level transaction committed
		if undoBuf.Tail() != 0 {
			return errors.New("tx.undo: buffer not correctly parsed when commit!")
		}
		setUndoHdr(0) // discard all logs.
	} /* else {
		mfence()
	} */
	return nil
}

func rollbackUndo() error {
	for undoBuf.Tail() > nest[level-1] {
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
	if undoBuf.Tail() != nest[level-1] {
		return errors.New("tx.undo: buffer not correctly parsed when rollback!")
	}
	setUndoHdr(undoBuf.Tail())
	return nil
}
