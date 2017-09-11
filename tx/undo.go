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
	"reflect"
	"unsafe"
	"log"
)

type undoHeader struct {
	tail int              // current offset of log buffer
}

type entryHeader struct {
	offset uintptr
	size int
}

var (
	undoOff uintptr                 // offset of undo area
	undoHdr *undoHeader				// transaction header
	undoBuf logBuffer				// volatile wrapper for log buffer
	undoEntry entryHeader			// volatile entry header
	entrySlice []byte  				// underlying raw byte slice of undoEntry
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

func commitUndo() error {
	/* Need to flush current value of logged areas. */
	for undoBuf.Tail() > 0 {
		_, err := undoBuf.Read(entrySlice)
		if err != nil {
			return err
		}
		
		/* Flush change. */
		// flush(undoEntry.offset, undoEntry.size)

		undoBuf.Rewind(undoEntry.size)
	}
	setUndoHdr(undoBuf.Tail()) // discard logs.
	return nil
}

func rollbackUndo() error {
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
	setUndoHdr(undoBuf.Tail())
	return nil
}
