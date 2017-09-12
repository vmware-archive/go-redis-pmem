package heap

/*
 * a toy implementation that allocate linearly.
 * just to enable examples before a real implementation is avaliable
 * will remove this later
 */

import (
    "pmem/tx"
    "log"
    "unsafe"
)

type heapHeader struct {
    offset,
    end int
}

var (
    _header *heapHeader
    _data []byte
)

func Init(data []byte, size int) {
    _data = data
    _header = (*heapHeader)(unsafe.Pointer(&_data[0]))
    hdSize := int(unsafe.Sizeof(*_header))

    tx.Begin()
    tx.LogUndo(_header)
    if _header.end == 0 {
        // first time initialization
        _header.end = size
        _header.offset = hdSize
    } else {
        if _header.end != size {
            log.Fatal("Heap size does not match!")
        }
    }
    tx.Commit()
}

func Alloc(size int) unsafe.Pointer {
    offset := uintptr(_header.offset)
    tx.Begin()
    tx.LogUndo(_header)
    if _header.offset + size < _header.end {
        _header.offset += size
    } else {
        log.Fatal("Run out of heap!")
    }
    tx.Commit()
    return unsafe.Pointer(&_data[offset])
}