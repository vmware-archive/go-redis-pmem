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
    _header heapHeader
    _data []byte
)

func Init(data []byte, size int) {
    _data = data
    hdSize := int(unsafe.Sizeof(_header))
    headerSlice := (*[1<<30]byte)(unsafe.Pointer(&_header))[:hdSize:hdSize]
    copy(headerSlice, _data)
    if _header.end == 0 {
        // first time initialization
        _header.end = size
        _header.offset = hdSize
        tx.Begin()
        tx.UpdateRedo(unsafe.Pointer(&_data[0]), 
                      unsafe.Pointer(&_header), 
                      unsafe.Sizeof(_header))
        tx.Commit()
        // log.Println("heap header initialized: ", _header)
    } else {
        if _header.end != size {
            log.Fatal("Heap size does not match!")
        }
    }
}

func Alloc(size int) unsafe.Pointer {
    offset := uintptr(_header.offset)
    _header.offset += size
    if _header.offset < _header.end {
        tx.Begin()
        tx.UpdateRedo(unsafe.Pointer(&_data[0]), 
                      unsafe.Pointer(&_header), 
                      unsafe.Sizeof(_header))
        tx.Commit()
    } else {
        log.Fatal("Run out of heap!")
    }
    return unsafe.Pointer(&_data[offset])
}
