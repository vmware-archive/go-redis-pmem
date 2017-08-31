package region

import (
    "syscall"
    "os"
    "log"
    "unsafe"
    "pmem/tx"
    "pmem/heap"
)

const (
    MAGIC = 657071
)

/* 
 * basic information stored at the begining of region.
 */
type pmemHeader struct {
    magic, 
    uuid, 
    size int
    offset, 
    oldOffset, 
    rootOffset uintptr
}

// a volaitle copy of the region header
var _region pmemHeader

func mmap(fname string, size int) (fdata []byte) {
    f, err := os.OpenFile(fname, 
                          os.O_CREATE|os.O_RDWR, 
                          0666)
    if err != nil {
        log.Fatal(err)
    }
    err = f.Truncate(int64(size))
    if err != nil {
        log.Fatal(err)
    }
    fdata, err = syscall.Mmap(int(f.Fd()),
                              0,
                              size,
                              syscall.PROT_WRITE|syscall.PROT_READ,
                              syscall.MAP_SHARED)
    if err != nil {
        log.Fatal(err)
    }
    return fdata
}

func Init(pathname string, size, uuid int) {
    // (1) mmap region
    fdata := mmap(pathname, size)

    // (2) read pmem header from mmaped region
    hdSize := int(unsafe.Sizeof(_region))
    // hack here (unsafe): directly convert pointer to byte array
    regionSlice := (*[1<<30]byte)(unsafe.Pointer(&_region))[:hdSize:hdSize]
    copy(regionSlice, fdata)    

    // (3) init log and heap
    tx.Init(fdata[hdSize:(hdSize + tx.LOGSIZE)], size - hdSize)
    heapOffset := hdSize + tx.LOGSIZE
    heap.Init(fdata[heapOffset:], size - heapOffset)

    // (4) update pmem region header
    if _region.uuid == 0 {
        // first time initialization
        _region.magic = MAGIC
        _region.uuid = uuid
        _region.size = size
        _region.offset = uintptr(unsafe.Pointer(&fdata[0]))
        _region.oldOffset = _region.offset
        tx.Begin()
        tx.UpdateRedo(unsafe.Pointer(&fdata[0]), 
                      unsafe.Pointer(&_region), 
                      unsafe.Sizeof(_region))
        tx.Commit()
        copy(regionSlice, fdata)
        // log.Println("region header initialized: ", _region)
    } else if _region.uuid == uuid {
        // retrieve existing region
        if _region.magic != MAGIC {
            log.Fatal("Region magic does not match!")
        }
        if _region.size != size {
            log.Fatal("Region size does not match!")
        }
        _region.oldOffset = _region.offset
        _region.offset = uintptr(unsafe.Pointer(&fdata[0]))
        tx.Begin()
        tx.UpdateRedo(unsafe.Pointer(&fdata[0]), 
                      unsafe.Pointer(&_region), 
                      unsafe.Sizeof(_region))
        tx.Commit()
        copy(regionSlice, fdata)
        // log.Println("region header updated: ", _region)
    } else {
        log.Fatal("Region uuid does not match!")
    }
}

func SetRoot(ptr unsafe.Pointer) {
    _region.rootOffset = uintptr(ptr) - _region.offset
    tx.Begin()
    tx.UpdateRedo(unsafe.Pointer(_region.offset), 
                  unsafe.Pointer(&_region), 
                  unsafe.Sizeof(_region))
    tx.Commit()
    // log.Println("root offset updated: ", _region)
}

func GetRoot() unsafe.Pointer {
    if _region.rootOffset == 0 {
        return nil
    } else {
        return unsafe.Pointer(_region.rootOffset + _region.offset)
    }
}

func Swizzle(ptr unsafe.Pointer) unsafe.Pointer {
    var newptr unsafe.Pointer = nil
    if ptr != nil {
        newptr = unsafe.Pointer(uintptr(ptr) - _region.oldOffset + _region.offset)
    }
    // log.Println("Swizzle", ptr, " to ", newptr)
    return newptr
} 