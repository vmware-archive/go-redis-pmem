package region

import (
	"log"
	"os"
	"pmem/heap"
	"pmem/transaction"
	"syscall"
	"unsafe"
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
var _region *pmemHeader

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
	_region = (*pmemHeader)(unsafe.Pointer(&fdata[0]))
	hdSize := int(unsafe.Sizeof(*_region))

	// (3) init log and heap
	transaction.Init(fdata[hdSize:(hdSize + transaction.LOGSIZE)])
	undoTx := transaction.NewUndo()
	heapOffset := hdSize + transaction.LOGSIZE
	heap.Init(undoTx, fdata[heapOffset:], size-heapOffset)

	// (4) update pmem region header
	undoTx.Begin()
	undoTx.Log(_region)
	if _region.uuid == 0 {
		log.Println("Initializing empty region.")
		_region.magic = MAGIC
		_region.uuid = uuid
		_region.size = size
		_region.offset = uintptr(unsafe.Pointer(&fdata[0]))
		_region.oldOffset = _region.offset
	} else if _region.uuid == uuid {
		log.Println("Retriving region info.")
		if _region.magic != MAGIC {
			log.Fatal("Region magic does not match!")
		}
		if _region.size != size {
			log.Fatal("Region size does not match!")
		}
		_region.oldOffset = _region.offset
		_region.offset = uintptr(unsafe.Pointer(&fdata[0]))
	} else {
		log.Fatal("Region uuid does not match!")
	}
	undoTx.Commit()
	transaction.Release(undoTx)
}

func SetRoot(t transaction.TX, ptr unsafe.Pointer) {
	t.Begin()
	t.Log(&_region.rootOffset)
	_region.rootOffset = uintptr(ptr) - _region.offset
	t.Commit()
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
