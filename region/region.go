package region

import (
	"log"
	"pmem/transaction"
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

func InitMeta(ptr unsafe.Pointer, size, uuid int) bool {
	_region = (*pmemHeader)(ptr)
	hdSize := int(unsafe.Sizeof(*_region))
	mdata := (*[1 << 32]byte)(ptr)[:size:size]

	transaction.Init(mdata[hdSize:])
	return initMeta(ptr, size, uuid)
}

func initMeta(ptr unsafe.Pointer, size, uuid int) bool {
	undoTx := transaction.NewUndo()
	isNew := false

	undoTx.Begin()
	undoTx.Log(_region)
	if _region.uuid == 0 {
		log.Println("Initializing empty region.")
		_region.magic = MAGIC
		_region.uuid = uuid
		_region.size = size
		_region.offset = uintptr(ptr)
		_region.oldOffset = _region.offset
		isNew = true
	} else if _region.uuid == uuid {
		log.Println("Retriving region info.")
		if _region.magic != MAGIC {
			log.Fatal("Region magic does not match!")
		}
		if _region.size != size {
			log.Fatal("Region size does not match!")
		}
		log.Println("Old mapping offset ", (unsafe.Pointer)(_region.offset), " New offset ", ptr)
		_region.oldOffset = _region.offset
		_region.offset = uintptr(ptr)
	} else {
		log.Fatal("Region uuid does not match!")
	}
	undoTx.Commit()
	transaction.Release(undoTx)
	return isNew
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
