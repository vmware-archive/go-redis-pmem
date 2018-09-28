package region

import (
	"log"
	"pmem/transaction"
	"unsafe"
)

const (
	MAGIC = 657071
)

// Information about persistent memory region set as application root
type pmemHeader struct {
	// A magic constant stored at the beginning of the root section
	magic,
	// Persistent memory initialization size
	size int
	// The address at which persistent memory file is mapped
	mapAddr,
	// Address at which persistent memory region was mapped in an earlier run
	// of the application (if applicable). This will be used for pointer swizzling.
	oldMapAddr,
	// dbOffset is the offset of the database root pointer from the persistent
	// memory mapped address.
	dbOffset uintptr
	// dbRoot is the pointer to the database root datastructure.
	dbRoot,
	// gcPtr is used by the transaction library to store the pointer to the log area
	gcPtr unsafe.Pointer
}

// _region is the application root datastructure
var _region *pmemHeader

// Initialize the persistent memory header metadata
func Init(mapAddr unsafe.Pointer, size int) unsafe.Pointer {
	_region = pnew(pmemHeader)
	_region.magic = MAGIC
	_region.size = size
	_region.mapAddr = uintptr(mapAddr)
	_region.oldMapAddr = _region.mapAddr
	_region.gcPtr = transaction.Init(nil)

	transaction.Persist(unsafe.Pointer(_region), int(unsafe.Sizeof(*_region)))
	return unsafe.Pointer(_region)
}

// Re-initialize the persistent memory header metadata
func ReInit(regionPtr unsafe.Pointer, size int) {
	_region = (*pmemHeader)(regionPtr)
	if _region.magic != MAGIC {
		log.Fatal("Region magic does not match!")
	}
	if _region.gcPtr == nil {
		log.Fatal("gcPtr is nil")
	}
	if _region.size != size {
		log.Fatal("Region size does not match!")
	}
	// Currently not changing dbRoot, mapAddr, and oldMapAddr as swizzling is not supported
	transaction.Init(_region.gcPtr)
}

func SetDbRoot(ptr unsafe.Pointer) {
	_region.dbOffset = uintptr(ptr) - _region.mapAddr
	_region.dbRoot = ptr
	transaction.Persist(unsafe.Pointer(_region), int(unsafe.Sizeof(*_region)))
}

func GetDbRoot() unsafe.Pointer {
	if _region.dbOffset == 0 {
		return nil
	}
	return unsafe.Pointer(_region.dbOffset + _region.mapAddr)
}

func Swizzle(ptr unsafe.Pointer) unsafe.Pointer {
	var newptr unsafe.Pointer
	if ptr != nil {
		newptr = unsafe.Pointer(uintptr(ptr) - _region.oldMapAddr + _region.mapAddr)
	}
	// log.Println("Swizzle", ptr, " to ", newptr)
	return newptr
}
