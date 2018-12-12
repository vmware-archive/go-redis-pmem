package region

import (
	"go-pmem-transaction/transaction"
	"log"
	"runtime"
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
	// dbRoot is the pointer to the database root datastructure.
	dbRoot,
	// gcPtr is used by the transaction library to store the pointer to the log area
	gcPtr unsafe.Pointer
}

// _region is the application root datastructure
var _region *pmemHeader

// Initialize the persistent memory header metadata
func Init(size int) unsafe.Pointer {
	_region = pnew(pmemHeader)
	_region.magic = MAGIC
	_region.size = size
	_region.gcPtr = transaction.Init(nil, "undo")
	runtime.PersistRange(unsafe.Pointer(_region), unsafe.Sizeof(*_region))
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
	_region.gcPtr = transaction.Init(_region.gcPtr, "undo")
}

func SetDbRoot(ptr unsafe.Pointer) {
	_region.dbRoot = ptr
	runtime.PersistRange(unsafe.Pointer(&_region.dbRoot),
		unsafe.Sizeof(_region.dbRoot))
}

func GetDbRoot() unsafe.Pointer {
	return _region.dbRoot
}
