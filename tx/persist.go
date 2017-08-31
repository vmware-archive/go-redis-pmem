package tx

/*
 * low level flush/fence, may need to call into c code
 */
 
import (
	"unsafe"
)

func FlushRange(ptr unsafe.Pointer, size uintptr) {
}

func Mfence() {
}