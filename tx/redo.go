package tx

import (
    "unsafe"
)

func UpdateRedo(dst unsafe.Pointer, src unsafe.Pointer, size uintptr) {
    // currently just directly update without logging
    // hack here (unsafe): directly convert pointer to byte array
    dstSlice := (*[LOGSIZE]byte)(dst)[:size:size] 
    srcSlice := (*[LOGSIZE]byte)(src)[:size:size] 
    copy(dstSlice, srcSlice)
    FlushRange(dst, size)
}