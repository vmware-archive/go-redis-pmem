package redis

import (
	_ "fmt"
	"pmem/transaction"
	"unsafe"
)

func (c *client) getStringOrReply(i interface{}, emptymsg []byte, errmsg []byte) ([]byte, bool) {
	if i == nil {
		if emptymsg != nil {
			c.addReply(emptymsg)
		}
		return nil, true
	}
	switch v := i.(type) {
	case []byte:
		return v, true
	case *[]byte:
		return *v, true
	default:
		if errmsg != nil {
			c.addReply(errmsg)
		}
		return nil, false
	}
}

func (c *client) getHashOrReply(i interface{}, emptymsg []byte, errmsg []byte) (interface{}, bool) {
	if i == nil {
		if emptymsg != nil {
			c.addReply(emptymsg)
		}
		return nil, true
	}
	switch i.(type) { //TODO: support ziplist
	case *dict:
		return i, true
	default:
		if errmsg != nil {
			c.addReply(errmsg)
		}
		return nil, false
	}
}

func (c *client) getZsetOrReply(i interface{}, emptymsg []byte, errmsg []byte) (interface{}, bool) {
	if i == nil {
		if emptymsg != nil {
			c.addReply(emptymsg)
		}
		return nil, true
	}
	switch i.(type) { //TODO: support ziplist
	case *zset:
		return i, true
	default:
		if errmsg != nil {
			c.addReply(errmsg)
		}
		return nil, false
	}
}

// slice and interface are passed by value, so the returned slice/interface maybe actually in volatile memory, and only the underlying data is in pmem.
func shadowCopyToPmem(v []byte) []byte {
	pv := pmake([]byte, len(v)) // here pv is actually in volatile memory, but it's pointing to in pmem array.
	copy(pv, v)
	transaction.Persist(unsafe.Pointer(&pv[0]), len(pv)) // shadow update needs to be flushed
	return pv
}

func shadowCopyToPmemI(v []byte) interface{} {
	pv := pnew([]byte) // a walk around to slove the pass by value problem of slice
	*pv = pmake([]byte, len(v))
	copy(*pv, v)
	transaction.Persist(unsafe.Pointer(&(*pv)[0]), len(*pv)) // shadow update needs to be flushed
	return pv                                                // make sure interface is pointing to a in pmem slice header
}
