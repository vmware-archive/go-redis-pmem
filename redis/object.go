package redis

import (
	_ "fmt"
	"pmem/transaction"
	"strconv"
	"unsafe"
)

func (c *client) getStringOrReply(i interface{}, emptymsg []byte, errmsg []byte) ([]byte, bool) {
	s, ok := getString(i)
	if !ok {
		if errmsg != nil {
			c.addReply(errmsg)
		} else {
			c.addReply(shared.wrongtypeerr)
		}
	} else {
		if emptymsg != nil && s == nil {
			c.addReply(emptymsg)
		}
	}
	return s, ok
}

func getString(i interface{}) ([]byte, bool) {
	if i == nil {
		return nil, true
	}
	switch v := i.(type) {
	case []byte:
		return v, true
	case *[]byte:
		return *v, true
	case float64:
		return []byte(strconv.FormatFloat(v, 'f', 17, 64)), true
	case int64:
		return []byte(strconv.FormatInt(v, 10)), true
	default:
		return nil, false
	}
}

func (c *client) getLongLongOrReply(i interface{}, errmsg []byte) (int64, bool) {
	ll, ok := getLongLong(i)
	if !ok {
		if errmsg != nil {
			c.addReply(errmsg)
		} else {
			c.addReplyError([]byte("value is not an integer or out of range"))
		}
	}
	return ll, ok
}

func getLongLong(i interface{}) (int64, bool) {
	if i == nil {
		return int64(0), true
	}
	switch v := i.(type) {
	case int64:
		return v, true
	case []byte:
		i, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return int64(0), false
		} else {
			return i, true
		}
	case *[]byte:
		i, err := strconv.ParseInt(string(*v), 10, 64)
		if err != nil {
			return int64(0), false
		} else {
			return i, true
		}
	default:
		return int64(0), false
	}
}

func (c *client) getLongDoubleOrReply(i interface{}, errmsg []byte) (float64, bool) {
	f, ok := getLongDouble(i)
	if !ok {
		if errmsg != nil {
			c.addReply(errmsg)
		} else {
			c.addReplyError([]byte("value is not a valid float"))
		}
	}
	return f, ok
}

func getLongDouble(i interface{}) (float64, bool) {
	if i == nil {
		return float64(0), true
	}
	switch v := i.(type) {
	case float64:
		return v, true
	case []byte:
		i, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return float64(0), false
		} else {
			return i, true
		}
	case *[]byte:
		i, err := strconv.ParseFloat(string(*v), 64)
		if err != nil {
			return float64(0), false
		} else {
			return i, true
		}
	default:
		return float64(0), false
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
	if len(v) > 0 {
		copy(pv, v)
		transaction.Persist(unsafe.Pointer(&pv[0]), len(pv)) // shadow update needs to be flushed
	}
	return pv
}

func shadowCopyToPmemI(v []byte) interface{} {
	pv := pnew([]byte) // a walk around to slove the pass by value problem of slice
	*pv = pmake([]byte, len(v))
	if len(v) > 0 {
		copy(*pv, v)
		transaction.Persist(unsafe.Pointer(&(*pv)[0]), len(*pv)) // shadow update needs to be flushed
	}
	return pv // make sure interface is pointing to a in pmem slice header
}
