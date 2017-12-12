package redis

import (
	_ "fmt"
	_ "pmem/transaction"
	"strconv"
	"time"
)

const (
	OBJ_SET_NO_FLAGS = 0
	OBJ_SET_NX       = 1 << 0
	OBJ_SET_XX       = 1 << 1
	OBJ_SET_EX       = 1 << 2
	OBJ_SET_PX       = 1 << 3
)

// SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>]
func setCommand(c *client) {
	ms := false
	var expire []byte
	flags := OBJ_SET_NO_FLAGS

	for i := 3; i < c.argc; i++ {
		curr := c.argv[i]
		var next []byte
		if i < c.argc-1 {
			next = c.argv[i+1]
		}
		if (curr[0] == 'n' || curr[0] == 'N') && (curr[1] == 'x' || curr[1] == 'X') && len(curr) == 2 && (flags&OBJ_SET_XX) == 0 {
			flags |= OBJ_SET_NX
		} else if (curr[0] == 'x' || curr[0] == 'X') && (curr[1] == 'x' || curr[1] == 'X') && len(curr) == 2 && (flags&OBJ_SET_NX) == 0 {
			flags |= OBJ_SET_XX
		} else if (curr[0] == 'e' || curr[0] == 'E') && (curr[1] == 'x' || curr[1] == 'X') && len(curr) == 2 && (flags&OBJ_SET_PX) == 0 {
			flags |= OBJ_SET_EX
			expire = next
			i++
		} else if (curr[0] == 'p' || curr[0] == 'P') && (curr[1] == 'x' || curr[1] == 'X') && len(curr) == 2 && (flags&OBJ_SET_EX) == 0 {
			flags |= OBJ_SET_PX
			ms = true
			expire = next
			i++
		} else {
			c.addReply(shared.syntaxerr)
			return
		}
	}
	c.setGeneric(flags, c.argv[1], c.argv[2], expire, ms, nil, nil)
}

func setnxCommand(c *client) {
	c.setGeneric(OBJ_SET_NX, c.argv[1], c.argv[2], nil, false, shared.cone, shared.czero)
}

func setexCommand(c *client) {
	c.setGeneric(OBJ_SET_NO_FLAGS, c.argv[1], c.argv[3], c.argv[2], false, nil, nil)
}

func psetexCommand(c *client) {
	c.setGeneric(OBJ_SET_NO_FLAGS, c.argv[1], c.argv[3], c.argv[2], true, nil, nil)
}

func (c *client) setGeneric(flags int, key, val []byte, expire []byte, ms bool, okReply, abortReply []byte) {
	var ns int64 = -1
	if expire != nil {
		t, err := c.getLongLongFromObjectOrReply(expire, nil)
		if err != nil {
			return
		}
		if t <= 0 {
			c.addReplyError([]byte("invalid expire time"))
			return
		}
		if ms {
			ns = t * 1000000
		} else {
			ns = t * 1000000000
		}
	}

	c.db.lockKeyWrite(c.tx, key)
	if ((flags&OBJ_SET_NX) > 0 && c.db.lookupKeyWrite(c.tx, key) != nil) || ((flags&OBJ_SET_XX) > 0 && c.db.lookupKeyWrite(c.tx, key) == nil) {
		if abortReply != nil {
			c.addReply(abortReply)
		} else {
			c.addReply(shared.nullbulk)
		}
		return
	}
	c.db.setKey(c.tx, key, val)
	if expire != nil {
		when := time.Now().UnixNano() + ns
		c.db.setExpire(c.tx, key, strconv.AppendInt(nil, when, 10))
	}

	if okReply != nil {
		c.addReply(okReply)
	} else {
		c.addReply(shared.ok)
	}
}

func getCommand(c *client) {
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		c.getGeneric()
	} else { // expired
		c.addReply(shared.nullbulk)
	}
}

func getsetCommand(c *client) {
	c.db.lockKeyWrite(c.tx, c.argv[1])
	if !c.getGeneric() {
		return
	}
	c.db.setKey(c.tx, c.argv[1], c.argv[2])
}

func (c *client) getGeneric() bool {
	v, ok := c.getStringOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), shared.nullbulk, shared.wrongtypeerr)
	if v != nil {
		c.addReplyBulk(v)
	}
	return ok
}

func setrangeCommand(c *client) {
	offset, err := slice2i(c.argv[2])
	if err != nil {
		c.addReplyError([]byte("value is not an integer"))
	}
	if offset < 0 {
		c.addReplyError([]byte("offset is out of range"))
		return
	}
	update := c.argv[3]
	c.db.lockKeyWrite(c.tx, c.argv[1])
	v, ok := c.getStringOrReply(c.db.lookupKeyWrite(c.tx, c.argv[1]), nil, shared.wrongtypeerr)
	if !ok {
		return
	}

	if v == nil {
		if len(update) == 0 {
			c.addReply(shared.czero)
			return
		}
		newv := make([]byte, offset, offset+len(update)) // TODO: need to allocate in pmem
		c.db.setKey(c.tx, c.argv[1], newv)
		v = newv
	} else {
		olen := len(v)
		if len(update) == 0 {
			c.addReplyLongLong(olen)
			return
		}
	}

	if len(update) > 0 {
		needed := offset + len(update)
		if needed > len(v) {
			if offset > len(v) {
				v = append(v, make([]byte, offset-len(v))...) // TODO: need to allocate in pmem
			}
			newv := append(v[:offset], update...) // TODO: need to allocate in pmem
			c.db.setKey(c.tx, c.argv[1], newv)
			c.addReplyLongLong(needed)
		} else {
			c.tx.Log(v[offset:needed])
			copy(v[offset:], update)
			c.addReplyLongLong(len(v))
		}
	}
}

func getrangeCommand(c *client) {
	start, err := slice2i(c.argv[2])
	if err != nil {
		c.addReplyError([]byte("value is not an integer"))
	}
	end, err := slice2i(c.argv[3])
	if err != nil {
		c.addReplyError([]byte("value is not an integer"))
	}

	var v []byte
	var ok bool
	if c.db.lockKeyRead(c.tx, c.argv[1]) { // not expired
		v, ok = c.getStringOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), nil, shared.wrongtypeerr)
		if v == nil || !ok {
			return
		}
	}

	strlen := len(v)

	if start < 0 {
		start += strlen
	}
	if end < 0 {
		end += strlen
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= strlen {
		end = strlen - 1
	}

	if start > end || strlen == 0 {
		c.addReply(shared.emptybulk)
	} else {
		c.addReplyBulk(v[start : end+1])
	}
}

func mgetCommand(c *client) {
	c.addReplyMultiBulkLen(c.argc - 1)
	alives := c.db.lockKeysRead(c.tx, c.argv[1:], 1)
	for i, k := range c.argv[1:] {
		if alives[i] {
			v, _ := c.getStringOrReply(c.db.lookupKeyRead(c.tx, k), shared.nullbulk, shared.nullbulk)
			if v != nil {
				c.addReplyBulk(v)
			}
		} else { // expired key
			c.addReply(shared.nullbulk)
		}
	}
}

func msetCommand(c *client) {
	c.msetGeneric(false)
}

func msetnxCommand(c *client) {
	c.msetGeneric(true)
}

func (c *client) msetGeneric(nx bool) {
	if c.argc%2 == 0 {
		c.addReplyError([]byte("wrong number of arguments for MSET"))
		return
	}
	c.db.lockKeysWrite(c.tx, c.argv[1:], 2)
	if nx {
		for i := 1; i < c.argc; i += 2 {
			if c.db.lookupKeyWrite(c.tx, c.argv[i]) != nil {
				c.addReply(shared.czero)
				return
			}
		}
	}

	for i := 1; i < c.argc; i += 2 {
		c.db.setKey(c.tx, c.argv[i], c.argv[i+1])
	}
	if nx {
		c.addReply(shared.cone)
	} else {
		c.addReply(shared.ok)
	}
}

func appendCommand(c *client) {
	totlen := 0
	c.db.lockKeyWrite(c.tx, c.argv[1])
	v, ok := c.getStringOrReply(c.db.lookupKeyWrite(c.tx, c.argv[1]), nil, shared.wrongtypeerr)
	if !ok {
		return
	}
	if v == nil {
		/* Create the key */
		c.db.setKey(c.tx, c.argv[1], c.argv[2])
		totlen = len(c.argv[2])
	} else {
		/* Append the value */
		newv := append(v, c.argv[2]...) // TODO: need to create newv in pmem
		totlen = len(newv)
		c.db.setKey(c.tx, c.argv[1], newv)
	}
	c.addReplyLongLong(totlen)
}

func strlenCommand(c *client) {
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		v, _ := c.getStringOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), shared.czero, shared.wrongtypeerr)
		if v != nil {
			c.addReplyLongLong(len(v))
		}
	} else { // expired
		c.addReply(shared.czero)
	}
}
