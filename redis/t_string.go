package redis

import (
	_ "fmt"
	_ "pmem/transaction"
)

func setCommand(c *client) {
	c.db.dict.lockKey(c.tx, c.argv[1])
	c.db.setKey(c.tx, c.argv[1], c.argv[2])
	c.addReply(shared.ok)
}

func getCommand(c *client) {
	c.db.dict.lockKey(c.tx, c.argv[1])
	c.getGeneric()
}

func getsetCommand(c *client) {
	c.db.dict.lockKey(c.tx, c.argv[1])
	c.getGeneric()
	c.db.setKey(c.tx, c.argv[1], c.argv[2])
}

func (c *client) getGeneric() {
	v := c.db.lookupKey(c.argv[1])
	if v == nil {
		c.addReply(shared.nullbulk)
	} else {
		c.addReplyBulk(v)
	}
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
	c.db.dict.lockKey(c.tx, c.argv[1])
	v := c.db.lookupKey(c.argv[1])

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

	if len(update) == 0 {
		if v == nil {
			c.addReply(shared.czero)
		} else {
			c.addReplyLongLong(len(v))
		}
	} else {
		needed := offset + len(update)
		if needed > len(v) {
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

	c.db.dict.lockKey(c.tx, c.argv[1])
	v := c.db.lookupKey(c.argv[1])
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
	c.db.dict.lockKeys(c.tx, c.argv[1:], 1)
	for _, k := range c.argv[1:] {
		v := c.db.lookupKey(k)
		if v == nil {
			c.addReply(shared.nullbulk)
		} else {
			c.addReplyBulk(v)
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
	c.db.dict.lockKeys(c.tx, c.argv[1:], 2)
	if nx {
		for i := 1; i < c.argc; i += 2 {
			if c.db.lookupKey(c.argv[i]) != nil {
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
	c.db.dict.lockKey(c.tx, c.argv[1])
	v := c.db.lookupKey(c.argv[1])
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
	c.db.dict.lockKey(c.tx, c.argv[1])

	v := c.db.lookupKey(c.argv[1])
	if v == nil {
		c.addReply(shared.czero)
	}
	c.addReplyLongLong(len(v))
}
