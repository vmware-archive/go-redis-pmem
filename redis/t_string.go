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
	v := c.db.lookupKey(c.argv[1])
	if v == nil {
		c.addReply(shared.nullbulk)
	} else {
		c.addReplyBulk(v)
	}
}