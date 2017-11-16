package redis

import (
	"fmt"
	"pmem/transaction"
)

func setCommand(c *client) {
	tx := transaction.NewUndo()
	//fmt.Println("Set", string(c.argv[1]), string(c.argv[2]))
	a, u, v := c.db.Set(tx, c.argv[1], c.argv[2])
	transaction.Release(tx)
	c.add += a
	c.update += u
	c.avg = c.avg + (v - c.avg)/float64(c.add + c.update)
	if (c.add + c.update) % 100000 == 0 {
		fmt.Println("Client set statistics:", c.add, "inserts", c.update, "updates", c.avg, "avg compares per search.")
		c.add = 0
		c.update = 0
		c.avg = 0
	}  
	c.addReply(ok)
}

func getCommand(c *client) {
	tx := transaction.NewReadonly()
	v := c.db.Get(tx, c.argv[1])
	//fmt.Println("Get", string(c.argv[1]), v)
	transaction.Release(tx)
	if len(v) == 0 {
		c.addReply(nullbulk)
		//c.addReplyBulk(foo)
	} else {
		c.addReplyBulk(v)
	}
}