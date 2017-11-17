package redis

import (
	"pmem/transaction"
)

func existsCommand(c *client) {
	count := 0

	c.db.dict.lockKeys(c.tx, c.argv[1:])

	for _, key := range(c.argv[1:]) {
		if c.db.lookupKey(key) != nil {
			count++
		}
	}
	c.addReplyLongLong(count)
}

func delCommand(c *client) {
	count := 0

	c.db.dict.lockKeys(c.tx, c.argv[1:])

	for _, key := range(c.argv[1:]) {
		if c.db.delete(c.tx, key) {
			count++
		}
	}
	c.addReplyLongLong(count)	
}

func dbsizeCommand(c *client) {
	c.db.dict.lockAllKeys(c.tx)
	c.addReplyLongLong(c.db.dict.size())
}

func flushdbCommand(c *client) {
	c.db.dict.lockTables(c.tx)
	c.db.dict.empty(c.tx)
	c.addReply(shared.ok)
}

func randomkeyCommand(c *client) {
	c.db.dict.lockAllKeys(c.tx)
	if k := c.db.randomKey(); k != nil {
		c.addReplyBulk(k)
	} else {
		c.addReply(shared.nullbulk)
	}
}

func (db *redisDb) lookupKey(key []byte) []byte {
	_, _, _, e := db.dict.find(key)
	if e != nil {
		return e.value
	} else {
		return nil
	}
}

func (db *redisDb) randomKey() []byte {
	for {
		k := db.dict.randomKey()
		return k
	}
}

func (db *redisDb) setKey(tx transaction.TX, key, value []byte) {
	db.dict.set(tx, key, value)
}

func (db *redisDb) delete(tx transaction.TX, key []byte) bool {
	return db.dict.delete(tx, key)
}
