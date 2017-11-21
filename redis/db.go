package redis

import (
	"pmem/transaction"
	"strconv"
	"time"
)

func existsCommand(c *client) {
	count := 0

	c.db.lockKeysRead(c.tx, c.argv[1:], 1)

	for _, key := range c.argv[1:] {
		if c.db.lookupKeyRead(c.tx, key) != nil {
			count++
		}
	}
	c.addReplyLongLong(count)
}

func delCommand(c *client) {
	count := 0

	c.db.lockKeysWrite(c.tx, c.argv[1:], 1)

	for _, key := range c.argv[1:] {
		c.db.expireIfNeeded(c.tx, key)
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
	c.db.expire.lockTables(c.tx)
	c.db.dict.empty(c.tx)
	c.db.expire.empty(c.tx)
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

func (db *redisDb) lockKeyWrite(tx transaction.TX, key []byte) {
	db.dict.lockKey(tx, key)
	db.expire.lockKey(tx, key)
}

func (db *redisDb) lockKeyRead(tx transaction.TX, key []byte) {
	db.handleExpireKey(key)
	db.dict.lockKey(tx, key)
}

func (db *redisDb) lockKeysWrite(tx transaction.TX, keys [][]byte, stride int) {
	db.dict.lockKeys(tx, keys, stride)
	db.expire.lockKeys(tx, keys, stride)
}

func (db *redisDb) lockKeysRead(tx transaction.TX, keys [][]byte, stride int) {
	db.handleExpireKeys(keys, stride)
	db.dict.lockKeys(tx, keys, stride)
}

// called by readonly commands before any command lock acquires and operations.
func (db *redisDb) handleExpireKeys(keys [][]byte, stride int) {
	for i := 0; i < len(keys)/stride; i++ {
		db.handleExpireKey(keys[i*stride])
	}
}

// expire of each key can be considered a separate transaction.
func (db *redisDb) handleExpireKey(key []byte) {
	tx := transaction.NewUndo()
	tx.Begin()
	db.dict.lockKey(tx, key)
	db.expire.lockKey(tx, key)
	db.expireIfNeeded(tx, key)
	tx.Commit()
	transaction.Release(tx)
}

func (db *redisDb) lookupKeyWrite(tx transaction.TX, key []byte) []byte {
	db.expireIfNeeded(tx, key)
	return db.lookupKey(key)
}

func (db *redisDb) lookupKeyRead(tx transaction.TX, key []byte) []byte {
	// expires have to be handled separately before any operations in readonly commands.
	return db.lookupKey(key)
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

func (db *redisDb) setKey(tx transaction.TX, key, value []byte) (insert bool) {
	db.removeExpire(tx, key)
	return db.dict.set(tx, key, value)
}

func (db *redisDb) delete(tx transaction.TX, key []byte) bool {
	db.expire.delete(tx, key)
	return db.dict.delete(tx, key)
}

func expireCommand(c *client) {
	expireGeneric(c, time.Now(), time.Second)
}

func expireatCommand(c *client) {
	expireGeneric(c, time.Unix(0, 0), time.Second)
}

func pexpireCommand(c *client) {
	expireGeneric(c, time.Now(), time.Millisecond)
}

func pexpireatCommand(c *client) {
	expireGeneric(c, time.Unix(0, 0), time.Millisecond)
}

func expireGeneric(c *client, base time.Time, d time.Duration) {
	when, err := slice2i(c.argv[2])
	if err != nil {
		return
	}

	expire := base.Add(time.Duration(when) * d)

	c.db.lockKeyWrite(c.tx, c.argv[1])

	if c.db.lookupKeyWrite(c.tx, c.argv[1]) == nil {
		c.addReply(shared.czero)
		return
	}

	// TODO: Expire with negative ttl or with timestamp into the past should never executed as a DEL when load AOF or in the context of a slave.
	if expire.Before(time.Now()) {
		c.db.delete(c.tx, c.argv[1])
		c.addReply(shared.cone)
		return
	} else {
		c.db.setExpire(c.tx, c.argv[1], []byte(strconv.Itoa(int(expire.UnixNano())))) // TODO: directly store time in expire dict when support multiple types as value
		c.addReply(shared.cone)
		return
	}
}

func (db *redisDb) setExpire(tx transaction.TX, key, expire []byte) {
	_, _, _, e := db.dict.find(key) // share the main dict key to save space
	if e == nil {
		panic("Trying set expire on non-existing key!")
	}
	db.expire.set(tx, e.key, expire)
}

func (db *redisDb) removeExpire(tx transaction.TX, key []byte) bool {
	return db.expire.delete(tx, key)
}

// tx must be writable
func (db *redisDb) expireIfNeeded(tx transaction.TX, key []byte) {
	when := db.getExpire(key)
	if when < 0 {
		return
	}
	now := time.Now().UnixNano()
	if now < when {
		return
	}

	db.delete(tx, key)
}

func (db *redisDb) getExpire(key []byte) int64 { // TODO: directly use time struct
	_, _, _, e := db.expire.find(key)
	if e == nil {
		return -1
	}

	when, err := slice2i(e.value)
	if err != nil {
		panic("Expire value is not int!")
	}

	return int64(when)
}
