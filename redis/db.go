///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause
///////////////////////////////////////////////////////////////////////

package redis

import (
	"time"

	"github.com/vmware/go-pmem-transaction/transaction"
)

// General db locking order:
// expire -> dict
// rehash Lock -> table Lock -> bucket Lock
// table0 -> table1
// bucket id ascending

var expired chan []byte = make(chan []byte, 100)

func (db *redisDb) Cron() {
	go db.dict.Cron(100 * time.Millisecond)
	go db.expire.Cron(100 * time.Millisecond)
	go db.expireCron(10 * time.Millisecond)
}

// active expire (only check table 0 for simplicity)
func (db *redisDb) expireCron(sleep time.Duration) {
	tx := transaction.NewUndoTx()
	i := 0
	ticker := time.NewTicker(sleep)
	for {
		select {
		case key := <-expired:
			tx.Exec(db.lockKeyWrite, key) // lockKeyWrite calls expireIfNeeded.
		case <-ticker.C:
			tx.Begin()
			tx.RLock(db.expire.lock)
			mask := db.expire.tab[0].mask
			i = i & mask
			s := db.expire.shard(i)
			db.expire.lockShard(tx, 0, s)
			e := db.expire.tab[0].bucket[i]
			for e != nil {
				when := e.value.(int64)
				now := time.Now().UnixNano()
				if when <= now {
					db.dict.lockKey(tx, e.key)
					db.delete(tx, e.key)
					e = e.next
					// only delete one expire key in each transaction to prevent
					// deadlock.
					break
				}
				e = e.next
			}
			if e == nil { // finished checking current bucket
				i++
			}
			tx.End()
		}
	}
	transaction.Release(tx)
}

func (db *redisDb) swizzle(tx transaction.TX) {
	db.dict.swizzle(tx)
	db.expire.swizzle(tx)
}

func existsCommand(c *client) {
	var count int64

	alive := c.db.lockKeysRead(c.tx, c.argv[1:], 1)

	for i, key := range c.argv[1:] {
		if alive[i] {
			if c.db.lookupKeyRead(c.tx, key) != nil {
				count++
			}
		}
	}
	c.addReplyLongLong(count)
}

func delCommand(c *client) {
	var count int64

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
	c.addReplyLongLong(int64(c.db.dict.size()))
}

func flushdbCommand(c *client) {
	c.db.lockTablesWrite(c.tx)
	c.db.expire.empty(c.tx)
	c.db.dict.empty(c.tx)
	c.addReply(shared.ok)
}

func selectCommand(c *client) {
	// TODO: not implemented
	c.addReply(shared.ok)
}

func randomkeyCommand(c *client) {
	c.db.dict.lockAllKeys(c.tx)
	if de := c.db.randomKey(); de != nil {
		c.addReplyBulk(de.key)
	} else {
		c.addReply(shared.nullbulk)
	}
}

func (db *redisDb) lockKeyWrite(tx transaction.TX, key []byte) {
	db.expire.lockKey(tx, key)
	db.dict.lockKey(tx, key)
	db.expireIfNeeded(tx, key)
}

func (db *redisDb) lockKeyRead(tx transaction.TX, key []byte) bool {
	db.expire.lockKey(tx, key)
	db.dict.lockKey(tx, key)
	return db.checkLiveKey(key)
}

func (db *redisDb) lockKeysWrite(tx transaction.TX, keys [][]byte, stride int) {
	db.expire.lockKeys(tx, keys, stride)
	db.dict.lockKeys(tx, keys, stride)
	for i := 0; i < len(keys)/stride; i++ {
		db.expireIfNeeded(tx, keys[i*stride])
	}
}

func (db *redisDb) lockKeysRead(tx transaction.TX, keys [][]byte, stride int) []bool {
	db.expire.lockKeys(tx, keys, stride)
	db.dict.lockKeys(tx, keys, stride)
	return db.checkLiveKeys(keys, stride)
}

func (db *redisDb) lockTablesWrite(tx transaction.TX) {
	tx.WLock(db.expire.rehashLock)
	tx.WLock(db.expire.lock)
	tx.WLock(db.dict.rehashLock)
	tx.WLock(db.dict.lock)
}

func (db *redisDb) checkLiveKeys(keys [][]byte, stride int) []bool {
	alive := make([]bool, len(keys))
	for i := 0; i < len(keys)/stride; i++ {
		alive[i*stride] = db.checkLiveKey(keys[i*stride])
	}
	return alive
}

func (db *redisDb) checkLiveKey(key []byte) bool {
	when := db.getExpire(key)
	if when < 0 {
		return true
	}
	now := time.Now().UnixNano()
	if now < when {
		return true
	}
	expired <- key
	return false
}

func (db *redisDb) lookupKeyWrite(tx transaction.TX, key []byte) interface{} {
	return db.lookupKey(key)
}

func (db *redisDb) lookupKeyRead(tx transaction.TX, key []byte) interface{} {
	return db.lookupKey(key)
}

func (db *redisDb) lookupKey(key []byte) interface{} {
	_, _, _, e := db.dict.find(key)
	if e != nil {
		return e.value
	}
	return nil
}

func (db *redisDb) randomKey() *entry {
	for {
		entry := db.dict.randomKey()
		if entry == nil {
			return entry
		}
		if db.checkLiveKey(entry.key) {
			return entry
		}
	}
}

// key and value data should be in pmem
func (db *redisDb) setKey(tx transaction.TX, key []byte, value interface{}) (insert bool) {
	db.removeExpire(tx, key)
	return db.dict.set(tx, key, value)
}

func (db *redisDb) delete(tx transaction.TX, key []byte) bool {
	db.expire.delete(tx, key)
	return (db.dict.delete(tx, key) != nil)
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
	when, ok := c.getLongLongOrReply(c.argv[2], nil)
	if !ok {
		return
	}

	expire := base.Add(time.Duration(when) * d)

	c.db.lockKeyWrite(c.tx, c.argv[1])

	if c.db.lookupKeyWrite(c.tx, c.argv[1]) == nil {
		c.addReply(shared.czero)
		return
	}

	// TODO: Expire with negative ttl or with timestamp into the past should
	// never executed as a DEL when load AOF or in the context of a slave.
	if expire.Before(time.Now()) {
		c.db.delete(c.tx, c.argv[1])
		c.addReply(shared.cone)
		return
	} else {
		// int64 value should be inlined in interface, therefore should also be
		// persisted after set.
		c.db.setExpire(c.tx, c.argv[1], expire.UnixNano())
		c.addReply(shared.cone)
		return
	}
}

func (db *redisDb) setExpire(tx transaction.TX, key []byte, expire interface{}) {
	_, _, _, e := db.dict.find(key) // share the main dict key to save space
	if e == nil {
		panic("Trying set expire on non-existing key!")
	}
	db.expire.set(tx, e.key, expire)
}

func (db *redisDb) removeExpire(tx transaction.TX, key []byte) bool {
	return db.expire.delete(tx, key) != nil
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

func (db *redisDb) getExpire(key []byte) int64 {
	_, _, _, e := db.expire.find(key)
	if e == nil {
		return -1
	}

	switch value := e.value.(type) {
	case int64:
		return value
	default:
		panic("Expire value type error!")
	}
}

func ttlCommand(c *client) {
	ttlGeneric(c, false)
}

func pttlCommand(c *client) {
	ttlGeneric(c, true)
}

func ttlGeneric(c *client, ms bool) {
	var ttl int64 = -1

	c.db.expire.lockKey(c.tx, c.argv[1])
	c.db.dict.lockKey(c.tx, c.argv[1])

	if c.db.lookupKeyRead(c.tx, c.argv[1]) == nil {
		c.addReplyLongLong(-2)
		return
	}

	expire := c.db.getExpire(c.argv[1])
	if expire > 0 {
		ttl = expire - time.Now().UnixNano()
		if ttl < 0 {
			ttl = 0
		}
	}
	if ttl == -1 {
		c.addReplyLongLong(-1)
	} else {
		if ms {
			ttl = (ttl + 500000) / 1000000
		} else {
			ttl = (ttl + 500000000) / 1000000000
		}
		c.addReplyLongLong(ttl)
	}
}

func persistCommand(c *client) {
	c.db.lockKeyWrite(c.tx, c.argv[1])
	_, _, _, e := c.db.dict.find(c.argv[1])
	if e == nil {
		c.addReply(shared.czero)
	} else {
		if c.db.removeExpire(c.tx, c.argv[1]) {
			c.addReply(shared.cone)
		} else {
			c.addReply(shared.czero)
		}
	}
}
