package redis

/* Currently, write lock of the hash value will be acquired for all write hash commands (since we donot know wheter the command updates inner hash fields only or will also update the outter hash value).
 * Therefore, there is no extra lock required for field access within the hash value.
 * However, the outter lock also prevents concurrent writes or read/write into the same hash value. */

import "pmem/transaction"

type hashTypeIterator struct {
	subject interface{}
	di      *dictIterator
	de      *entry
}

/*============== hash type commands ====================*/

func hsetnxCommand(c *client) {
	c.db.lockKeyWrite(c.tx, c.argv[1])
	o := hashTypeLookupWriteOrCreate(c, c.argv[1])

	if hashTypeExists(o, c.argv[2]) {
		c.addReply(shared.czero)
	} else {
		hashTypeSet(c, o, c.argv[2], c.argv[3])
		c.addReply(shared.cone)
	}
}

func hsetCommand(c *client) {
	if c.argc%2 == 1 {
		c.addReplyError([]byte("wrong number of arguments for HMSET"))
		return
	}

	c.db.lockKeysWrite(c.tx, c.argv[1:], 2)

	o := hashTypeLookupWriteOrCreate(c, c.argv[1])
	if o == nil {
		return
	}

	created := 0
	for i := 2; i < c.argc; i += 2 {
		if !hashTypeSet(c, o, c.argv[i], c.argv[i+1]) {
			created++
		}
	}

	cmdname := c.argv[0]
	if cmdname[1] == 's' || cmdname[1] == 'S' { // HSET
		c.addReplyLongLong(created)
	} else { // HMSET
		c.addReply(shared.ok)
	}
}

func hgetCommand(c *client) {
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		o, ok := c.getHashOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), nil, shared.wrongtypeerr)
		if ok {
			c.addHashFieldToReply(o, c.argv[2])
		}
	} else { // expired
		c.addReply(shared.nullbulk)
	}
}

func hmgetCommand(c *client) {
	var o interface{}
	var ok bool
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		o, ok = c.getHashOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), nil, shared.wrongtypeerr)
		if !ok {
			return
		}
	}
	c.addReplyMultiBulkLen(c.argc - 2)
	for i := 2; i < c.argc; i++ {
		c.addHashFieldToReply(o, c.argv[i])
	}
}

func hdelCommand(c *client) {
	deleted := 0
	removed := false
	c.db.lockKeyWrite(c.tx, c.argv[1])
	o, ok := c.getHashOrReply(c.db.lookupKeyWrite(c.tx, c.argv[1]), shared.czero, shared.wrongtypeerr)
	if ok && o != nil {
		for i := 2; i < c.argc; i++ {
			if hashTypeDelete(c, o, c.argv[i]) {
				deleted++
				if hashTypeLength(o) == 0 {
					c.db.delete(c.tx, c.argv[1])
					removed = true
					break
				}
			}
		}
		if deleted > 0 {
			if removed {
				// key space event notifications
			}
		}
		c.addReplyLongLong(deleted)
	}
}

func hlenCommand(c *client) {
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		o, ok := c.getHashOrReply(c.db.lookupKeyWrite(c.tx, c.argv[1]), shared.czero, shared.wrongtypeerr)
		if ok && o != nil {
			c.addReplyLongLong(hashTypeLength(o))
		}
	} else { // expired
		c.addReply(shared.czero)
	}
}

func hstrlenCommand(c *client) {
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		o, ok := c.getHashOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), shared.czero, shared.wrongtypeerr)
		if ok && o != nil {
			c.addReplyLongLong(hashTypeGetValueLength(o, c.argv[2]))
		}
	} else { // expired
		c.addReply(shared.czero)
	}
}

func hkeysCommand(c *client) {
	genericHgetallCommand(c, true, false)
}

func hvalsCommand(c *client) {
	genericHgetallCommand(c, false, true)
}

func hgetallCommand(c *client) {
	genericHgetallCommand(c, true, true)
}

func genericHgetallCommand(c *client, getK, getV bool) {
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		o, ok := c.getHashOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), shared.emptymultibulk, shared.wrongtypeerr)
		if ok && o != nil {
			multiplier := 0
			if getK {
				multiplier++
			}
			if getV {
				multiplier++
			}
			length := hashTypeLength(o) * multiplier
			c.addReplyMultiBulkLen(length)

			hi := hashTypeInitIterator(o)
			for hi.hashTypeNext() {
				c.addHashIteratorCursorToReply(hi, getK, getV)
			}
		}
	} else { // expired
		c.addReply(shared.emptymultibulk)
	}
}

func hexistsCommand(c *client) {
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		o, ok := c.getHashOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), shared.czero, shared.wrongtypeerr)
		if ok && o != nil {
			if hashTypeExists(o, c.argv[2]) {
				c.addReply(shared.cone)
			} else {
				c.addReply(shared.czero)
			}
		}
	} else { // expired
		c.addReply(shared.czero)
	}
}

/*============== helper functions ====================*/

func hashTypeLookupWriteOrCreate(c *client, key []byte) interface{} {
	o, ok := c.getHashOrReply(c.db.lookupKeyWrite(c.tx, key), nil, shared.wrongtypeerr)
	if ok {
		if o == nil {
			o = NewDict(c.tx, 4, 4) // implicitly convert to interface
			c.db.setKey(c.tx, shadowCopyToPmem(key), o)
		}
	}
	return o
}

func hashTypeExists(o interface{}, field []byte) bool {
	switch d := o.(type) {
	case *dict:
		if hashTypeGetFromHashTable(d, field) != nil {
			return true
		} else {
			return false
		}
	default:
		panic("Unknown hash encoding")
	}
}

func hashTypeSet(c *client, o interface{}, field, value []byte) bool {
	var update bool
	switch d := o.(type) {
	case *dict:
		_, _, _, de := d.find(field)
		if de != nil {
			c.tx.Log(de)
			de.value = shadowCopyToPmemI(value)
			update = true
		} else {
			d.set(c.tx, shadowCopyToPmem(field), shadowCopyToPmemI(value))
			go hashTypeBgResize(c.db, c.argv[1])
		}
	default:
		panic("Unknown hash encoding")
	}
	return update
}

func hashTypeDelete(c *client, o interface{}, field []byte) bool {
	deleted := false
	switch d := o.(type) {
	case *dict:
		if d.delete(c.tx, field) != nil {
			deleted = true
			go hashTypeBgResize(c.db, c.argv[1])
		}
	default:
		panic("Unknown hash encoding")
	}
	return deleted
}

func hashTypeBgResize(db *redisDb, key []byte) {
	tx := transaction.NewUndo()
	rehash := true
	for rehash {
		// need to lock and get kv pair in every transaction
		tx.Begin()
		db.lockKeyWrite(tx, key)
		o := db.lookupKeyWrite(tx, key)
		if o == nil {
			break
		}
		var d *dict
		switch v := o.(type) {
		case *dict:
			d = v
		case *zset:
			d = v.dict
		default:
			rehash = false
		}
		if d != nil {
			if d.rehashIdx == -1 {
				_, _, size1 := d.resizeIfNeeded(tx)
				if size1 == 0 {
					rehash = false
				} else {
					//println("Rehash hash key", string(key), "to size", size1)
				}
			} else if d.rehashIdx == -2 {
				d.rehashSwap(tx)
				rehash = false
			} else {
				d.rehashStep(tx)
			}
		}
		tx.Commit()
	}
	transaction.Release(tx)
}

// need to check o != nil outside
func hashTypeLength(o interface{}) int {
	length := 0
	switch d := o.(type) {
	case *dict:
		length = d.size()
	default:
		panic("Unknown hash encoding")
	}
	return length
}

func hashTypeGetValueLength(o interface{}, field []byte) int {
	length := 0
	switch d := o.(type) {
	case *dict:
		v := hashTypeGetFromHashTable(d, field)
		length = len(v)
	default:
		panic("Unknown hash encoding")
	}
	return length
}

func hashTypeGetFromHashTable(d *dict, key []byte) []byte {
	_, _, _, de := d.find(key)
	if de == nil {
		return nil
	} else {
		switch v := de.value.(type) {
		case *[]byte:
			return *v
		default:
			panic("Unknown hash value encoding")
		}
	}
}

func hashTypeInitIterator(o interface{}) *hashTypeIterator {
	hi := new(hashTypeIterator)
	hi.subject = o
	switch d := o.(type) {
	case *dict:
		hi.di = d.getIterator()
	default:
		panic("Unknown hash value encoding")
	}
	return hi
}

func (hi *hashTypeIterator) hashTypeNext() bool {
	switch hi.subject.(type) {
	case *dict:
		hi.de = hi.di.next()
		if hi.de == nil {
			return false
		}
	default:
		panic("Unknown hash value encoding")
	}
	return true
}

func (hi *hashTypeIterator) hashTypeCurrentFromHashTable() ([]byte, []byte) {
	if hi.de == nil {
		return nil, nil
	}
	return hi.de.key, *hi.de.value.(*[]byte)
}

func (c *client) addHashIteratorCursorToReply(hi *hashTypeIterator, getK, getV bool) {
	switch hi.subject.(type) {
	case *dict:
		key, value := hi.hashTypeCurrentFromHashTable()
		if getK {
			c.addReplyBulk(key)
		}
		if getV {
			c.addReplyBulk(value)
		}
	default:
		panic("Unknown hash value encoding")
	}
}

func (c *client) addHashFieldToReply(o interface{}, field []byte) {
	if o == nil {
		c.addReply(shared.nullbulk)
		return
	}
	switch d := o.(type) {
	case *dict:
		value := hashTypeGetFromHashTable(d, field)
		if value == nil {
			c.addReply(shared.nullbulk)
		} else {
			c.addReplyBulk(value)
		}
	default:
		panic("Unknown hash encoding")
	}
}
