package redis

import (
	"pmem/transaction"
	"sort"
)

type (
	setslice []interface{}

	setTypeIterator struct {
		subject interface{}
		di      *dictIterator
		ii      int // intset iterator
	}
)

/*============== set commands ====================*/
func saddCommand(c *client) {
	c.db.lockKeyWrite(c.tx, c.argv[1])
	var added int64
	if set, ok := c.getSetOrReply(c.db.lookupKeyWrite(c.tx, c.argv[1]), nil); !ok {
		return
	} else {
		if set == nil {
			set = setTypeCreate(c.tx, c.argv[2])
			c.db.setKey(c.tx, shadowCopyToPmem(c.argv[1]), set)
		}
		for j := 2; j < c.argc; j++ {
			if setTypeAdd(c, c.argv[1], set, c.argv[j]) {
				added++
			}
		}
	}
	c.addReplyLongLong(added)
}

func sremCommand(c *client) {
	c.db.lockKeyWrite(c.tx, c.argv[1])
	var deleted int64
	set, ok := c.getSetOrReply(c.db.lookupKeyWrite(c.tx, c.argv[1]), shared.czero)
	if !ok || set == nil {
		return
	} else {
		for j := 2; j < c.argc; j++ {
			if setTypeRemove(c, c.argv[1], set, c.argv[j]) {
				deleted++
				if setTypeSize(set) == 0 {
					c.db.delete(c.tx, c.argv[1])
					break
				}
			}
		}
	}
	c.addReplyLongLong(deleted)
}

func smoveCommand(c *client) {
	c.db.lockKeysWrite(c.tx, c.argv[1:3], 1)
	var srcset, dstset interface{}
	ele := c.argv[3]
	ok := false
	/* If the source key does not exist return 0 */
	if srcset, ok = c.getSetOrReply(c.db.lookupKeyWrite(c.tx, c.argv[1]), shared.czero); !ok || srcset == nil {
		return
	}
	/* If the destination key is set and has the wrong type, return with an error. */
	if dstset, ok = c.getSetOrReply(c.db.lookupKeyWrite(c.tx, c.argv[2]), nil); !ok {
		return
	}

	/* If srcset and dstset are equal, SMOVE is a no-op */
	if dstset == srcset {
		if setTypeIsMember(srcset, ele) {
			c.addReply(shared.cone)
		} else {
			c.addReply(shared.czero)
		}
		return
	}

	/* If the element cannot be removed from the src set, return 0. */
	if !setTypeRemove(c, c.argv[1], srcset, ele) {
		c.addReply(shared.czero)
		return
	}

	/* Remove the src set from the database when empty */
	if setTypeSize(srcset) == 0 {
		c.db.delete(c.tx, c.argv[1])
	}

	/* Create the destination set when it doesn't exist */
	if dstset == nil {
		dstset = setTypeCreate(c.tx, ele)
		c.db.setKey(c.tx, shadowCopyToPmem(c.argv[2]), dstset)
	}

	/* An extra key has changed when ele was successfully added to dstset */
	if setTypeAdd(c, c.argv[2], dstset, ele) {
		// notify key space event
	}
	c.addReply(shared.cone)
}

func sinterCommand(c *client) {
	sinterGenericCommand(c, c.argv[1:], nil)
}

func sinterstoreCommand(c *client) {
	sinterGenericCommand(c, c.argv[2:], c.argv[1])
}

func sinterGenericCommand(c *client, setkeys [][]byte, dstkey []byte) {
	/* Lock all touched keys in main dict. */
	var keys [][]byte
	var alives []bool = nil
	if dstkey == nil { // read only commands
		keys = setkeys
		alives = c.db.lockKeysRead(c.tx, keys, 1)
	} else { // write commands.
		// TODO: write locks will be automatically aquired for all keys even we only need read locks for source keys.
		keys = append(setkeys, dstkey)
		c.db.lockKeysWrite(c.tx, keys, 1)
	}

	sets := make([]interface{}, len(setkeys))
	for i, key := range setkeys {
		if len(alives) > 0 && !alives[i] { // read expiired
			continue
		}
		if set, ok := c.getSetOrReply(c.db.lookupKeyWrite(c.tx, key), nil); !ok {
			return
		} else {
			if set == nil {
				if dstkey != nil {
					c.db.delete(c.tx, dstkey)
					c.addReply(shared.czero)
				} else {
					c.addReply(shared.emptymultibulk)
				}
				return
			} else {
				sets[i] = set
			}
		}
	}

	/* Sort sets from the smallest to largest, this will improve our
	 * algorithm's performance */
	sort.Sort(setslice(sets))

	/* The first thing we should output is the total number of elements...
	 * since this is a multi-bulk write, but at this stage we don't know
	 * the intersection set size, so we use a trick, append an empty object
	 * to the output list and save the pointer to later modify it with the
	 * right length */
	var dstset interface{}
	if dstkey == nil {
		c.addDeferredMultiBulkLength()
	} else {
		dstset = setTypeCreate(c.tx, nil)
	}

	/* Iterate all the elements of the first (smallest) set, and test
	 * the element against all the other sets, if at least one set does
	 * not include the element it is discarded */
	cardinality := 0
	si := setTypeInitIterator(sets[0])
	ele := setTypeNext(si)
	for ele != nil {
		j := 1
		for ; j < len(sets); j++ {
			if sets[j] == sets[0] {
				continue
			}
			if !setTypeIsMember(sets[j], ele) {
				break
			}
		}
		/* Only take action when all sets contain the member */
		if j == len(sets) {
			if dstkey == nil {
				e, _ := getString(ele)
				c.addReplyBulk(e)
				cardinality++
			} else {
				setTypeAdd(c, dstkey, dstset, ele)
			}
		}
		ele = setTypeNext(si)
	}

	if dstkey != nil {
		c.db.delete(c.tx, dstkey)
		if setTypeSize(dstkey) > 0 {
			c.db.setKey(c.tx, shadowCopyToPmem(dstkey), dstset)
			c.addReplyLongLong(int64(setTypeSize(dstkey)))
		} else {
			c.addReply(shared.czero)
		}
	} else {
		c.setDeferredMultiBulkLength(cardinality)
	}
}

func sismemberCommand(c *client) {
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		o, ok := c.getSetOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), shared.czero)
		if !ok || o == nil {
			return
		} else {
			if setTypeIsMember(o, c.argv[2]) {
				c.addReply(shared.cone)
			} else {
				c.addReply(shared.czero)
			}
		}
	} else { // expired
		c.addReply(shared.czero)
	}
}

func scardCommand(c *client) {
	if c.db.lockKeyRead(c.tx, c.argv[1]) {
		o, ok := c.getSetOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), shared.czero)
		if !ok || o == nil {
			return
		} else {
			c.addReplyLongLong(int64(setTypeSize(o)))
		}
	} else { // expired
		c.addReply(shared.czero)
	}
}

/*============== settype helper functions ====================*/
func setTypeCreate(tx transaction.TX, val interface{}) interface{} {
	// TODO: create intset or hash based on val.
	return NewDict(tx, 4, 4)
}

func setTypeAdd(c *client, skey []byte, subject interface{}, k interface{}) bool {
	switch s := subject.(type) {
	case *dict:
		key, _ := getString(k)
		_, _, _, de := s.find(key)
		if de == nil { // only add if not exist
			s.set(c.tx, shadowCopyToPmem(key), nil)
			go hashTypeBgResize(c.db, skey)
			return true
		} else {
			return false
		}
	default:
		panic("Unknown set encoding")
	}
}

func setTypeRemove(c *client, skey []byte, subject interface{}, k interface{}) bool {
	switch s := subject.(type) {
	case *dict:
		key, _ := getString(k)
		de := s.delete(c.tx, key)
		if de == nil {
			return false
		} else {
			go hashTypeBgResize(c.db, skey)
			return true
		}
	default:
		panic("Unknown set encoding")
	}
}

func setTypeIsMember(subject interface{}, k interface{}) bool {
	switch s := subject.(type) {
	case *dict:
		key, _ := getString(k)
		_, _, _, de := s.find(key)
		return de != nil
	default:
		panic("Unknown set encoding")
	}
}

func setTypeSize(subject interface{}) int {
	switch s := subject.(type) {
	case *dict:
		return s.size()
	default:
		panic("Unknown set encoding")
	}
}

func setTypeInitIterator(subject interface{}) *setTypeIterator {
	si := new(setTypeIterator)
	si.subject = subject
	switch set := subject.(type) {
	case *dict:
		si.di = set.getIterator()
	default:
		panic("Unknown set encoding")
	}
	return si
}

func setTypeNext(si *setTypeIterator) interface{} {
	switch si.subject.(type) {
	case *dict:
		de := si.di.next()
		if de == nil {
			return nil
		} else {
			return de.key
		}
	default:
		panic("Unknown set encoding")
	}
}

func (sets setslice) Len() int {
	return len(sets)
}

func (sets setslice) Swap(i, j int) {
	sets[i], sets[j] = sets[j], sets[i]
}

func (sets setslice) Less(i, j int) bool {
	return setTypeSize(sets[i]) < setTypeSize(sets[j])
}
