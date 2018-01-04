package redis

import (
	"bytes"
	"math/rand"
	"pmem/transaction"
	"strconv"
	"strings"
	"unsafe"
)

type (
	zset struct {
		dict *dict
		zsl  *zskiplist
	}

	zskiplist struct {
		header, tail *zskiplistNode
		length       uint
		level        int
	}

	zskiplistNode struct {
		ele      []byte
		score    float64
		backward *zskiplistNode
		level    []zskiplistLevel
	}

	zskiplistLevel struct {
		forward *zskiplistNode
		span    uint
	}

	zrangespec struct {
		min, max     float64
		minex, maxex bool
	}
)

const (
	/* Input flags. */
	ZADD_NONE = 0
	ZADD_INCR = (1 << 0)
	ZADD_NX   = (1 << 1)
	ZADD_XX   = (1 << 2)

	/* Output flags. */
	ZADD_NOP     = (1 << 3)
	ZADD_NAN     = (1 << 4)
	ZADD_ADDED   = (1 << 5)
	ZADD_UPDATED = (1 << 6)

	/* Flags only used by the ZADD command but not by zsetAdd() API: */
	ZADD_CH = (1 << 16)

	ZSKIPLIST_MAXLEVEL = 32
	ZSKIPLIST_P        = 0.25
)

func (zs *zset) swizzle(tx transaction.TX) {
	inPMem(unsafe.Pointer(zs))
	zs.zsl.swizzle(tx)
	zs.dict.swizzle(tx)
}

func (zsl *zskiplist) swizzle(tx transaction.TX) {
	inPMem(unsafe.Pointer(zsl))
	inPMem(unsafe.Pointer(zsl.header))
	inPMem(unsafe.Pointer(zsl.tail))
	length := uint(0)
	var b *zskiplistNode
	x := zsl.header.level[0].forward
	for x != nil {
		inPMem(unsafe.Pointer(x))
		inPMem(unsafe.Pointer(&(x.ele[0])))
		inPMem(unsafe.Pointer(x.backward))
		if x.backward != b {
			panic("skiplist backward does not match!")
		}
		for _, l := range x.level {
			inPMem(unsafe.Pointer(l.forward))
		}
		b = x
		x = x.level[0].forward
		length++
	}
	if length != zsl.length {
		print(length, zsl.length)
		panic("skiplist length does not match!")
	}
}

/*============== zset type commands ====================*/
func zaddCommand(c *client) {
	zaddGenericCommand(c, ZADD_NONE)
}

func zaddGenericCommand(c *client, flags int) {
	nanerr := []byte("resulting score is not a number (NaN)")

	/* Parse options. At the end 'scoreidx' is set to the argument position
	 * of the score of the first score-element pair. */
	scoreidx := 2
	for scoreidx < c.argc {
		opt := string(c.argv[scoreidx])
		if strings.EqualFold(opt, "nx") {
			flags |= ZADD_NX
		} else if strings.EqualFold(opt, "xx") {
			flags |= ZADD_XX
		} else if strings.EqualFold(opt, "ch") {
			flags |= ZADD_CH
		} else if strings.EqualFold(opt, "incr") {
			flags |= ZADD_INCR
		} else {
			break
		}
		scoreidx++
	}

	incr := (flags & ZADD_INCR) != 0
	nx := (flags & ZADD_NX) != 0
	xx := (flags & ZADD_XX) != 0
	ch := (flags & ZADD_CH) != 0

	/* After the options, we expect to have an even number of args, since
	 * we expect any number of score-element pairs. */
	elements := c.argc - scoreidx
	if elements%2 == 1 || elements == 0 {
		c.addReply(shared.syntaxerr)
		return
	}
	elements /= 2

	/* Check for incompatible options. */
	if nx && xx {
		c.addReplyError([]byte("XX and NX options at the same time are not compatible"))
		return
	}

	if incr && elements > 1 {
		c.addReplyError([]byte("INCR option supports a single increment-element pair"))
		return
	}

	var zobj interface{}
	var err error
	var score float64
	added := 0
	updated := 0
	processed := 0

	/* Start parsing all the scores, we need to emit any syntax error
	 * before executing additions to the sorted set, as the command should
	 * either execute fully or nothing at all. */
	scores := make([]float64, elements)
	for j := 0; j < elements; j++ {
		scores[j], err = strconv.ParseFloat(string(c.argv[scoreidx+j*2]), 64)
		if err != nil {
			goto cleanup
		}
	}

	/* Lookup the key and create the sorted set if does not exist. */
	c.db.lockKeyWrite(c.tx, c.argv[1])
	zobj = c.db.lookupKeyWrite(c.tx, c.argv[1])
	if zobj == nil {
		if xx {
			goto reply_to_client /* No key + XX option: nothing to do. */
		}
		/* TODO: implement ziplist encoding. */
		zobj = zsetCreate(c.tx)
		c.db.setKey(c.tx, shadowCopyToPmem(c.argv[1]), zobj)
	} else {
		switch zobj.(type) {
		case *zset:
			break
		default:
			c.addReplyError(shared.wrongtypeerr)
			goto cleanup
		}
	}

	for j := 0; j < elements; j++ {
		score = scores[j]

		ele := c.argv[scoreidx+1+j*2]
		retval, newscore, retflags := zsetAdd(c, zobj, score, ele, flags)
		if retval == 0 {
			c.addReplyError(nanerr)
			goto cleanup
		}
		if retflags&ZADD_ADDED != 0 {
			added++
		}
		if retflags&ZADD_UPDATED != 0 {
			updated++
		}
		if retflags&ZADD_NOP == 0 {
			processed++
		}
		score = newscore
	}
	// server.dirty += (added+updated)

reply_to_client:
	if incr { /* ZINCRBY or INCR option. */
		if processed > 0 {
			c.addReplyDouble(score)
		} else {
			c.addReply(shared.nullbulk)
		}
	} else { /* ZADD. */
		if ch {
			c.addReplyLongLong(added + updated)
		} else {
			c.addReplyLongLong(added)
		}
	}

cleanup:
	if added > 0 || updated > 0 {
		/* TODO: notify key space event */
	}
}

func zremCommand(c *client) {
	c.db.lockKeyWrite(c.tx, c.argv[1])
	zobj, ok := c.getZsetOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), shared.emptymultibulk, shared.wrongtypeerr)
	if !ok || zobj == nil {
		return
	}

	deleted := 0
	keyremoved := false
	for j := 2; j < c.argc; j++ {
		if zsetDel(c, zobj, c.argv[j]) {
			deleted++
		}
		if zsetLength(zobj) == 0 {
			c.db.delete(c.tx, c.argv[1])
			keyremoved = true
			break
		}
	}

	if deleted > 0 {
		// notify key space event
		if keyremoved {

		}
	}
	c.addReplyLongLong(deleted)
}

func zrangebyscoreCommand(c *client) {
	genericZrangebyscoreCommand(c, false)
}

func zrevrangebyscoreCommand(c *client) {
	genericZrangebyscoreCommand(c, true)
}

func genericZrangebyscoreCommand(c *client, reverse bool) {
	var minidx, maxidx int
	if reverse {
		maxidx = 2
		minidx = 3
	} else {
		minidx = 2
		maxidx = 3
	}

	zrange, err := zslParseRange(c.argv[minidx], c.argv[maxidx])
	if err != nil {
		c.addReplyError([]byte("min or max is not a float"))
		return
	}

	/* Parse optional extra arguments. Note that ZCOUNT will exactly have
	 * 4 arguments, so we'll never enter the following code path. */
	var withscores bool
	var limit int64 = -1
	var offset int64 = 0
	if c.argc > 4 {
		remaining := c.argc - 4
		pos := 4

		for remaining > 0 {
			if remaining >= 1 && bytes.Compare(c.argv[pos], []byte("WITHSCORES")) == 0 {
				pos++
				remaining--
				withscores = true
			} else if remaining >= 3 && bytes.Compare(c.argv[pos], []byte("LIMIT")) == 0 {
				var err error
				offset, err = strconv.ParseInt(string(c.argv[pos+1]), 19, 64)
				if err != nil {
					c.addReplyError([]byte("value is not an integer or out of range"))
					return
				}
				limit, err = strconv.ParseInt(string(c.argv[pos+2]), 19, 64)
				if err != nil {
					c.addReplyError([]byte("value is not an integer or out of range"))
					return
				}
				pos += 3
				remaining -= 3
			} else {
				c.addReply(shared.syntaxerr)
				return
			}
		}
	}

	/* Ok, lookup the key and get the range */
	if !c.db.lockKeyRead(c.tx, c.argv[1]) { // expired
		c.addReply(shared.emptymultibulk)
	}

	zobj, ok := c.getZsetOrReply(c.db.lookupKeyRead(c.tx, c.argv[1]), shared.emptymultibulk, shared.wrongtypeerr)
	if !ok || zobj == nil {
		return
	}

	rangelen := 0
	switch z := zobj.(type) {
	case *zset:
		zsl := z.zsl
		var ln *zskiplistNode
		if reverse {
			ln = zsl.lastInRange(&zrange)
		} else {
			ln = zsl.firstInRange(&zrange)
		}

		if ln == nil {
			c.addReply(shared.emptymultibulk)
			return
		}

		/* We don't know in advance how many matching elements there are in the
		 * list, so we push this object that will represent the multi-bulk
		 * length in the output buffer, and will "fix" it later */
		c.addDeferredMultiBulkLength()

		/* If there is an offset, just traverse the number of elements without
		 * checking the score because that is done in the next loop. */
		for ln != nil && offset != 0 {
			if reverse {
				ln = ln.backward
			} else {
				ln = ln.level[0].forward
			}
			offset--
		}

		for ln != nil && limit != 0 {
			/* Abort when the node is no longer in range. */
			if reverse {
				if !zslValueGteMin(ln.score, &zrange) {
					break
				}
			} else {
				if !zslValueLteMax(ln.score, &zrange) {
					break
				}
			}
			rangelen++
			c.addReplyBulk(ln.ele)

			if withscores {
				c.addReplyDouble(ln.score)
			}

			/* Move to next node */
			if reverse {
				ln = ln.backward
			} else {
				ln = ln.level[0].forward
			}
			limit--
		}
	default:
		panic("Unknown sorted set encoding")
	}

	if withscores {
		rangelen *= 2
	}
	c.setDeferredMultiBulkLength(rangelen)
}

/*============== helper functions ====================*/
func zsetCreate(tx transaction.TX) *zset {
	zs := pnew(zset)
	tx.Log(zs)
	zs.dict = NewDict(tx, 4, 4)
	zs.zsl = zslCreate(tx)
	return zs
}

func zsetAdd(c *client, zobj interface{}, score float64, ele []byte, flags int) (int, float64, int) {
	incr := (flags & ZADD_INCR) != 0
	nx := (flags & ZADD_NX) != 0
	xx := (flags & ZADD_XX) != 0
	flags = 0

	switch zs := zobj.(type) {
	case *zset:
		_, _, _, de := zs.dict.find(ele)
		if de != nil {
			/* NX? Return, same element already exists. */
			if nx {
				return 1, 0, flags | ZADD_NOP
			}
			curscore := de.value.(float64)

			/* NX? Return, same element already exists. */
			if incr {
				score += curscore
			}

			/* Remove and re-insert when score changes. */
			if score != curscore {
				node := zs.zsl.delete(c.tx, curscore, ele)
				zs.zsl.insert(c.tx, score, node.ele)
				c.tx.Log(de)
				de.value = score // Change dictionary value to directly store score value instead of pointer to score in zsl.
				flags |= ZADD_UPDATED
			}
			return 1, score, flags
		} else if !xx {
			ele = shadowCopyToPmem(ele)
			zs.zsl.insert(c.tx, score, ele)
			zs.dict.set(c.tx, ele, score)
			go hashTypeBgResize(c.db, c.argv[1])
			return 1, score, flags | ZADD_ADDED
		} else {
			return 1, 0, flags | ZADD_NOP
		}
	default:
		panic("Unknown sorted set encoding")
	}

}

func zsetDel(c *client, zobj interface{}, ele []byte) bool {
	switch zs := zobj.(type) {
	case *zset:
		de := zs.dict.delete(c.tx, ele)
		if de != nil {
			score := de.value.(float64)
			retvalue := zs.zsl.delete(c.tx, score, ele)
			if retvalue == nil {
				panic("Zset dict and skiplist does not match!")
			}
			// TODO: resize
			go hashTypeBgResize(c.db, c.argv[1])
			return true
		}
	default:
		panic("Unknown sorted set encoding")
	}
	return false
}

func zsetLength(zobj interface{}) uint {
	switch zs := zobj.(type) {
	case *zset:
		return zs.zsl.length
	default:
		panic("Unknown sorted set encoding")
	}
}

func zslCreate(tx transaction.TX) *zskiplist {
	zsl := pnew(zskiplist)
	tx.Log(zsl)
	zsl.level = 1
	zsl.length = 0
	zsl.header = zslCreateNode(tx, ZSKIPLIST_MAXLEVEL, 0, nil)
	return zsl
}

func zslCreateNode(tx transaction.TX, level int, score float64, ele []byte) *zskiplistNode {
	zn := pnew(zskiplistNode)
	tx.Log(zn)
	zn.score = score
	zn.ele = ele
	zn.level = pmake([]zskiplistLevel, level)
	return zn
}

func zslRandomLevel() int {
	level := 1
	t := ZSKIPLIST_P * 0xFFFF
	ti := uint32(t)
	for rand.Uint32()&0xFFFF < ti {
		level++
	}
	if level < ZSKIPLIST_MAXLEVEL {
		return level
	} else {
		return ZSKIPLIST_MAXLEVEL
	}
}

func zslParseRange(min, max []byte) (spec zrangespec, err error) {
	if min[0] == '(' {
		spec.min, err = strconv.ParseFloat(string(min[1:]), 64)
		if err != nil {
			return
		} else {
			spec.minex = true
		}
	} else {
		spec.min, err = strconv.ParseFloat(string(min), 64)
		if err != nil {
			return
		}
	}

	if max[0] == '(' {
		spec.max, err = strconv.ParseFloat(string(max[1:]), 64)
		if err != nil {
			return
		} else {
			spec.maxex = true
		}
	} else {
		spec.max, err = strconv.ParseFloat(string(max), 64)
		if err != nil {
			return
		}
	}
	return
}

func zslValueGteMin(value float64, spec *zrangespec) bool {
	if spec.minex {
		return value > spec.min
	} else {
		return value >= spec.min
	}
}

func zslValueLteMax(value float64, spec *zrangespec) bool {
	if spec.maxex {
		return value < spec.max
	} else {
		return value <= spec.max
	}
}

func (zsl *zskiplist) insert(tx transaction.TX, score float64, ele []byte) *zskiplistNode {
	update := make([]*zskiplistNode, ZSKIPLIST_MAXLEVEL)
	rank := make([]uint, ZSKIPLIST_MAXLEVEL)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		/* store rank that is crossed to reach the insert position */
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					bytes.Compare(x.level[i].forward.ele, ele) < 0)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}
	/* we assume the element is not already inside, since we allow duplicated
	 * scores, reinserting the same element should never happen since the
	 * caller of zslInsert() should test in the hash table if the element is
	 * already inside or not. */
	tx.Log(zsl)
	level := zslRandomLevel()
	if level > zsl.level {
		tx.Log(zsl.header.level[zsl.level:])
		for i := zsl.level; i < level; i++ {
			rank[i] = 0
			update[i] = zsl.header
			update[i].level[i].span = zsl.length
		}
		zsl.level = level
	}
	x = zslCreateNode(tx, level, score, ele)
	tx.Log(x)
	tx.Log(x.level)
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		tx.Log(&update[i].level[i])
		update[i].level[i].forward = x

		/* update span covered by update[i] as x is inserted here */
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	/* increment span for untouched levels */
	for i := level; i < zsl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == zsl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		tx.Log(&x.level[0].forward.backward)
		x.level[0].forward.backward = x
	} else {
		zsl.tail = x
	}
	zsl.length++
	return x
}

func (zsl *zskiplist) delete(tx transaction.TX, score float64, ele []byte) *zskiplistNode {
	update := make([]*zskiplistNode, ZSKIPLIST_MAXLEVEL)
	x := zsl.header

	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					bytes.Compare(x.level[i].forward.ele, ele) < 0)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	if x != nil && score == x.score && bytes.Compare(x.ele, ele) == 0 {
		zsl.deleteNode(tx, x, update)
		return x
	}
	return nil
}

func (zsl *zskiplist) deleteNode(tx transaction.TX, x *zskiplistNode, update []*zskiplistNode) {
	for i := 0; i < zsl.level; i++ {
		tx.Log(&update[i].level[i])
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span -= 1
		}
	}

	tx.Log(zsl)
	if x.level[0].forward != nil {
		tx.Log(&x.level[0].forward.backward)
		x.level[0].forward.backward = x.backward
	} else {
		zsl.tail = x.backward
	}

	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

func (zsl *zskiplist) firstInRange(zrange *zrangespec) *zskiplistNode {
	if !zsl.isInRange(zrange) {
		return nil
	}

	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			!zslValueGteMin(x.level[i].forward.score, zrange) {
			x = x.level[i].forward
		}
	}

	x = x.level[0].forward
	if !zslValueLteMax(x.score, zrange) {
		return nil
	}
	return x
}

func (zsl *zskiplist) lastInRange(zrange *zrangespec) *zskiplistNode {
	if !zsl.isInRange(zrange) {
		return nil
	}

	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			zslValueLteMax(x.level[i].forward.score, zrange) {
			x = x.level[i].forward
		}
	}

	if !zslValueGteMin(x.score, zrange) {
		return nil
	}
	return x
}

func (zsl *zskiplist) isInRange(zrange *zrangespec) bool {
	if zrange.min > zrange.max || (zrange.min == zrange.max && (zrange.minex || zrange.maxex)) {
		return false
	}
	x := zsl.tail
	if x == nil || !zslValueGteMin(x.score, zrange) {
		return false
	}
	x = zsl.header.level[0].forward
	if x == nil || !zslValueLteMax(x.score, zrange) {
		return false
	}
	return true
}
