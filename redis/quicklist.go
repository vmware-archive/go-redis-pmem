package redis

import (
	"fmt"

	"github.com/vmware/go-pmem-transaction/transaction"
)

type (
	quicklist struct {
		head, tail     *quicklistNode
		count, length  int
		fill, compress int
	}

	quicklistNode struct {
		prev, next *quicklistNode
		zl         *ziplist
	}

	quicklistIter struct {
		ql         *quicklist
		current    *quicklistNode
		offset, zi int
		startHead  bool
	}

	quicklistEntry struct {
		ql         *quicklist
		node       *quicklistNode
		value      interface{}
		offset, zi int
	}
)

var (
	optimization_level [5]int = [...]int{4096, 8192, 16384, 32768, 65536}
)

func quicklistCreate(tx transaction.TX) *quicklist {
	ql := pnew(quicklist)
	tx.Log(ql)
	ql.fill = -2
	return ql
}

func quicklistNew(tx transaction.TX, fill, compress int) *quicklist {
	ql := quicklistCreate(tx)
	ql.SetOptions(tx, fill, compress)
	return ql
}

func quicklistCreateFromZiplist(tx transaction.TX, fill, compress int, zl *ziplist) *quicklist {
	ql := quicklistNew(tx, fill, compress)
	ql.AppendValuesFromZiplist(tx, zl)
	return ql
}

func quicklistCreateNode(tx transaction.TX) *quicklistNode {
	node := pnew(quicklistNode)
	tx.Log(node)
	node.zl = ziplistNew(tx)
	return node
}

func (ql *quicklist) SetOptions(tx transaction.TX, fill, compress int) {
	ql.SetFill(tx, fill)
	ql.SetCompressDepth(tx, compress)
}

func (ql *quicklist) SetFill(tx transaction.TX, fill int) {
	tx.Log(&ql.fill)
	if fill > 1<<15 {
		fill = 1 << 15
	} else if fill < -5 {
		fill = -5
	}
	ql.fill = fill
}

func (ql *quicklist) SetCompressDepth(tx transaction.TX, compress int) {
	// TODO
}

func (ql *quicklist) Compress(tx transaction.TX, node *quicklistNode) {
	// TODO
}

func (ql *quicklist) AppendValuesFromZiplist(tx transaction.TX, zl *ziplist) {
	pos := 0
	val := zl.Get(pos)
	for val != nil {
		ql.PushTail(tx, val)
		pos = zl.Next(pos)
		val = zl.Get(pos)
	}
}

func (ql *quicklist) Count() int {
	return ql.count
}

func (qe *quicklistEntry) Compare(val interface{}) bool {
	return qe.node.zl.Compare(qe.zi, val)
}

func (ql *quicklist) GetIterator(startHead bool) *quicklistIter {
	iter := new(quicklistIter)
	if startHead {
		iter.current = ql.head
		iter.offset = 0
	} else {
		iter.current = ql.tail
		iter.offset = -1
	}
	iter.startHead = startHead
	iter.ql = ql
	iter.zi = -1

	return iter
}

func (ql *quicklist) GetIteratorAtIdx(startHead bool, idx int) *quicklistIter {
	var entry quicklistEntry
	if ql.Index(idx, &entry) {
		base := ql.GetIterator(startHead)
		base.current = entry.node
		base.offset = entry.offset
		return base
	} else {
		return nil
	}
}

func (iter *quicklistIter) Next(entry *quicklistEntry) bool {
	if iter == nil {
		return false
	}
	entry.ql = iter.ql
	entry.node = iter.current
	if iter.current == nil {
		return false
	}

	if iter.zi < 0 {
		iter.zi = iter.current.zl.Index(iter.offset)
	} else {
		if iter.startHead {
			iter.zi = iter.current.zl.Next(iter.zi)
			iter.offset++
		} else {
			iter.zi = iter.current.zl.Prev(iter.zi)
			iter.offset--
		}
	}

	entry.zi = iter.zi
	entry.offset = iter.offset

	if iter.zi >= 0 {
		entry.value = entry.node.zl.Get(entry.zi)
		return true
	} else {
		if iter.startHead {
			iter.current = iter.current.next
			iter.offset = 0
		} else {
			iter.current = iter.current.prev
			iter.offset = -1
		}
		return iter.Next(entry)
	}
}

func (iter *quicklistIter) DelEntry(tx transaction.TX, entry *quicklistEntry) {
	prev := entry.node.prev
	next := entry.node.next
	deleteNode := entry.ql.delIndex(tx, entry.node, entry.zi)
	iter.zi = -1
	if deleteNode {
		if iter.startHead {
			iter.current = next
			iter.offset = 0
		} else {
			iter.current = prev
			iter.offset = -1
		}
	}
}

func (ql *quicklist) Index(idx int, entry *quicklistEntry) bool {
	n := new(quicklistNode)
	forward := (idx >= 0)
	var index, accum int
	if forward {
		index = idx
		n = ql.head
	} else {
		index = -idx - 1
		n = ql.tail
	}

	for n != nil {
		if accum+int(n.zl.entries) > index {
			break
		} else {
			accum += int(n.zl.entries)
			if forward {
				n = n.next
			} else {
				n = n.prev
			}
		}
	}

	if n == nil {
		return false
	}

	entry.node = n
	if forward {
		entry.offset = index - accum
	} else {
		entry.offset = -index - 1 + accum
	}

	entry.zi = n.zl.Index(entry.offset)
	entry.value = n.zl.Get(entry.zi)
	return true
}

func (ql *quicklist) Push(tx transaction.TX, val interface{}, head bool) {
	if head {
		ql.PushHead(tx, val)
	} else {
		ql.PushTail(tx, val)
	}
}

func (ql *quicklist) PushHead(tx transaction.TX, val interface{}) bool {
	tx.Log(ql)
	origHead := ql.head
	if origHead.allowInsert(ql.fill, val) {
		origHead.zl.Push(tx, val, true)
	} else {
		node := quicklistCreateNode(tx)
		node.zl.Push(tx, val, true)
		ql.insertNodeBefore(tx, ql.head, node)
	}
	ql.count++
	return origHead != ql.head
}

func (ql *quicklist) PushTail(tx transaction.TX, val interface{}) bool {
	tx.Log(ql)
	origTail := ql.tail
	if origTail.allowInsert(ql.fill, val) {
		origTail.zl.Push(tx, val, false)
	} else {
		node := quicklistCreateNode(tx)
		node.zl.Push(tx, val, false)
		ql.insertNodeAfter(tx, ql.tail, node)
	}
	ql.count++
	return origTail != ql.tail
}

func (ql *quicklist) InsertBefore(tx transaction.TX, entry *quicklistEntry, val interface{}) {
	ql.insert(tx, entry, val, false)
}

func (ql *quicklist) InsertAfter(tx transaction.TX, entry *quicklistEntry, val interface{}) {
	ql.insert(tx, entry, val, true)
}

func (ql *quicklist) Pop(tx transaction.TX, head bool) interface{} {
	tx.Log(ql)
	if ql.count == 0 {
		return nil
	}
	var node *quicklistNode
	var idx int
	if head && ql.head != nil {
		node = ql.head
		idx = 0
	} else if !head && ql.tail != nil {
		node = ql.tail
		idx = -1
	} else {
		return nil
	}

	pos := node.zl.Index(idx)
	val := node.zl.Get(pos)
	if val != nil {
		ql.delIndex(tx, node, pos)
	}
	return val
}

func (ql *quicklist) ReplaceAtIndex(tx transaction.TX, idx int, val interface{}) bool {
	var entry quicklistEntry
	if ql.Index(idx, &entry) {
		entry.node.zl.Delete(tx, entry.zi)
		entry.node.zl.insert(tx, entry.zi, val)
		return true
	} else {
		return false
	}
}

func (ql *quicklist) DelRange(tx transaction.TX, start, count int) int {
	if count <= 0 || start > ql.count || start < -ql.count {
		return 0
	}
	extent := count
	if start >= 0 && extent > ql.count-start {
		extent = ql.count - start
	} else if start < 0 && extent > -start {
		extent = -start
	}

	var entry quicklistEntry
	if !ql.Index(start, &entry) {
		return 0
	}
	node := entry.node
	toDel := extent
	tx.Log(ql)
	for extent > 0 {
		next := node.next
		del := 0
		deleteNode := false
		nodeCount := int(node.zl.entries)
		if entry.offset == 0 && extent >= nodeCount {
			deleteNode = true
			del = nodeCount
		} else if entry.offset >= 0 && extent >= nodeCount {
			del = nodeCount - entry.offset
		} else if entry.offset < 0 {
			del = -entry.offset
			if del > extent {
				del = extent
			}
		} else {
			del = extent
		}

		if deleteNode {
			ql.delNode(tx, node)
		} else {
			node.zl.DeleteRange(tx, entry.offset, uint(del))
			ql.count -= del
			if node.zl.Len() == 0 {
				ql.delNode(tx, node)
			}
		}
		extent -= del
		node = next
		entry.offset = 0
	}
	return toDel
}

func (ql *quicklist) insertNodeBefore(tx transaction.TX, oldNode, newNode *quicklistNode) {
	ql.insertNode(tx, oldNode, newNode, false)
}

func (ql *quicklist) insertNodeAfter(tx transaction.TX, oldNode, newNode *quicklistNode) {
	ql.insertNode(tx, oldNode, newNode, true)
}

/* ql should be logged outside this call. */
func (ql *quicklist) insertNode(tx transaction.TX, oldNode, newNode *quicklistNode, after bool) {
	tx.Log(newNode)
	if after {
		newNode.prev = oldNode
		if oldNode != nil {
			tx.Log(oldNode)
			newNode.next = oldNode.next
			if oldNode.next != nil {
				tx.Log(oldNode.next)
				oldNode.next.prev = newNode
			}
			oldNode.next = newNode
		}
		if ql.tail == oldNode {
			ql.tail = newNode
		}
	} else {
		newNode.next = oldNode
		if oldNode != nil {
			tx.Log(oldNode)
			newNode.prev = oldNode.prev
			if oldNode.prev != nil {
				tx.Log(oldNode.prev)
				oldNode.prev.next = newNode
			}
			oldNode.prev = newNode
		}
		if ql.head == oldNode {
			ql.head = newNode
		}
	}
	if ql.length == 0 {
		ql.head = newNode
		ql.tail = newNode
	}
	if oldNode != nil {
		ql.Compress(tx, oldNode)
	}
	ql.length++
}

func (ql *quicklist) insert(tx transaction.TX, entry *quicklistEntry, val interface{}, after bool) {
	tx.Log(ql)
	node := entry.node
	if node == nil {
		newNode := quicklistCreateNode(tx)
		newNode.zl.Push(tx, val, true)
		ql.insertNode(tx, nil, newNode, false)
		ql.count++
		return
	}
	full := false
	atTail := false
	atHead := false
	fullNext := false
	fullPrev := false
	if !node.allowInsert(ql.fill, val) {
		full = true
	}
	if after && entry.offset == int(node.zl.entries) {
		atTail = true
		if !node.next.allowInsert(ql.fill, val) {
			fullNext = true
		}
	}
	if !after && entry.offset == 0 {
		atHead = true
		if !node.prev.allowInsert(ql.fill, val) {
			fullPrev = true
		}
	}

	if !full && after {
		// insert/append to current node after entry
		next := node.zl.Next(entry.zi)
		if next == -1 {
			node.zl.Push(tx, val, false)
		} else {
			node.zl.insert(tx, next, val)
		}
	} else if !full && !after {
		// insert to current node before entry
		node.zl.insert(tx, entry.zi, val)
	} else if full && atTail && node.next != nil && !fullNext {
		// insert to head of next node
		newNode := node.next
		newNode.zl.Push(tx, val, true)
	} else if full && atHead && node.prev != nil && !fullPrev {
		// append to tail of prev node
		newNode := node.prev
		newNode.zl.Push(tx, val, false)
	} else if full && ((atTail && node.next != nil && fullNext) || (atHead && node.prev != nil && fullPrev)) {
		// create a new node
		newNode := quicklistCreateNode(tx)
		tx.Log(newNode)
		newNode.zl = ziplistNew(tx)
		newNode.zl.Push(tx, val, false)
		ql.insertNode(tx, node, newNode, after)
	} else if full {
		// need to split full node
		newNode := node.split(tx, entry.offset, after)
		if after {
			newNode.zl.Push(tx, val, true)
		} else {
			newNode.zl.Push(tx, val, false)
		}
		ql.insertNode(tx, node, newNode, after)
		ql.mergeNodes(tx, node)
	}
	ql.count++
}

func (node *quicklistNode) allowInsert(fill int, val interface{}) bool {
	if node == nil {
		return false
	}
	zlOverhead := 0
	size := 4 // approximate size for intger store
	s, ok := val.([]byte)
	if ok {
		size = len(s)
	}
	if size < 254 {
		zlOverhead = 1
	} else {
		zlOverhead = 5
	}

	if size < 64 {
		zlOverhead += 1
	} else if size < 16384 {
		zlOverhead += 2
	} else {
		zlOverhead += 5
	}

	newsize := node.zl.Len() + zlOverhead + size
	if nodeSizeMeetOptimizationRequirement(newsize, fill) {
		return true
	} else if newsize > 8192 {
		return false
	} else if int(node.zl.entries) < fill {
		return true
	} else {
		return false
	}
}

func (node *quicklistNode) split(tx transaction.TX, offset int, after bool) *quicklistNode {
	newNode := quicklistCreateNode(tx)
	/* copy the whole zl into new node */
	tx.Log(newNode)
	newNode.zl = node.zl.deepCopy(tx)

	/* remove dup entries in two nodes */
	if after {
		node.zl.DeleteRange(tx, offset+1, node.zl.entries)
		newNode.zl.DeleteRange(tx, 0, uint(offset)+1)
	} else {
		node.zl.DeleteRange(tx, 0, uint(offset))
		newNode.zl.DeleteRange(tx, offset, newNode.zl.entries)
	}
	return newNode
}

/* ql should be logged outside the call. */
func (ql *quicklist) mergeNodes(tx transaction.TX, center *quicklistNode) {
	fill := ql.fill
	var prev, prevPrev, next, nextNext, target *quicklistNode
	if center.prev != nil {
		prev = center.prev
		if center.prev.prev != nil {
			prevPrev = center.prev.prev
		}
	}

	if center.next != nil {
		next = center.next
		if center.next.next != nil {
			nextNext = center.next.next
		}
	}

	if ql.allowNodeMerge(prev, prevPrev, fill) {
		ql.ziplistMerge(tx, prevPrev, prev)
	}

	if ql.allowNodeMerge(next, nextNext, fill) {
		ql.ziplistMerge(tx, next, nextNext)
	}

	if ql.allowNodeMerge(center, center.prev, fill) {
		target = ql.ziplistMerge(tx, center.prev, center)
	} else {
		target = center
	}

	if ql.allowNodeMerge(target, target.next, fill) {
		ql.ziplistMerge(tx, target, target.next)
	}
}

func (ql *quicklist) allowNodeMerge(a, b *quicklistNode, fill int) bool {
	if a == nil || b == nil {
		return false
	}
	mergeSize := a.zl.Len() + b.zl.Len()
	if nodeSizeMeetOptimizationRequirement(mergeSize, fill) {
		return true
	} else if mergeSize > 8192 {
		return false
	} else if int(a.zl.entries+b.zl.entries) <= fill {
		return true
	} else {
		return false
	}
}

/* ql should be logged outside the call. */
func (ql *quicklist) ziplistMerge(tx transaction.TX, a, b *quicklistNode) *quicklistNode {
	a.zl.Merge(tx, b.zl)
	tx.Log(b.zl)
	b.zl.entries = 0
	ql.delNode(tx, b)
	return a
}

func nodeSizeMeetOptimizationRequirement(sz, fill int) bool {
	if fill >= 0 {
		return false
	}
	idx := -fill - 1
	if idx < len(optimization_level) {
		if sz <= optimization_level[idx] {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

/* ql should be logged outside the call. */
func (ql *quicklist) delIndex(tx transaction.TX, node *quicklistNode, pos int) bool {
	tx.Log(node)
	deleteNode := false
	node.zl.Delete(tx, pos)
	if node.zl.entries == 0 {
		ql.delNode(tx, node)
		deleteNode = true
	}
	ql.count--
	return deleteNode
}

/* ql should be logged outside this call. */
func (ql *quicklist) delNode(tx transaction.TX, node *quicklistNode) {
	if node.next != nil {
		tx.Log(node.next)
		node.next.prev = node.prev
	}
	if node.prev != nil {
		tx.Log(node.prev)
		node.prev.next = node.next
	}

	if node == ql.tail {
		ql.tail = node.prev
	}
	if node == ql.head {
		ql.head = node.next
	}

	ql.Compress(tx, nil)
	ql.count -= int(node.zl.entries)
	ql.length--
}

func (ql *quicklist) print() {
	iter := ql.GetIterator(true)
	fmt.Print("(", ql.count, " ", ql.length, ")")
	var entry quicklistEntry
	var prev *quicklistNode
	for iter.Next(&entry) {
		if entry.node != prev {
			if prev != nil {
				fmt.Print("] ")
			}
			prev = entry.node
			fmt.Print("[")
		}
		switch s := entry.value.(type) {
		case []byte:
			fmt.Print(entry.offset, " ", string(s), ", ")
		default:
			fmt.Print(entry.offset, " ", s, ", ")
		}
	}
	fmt.Print("]\n")
}

func (ql *quicklist) verify() bool {
	size := 0
	iter := ql.GetIterator(true)
	var entry quicklistEntry
	for iter.Next(&entry) {
		size++
	}
	if size != ql.count {
		println("ql len mismatch when iterate forward!")
		return false
	}
	size = 0
	iter = ql.GetIterator(false)
	for iter.Next(&entry) {
		size++
	}
	if size != ql.count {
		println("ql len mismatch when iterate backward!")
		return false
	}
	size = 0
	node := ql.head
	for node != nil {
		size += int(node.zl.entries)
		if !node.zl.verify() {
			return false
		}
		node = node.next
	}
	if size != ql.count {
		println("ql len mismatch with zl entries!")
		return false
	}
	return true
}
