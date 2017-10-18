package redis

import (
	"pmem/transaction"
	"unsafe"
	"hash"
	"hash/fnv"
	"sync"
	"fmt"
	"bytes"
)

const (
	DictInitSize = 4
	Ratio = 4
	Shards = 100
)

var (
	fnvHash hash.Hash32 = fnv.New32a()
)

type (
	dict struct {
		tab [2]table
		rehashIdx int
		lock *sync.RWMutex			
		bucketlock [2][]sync.RWMutex	// finegrained bucket locks
	}

	table struct {
		bucket []*entry
		used int
		mask int
		lock *sync.RWMutex		 // lock for used
	}

	entry struct {
		key []byte
		value []byte
		next *entry
	}
)

func NewDict(undoTx transaction.TX) *dict {
	// should be replaced with pNew(dict)
	d := new(dict)

	undoTx.Begin()
	undoTx.Log(d)
	d.resetTable(0, DictInitSize)
	d.resetTable(1, 0)
	d.rehashIdx = -1
	d.lock = new(sync.RWMutex)
	d.bucketlock[0] = make([]sync.RWMutex, Shards)
	d.bucketlock[1] = make([]sync.RWMutex, Shards)
	undoTx.Commit()
	return d
}

func (d *dict) resetTable(i int, s int) {
	if s == 0 {
		d.tab[i].bucket = nil
		d.tab[i].mask = 0
	} else {
		// should be replaced with pMake
		d.tab[i].bucket = make([]*entry, s)
		d.tab[i].mask = s - 1
	}
	d.tab[i].used = 0
	d.tab[i].lock = new(sync.RWMutex)
}

func newLocks(n int) (l []*sync.RWMutex) {
	l = make([]*sync.RWMutex,n)
	for i, _ := range(l) {
		l[i] = new(sync.RWMutex)
	}
	return l
}

func (d *dict) hashKey(key []byte) int { // may implement fast hashing in asm
	fnvHash.Write(key)
	h := int(fnvHash.Sum32())
	fnvHash.Reset()
	return h
}

func (d *dict) findKey(undoTx transaction.TX, key []byte, readOnly bool) (int, int, *entry, *entry) {
	h := d.hashKey(key)
	var (
		t, maxt, i int
		pre, curr *entry
	)
	if d.rehashIdx >= 0 {
		maxt = 1
	} else {
		maxt = 0
	}
	for t = 0; t <= maxt; t++ {
		i = h & d.tab[t].mask
		if readOnly {
			undoTx.RLock(&d.bucketlock[t][i % Shards])
			//undoTx.RLock(d.tab[t].lock)
		} else {
			undoTx.WLock(&d.bucketlock[t][i % Shards])
			//undoTx.WLock(d.tab[t].lock)
		}
		curr = d.tab[t].bucket[i]
		for curr != nil {
			if bytes.Compare(curr.key, key) == 0 {
				return t, i, pre, curr
			}
			pre = curr
			curr = curr.next
		}
	}
	return maxt, i, pre, curr
}

func (d *dict) Used(undoTx transaction.TX) int {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.RLock(d.lock)
	undoTx.RLock(d.tab[0].lock)
	undoTx.RLock(d.tab[1].lock)
	return d.tab[0].used + d.tab[1].used
}

func (d *dict) Set(undoTx transaction.TX, key, value []byte) {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.RLock(d.lock)

	t, i, _, e := d.findKey(undoTx, key, false)

	// copy volatile value into pmem heap (need pmake and a helper function for copy)
	v := make([]byte, len(value))//(*[1<<30]byte)(heap.Alloc(undoTx, len(value)))[:len(value):len(value)]
	copy(v, value)
	transaction.Persist(unsafe.Pointer(&v[0]), len(v) * int(unsafe.Sizeof(v[0]))) // shadow update

	if e != nil { // note that gc cannot recycle e.value before commit.
		undoTx.Log(&e.value)
		e.value = v
	} else {
		// copy volatile value into pmem heap (need pmake and a helper function for this copy)
		k := make([]byte, len(key))//(*[1<<30]byte)(heap.Alloc(undoTx, len(key)))[:len(key):len(key)]
		copy(k, key)
		transaction.Persist(unsafe.Pointer(&k[0]), len(k) * int(unsafe.Sizeof(k[0]))) // shadow update

		// should be replaced with pNew
		e2 := new(entry)//(*entry)(heap.Alloc(undoTx, int(unsafe.Sizeof(*e))))
		e2.key = k
		e2.value = v
		e2.next = d.tab[t].bucket[i]
		transaction.Persist(unsafe.Pointer(e2), int(unsafe.Sizeof(*e2))) // shadow update
		undoTx.Log(d.tab[t].bucket[i:i+1])
		d.tab[t].bucket[i] = e2
		undoTx.WLock(d.tab[t].lock)
		undoTx.Log(d.tab[t].used)
		d.tab[t].used++
	}
}

func (d *dict) Get(undoTx transaction.TX, key []byte) []byte {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.RLock(d.lock)

	_, _, _, e := d.findKey(undoTx, key, true)
	if e != nil {
		return e.value
	}
	return []byte{}
}

func (d *dict) Del(undoTx transaction.TX, key []byte) bool {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.RLock(d.lock)

	t, i, p, e := d.findKey(undoTx, key, false)
	deleted := false
	if e != nil { // note that gc cannot recycle e before commit.
		// update bucket (already locked when find key)
		if p != nil {
			undoTx.Log(p)
			p.next = e.next
		} else {
			undoTx.Log(d.tab[t].bucket[i:i+1])
			d.tab[t].bucket[i] = e.next
		}

		undoTx.WLock(d.tab[t].lock)
		undoTx.Log(d.tab[t].used)
		d.tab[t].used--
		deleted = true
	}
	return deleted
}

func (d *dict) Rehash(undoTx transaction.TX, n int) bool {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.WLock(d.lock)

	if d.rehashIdx < 0 {
		return false
	}

	maxvisit := n*10

	undoTx.Log(d)
	for n > 0 && d.tab[0].used > 0 {
		if maxvisit--; maxvisit == 0 {
			break
		}

		e := d.tab[0].bucket[d.rehashIdx]
		if e != nil {
			undoTx.Log(d.tab[0].bucket[d.rehashIdx:d.rehashIdx+1])
			for n > 0 && e != nil {
				next := e.next
				i := d.hashKey(e.key) & d.tab[1].mask
				undoTx.Log(e)
				e.next = d.tab[1].bucket[i]
				undoTx.Log(d.tab[1].bucket[i:i+1])
				d.tab[1].bucket[i] = e
				d.tab[0].used--
				d.tab[1].used++
				e = next
				n--
			}
			d.tab[0].bucket[d.rehashIdx] = e 
		}
		if e == nil {
			d.rehashIdx++
		}
	}

	if d.tab[0].used == 0 {
		d.tab[0] = d.tab[1]
		d.resetTable(1, 0)
		d.rehashIdx = -1
		fmt.Println("Rehash finished!")
	}

	return d.rehashIdx >= 0
}

func (d *dict) nextPower(s int) int {
	i := DictInitSize
	for i < s {
		i *= 2
	}
	return i
}

func (d *dict) resize(undoTx transaction.TX, s int) {
	s = d.nextPower(s) 

	undoTx.WLock(d.lock)
	undoTx.Log(d)
	d.resetTable(1, s)
	d.rehashIdx = 0
	fmt.Println("Dict used", d.tab[0].used, "Resized to", s)
}

func (d *dict) ResizeIfNeeded(undoTx transaction.TX) {
	undoTx.Begin()
	defer undoTx.Commit()

	/*	Note it is safe to unlock d.lock before commit time.
		For d.rehashIdx & tab & size, no other threads beside current one will ever update it.
		For used, it is OK to read stale data here. */
	d.lock.RLock()
	rehashIdx := d.rehashIdx
	size := len(d.tab[0].bucket)
	used := d.tab[0].used
	d.lock.RUnlock()

	if rehashIdx < 0 {
		if used > size {
			d.resize(undoTx, used * 2)
			return
		} 
		if size > DictInitSize && used < size / Ratio {
			d.resize(undoTx, used)
			return
		}
	}
}