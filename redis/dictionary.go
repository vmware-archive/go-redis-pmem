package redis

import (
	"pmem/transaction"
	"pmem/heap"
	"unsafe"
	"errors"
	"hash"
	"hash/fnv"
)

const (
	DictInitSize = 4
	Ratio = 4
)

var (
	fnvHash hash.Hash32 = fnv.New32a()
)

type (
	dict struct {
		tables [2][]*entry
		used [2]int
		mask [2]int
		rehashIdx int
	}

	entry struct {
		key []byte
		value []byte
		next *entry
	}
)

func New(undoTx transaction.TX) *dict {
	var (
		d *dict
		t []*entry
		e *entry
	)
	// should be replaced with pNew(dict)
	d = (*dict)(heap.Alloc(undoTx, int(unsafe.Sizeof(*d))))
	// should be replaced with pMake([]entry, DictInitSize)
	t = (*[DictInitSize]*entry)(heap.Alloc(undoTx, int(unsafe.Sizeof(e)) * DictInitSize))[:DictInitSize:DictInitSize]

	undoTx.Begin()
	undoTx.Log(d)
	d.tables[0] = t
	d.mask[0] = DictInitSize - 1
	d.rehashIdx = -1
	undoTx.Commit()
	return d
}

func (d *dict) hashKey(key string) int { // may implement fast hashing in asm
	fnvHash.Write([]byte(key))
	h := int(fnvHash.Sum32())
	fnvHash.Reset()
	return h
}

func (d *dict) findKey(undoTx transaction.TX, key string) (int, int, *entry, *entry) {
	d.rehashStep(undoTx)
	h := d.hashKey(key)
	var (
		t, maxt, i int
		pre, curr *entry
	)
	if d.rehashing() {
		maxt = 1
	} else {
		maxt = 0
	}
	for t = 0; t <= maxt; t++ {
		i = h & d.mask[t]
		curr = d.tables[t][i]
		for curr != nil {
			if string(curr.key) == key {
				return t, i, pre, curr
			}
			pre = curr
			curr = curr.next
		}
	}
	return maxt, i, pre, curr
}


func (d *dict) Set(undoTx transaction.TX, key, value string) {
	t, i, _, e := d.findKey(undoTx, key)

	// copy volatile value into pmem heap
	v := (*[1<<30]byte)(heap.Alloc(undoTx, len(value)))[:len(value):len(value)]
	copy(v, value)
	transaction.Persist(unsafe.Pointer(&v[0]), len(v)) // shadow update

	undoTx.Begin()
	if e != nil { // note that gc cannot recycle e.value before commit.
		undoTx.Log(&e.value)
		e.value = v
	} else {
		k := (*[1<<30]byte)(heap.Alloc(undoTx, len(key)))[:len(key):len(key)]
		copy(k, key)
		transaction.Persist(unsafe.Pointer(&k[0]), len(k)) // shadow update

		e2 := (*entry)(heap.Alloc(undoTx, int(unsafe.Sizeof(*e))))
		e2.key = k
		e2.value = v
		e2.next = d.tables[t][i]
		transaction.Persist(unsafe.Pointer(e2), int(unsafe.Sizeof(*e2))) // shadow update
		undoTx.Log(d.tables[t][i:i+1])
		d.tables[t][i] = e2
		undoTx.Log(d.used[t:])
		d.used[t]++
	}
	undoTx.Commit()
	d.expandIfNeeded(undoTx)
}

func (d *dict) Get(undoTx transaction.TX, key string) string {
	_, _, _, e := d.findKey(undoTx, key)
	if e != nil {
		return string(e.value)
	}
	return ""
}

func (d *dict) Del(undoTx transaction.TX, key string) bool {
	t, i, p, e := d.findKey(undoTx, key)
	deleted := false
	undoTx.Begin()
	if e != nil { // note that gc cannot recycle e before commit.
		if p != nil {
			undoTx.Log(p)
			p.next = e.next
		} else {
			undoTx.Log(d.tables[t][i:i+1])
			d.tables[t][i] = e.next
		}
		undoTx.Log(d.used[t:])
		d.used[t]--
		deleted = true
	}
	undoTx.Commit()
	d.shrinkIfNeeded(undoTx)
	return deleted
}

func (d *dict) rehashing() bool {
	return d.rehashIdx >= 0
}

func (d *dict) rehash(undoTx transaction.TX, n int) bool {
	visit := n*10
	if !d.rehashing() {
		return false
	}

	undoTx.Begin()
	undoTx.Log(d)
	for n > 0 && d.used[0] > 0 {
		e := d.tables[0][d.rehashIdx]
		for e == nil {
			visit--
			if visit == 0 {
				return true
			}
			d.rehashIdx++
			e = d.tables[0][d.rehashIdx]
		}
		
		for e != nil {			
			next := e.next
			i := d.hashKey(string(e.key)) & d.mask[1]
			undoTx.Log(e)
			e.next = d.tables[1][i]
			undoTx.Log(d.tables[1][i:i+1])
			d.tables[1][i] = e
			d.used[0]--
			d.used[1]++
			e = next
		}
		undoTx.Log(d.tables[0][d.rehashIdx:d.rehashIdx+1])
		d.tables[0][d.rehashIdx] = nil
		d.rehashIdx++
		n--
	}

	if d.used[0] == 0 {
		d.tables[0] = d.tables[1]
		d.tables[1] = nil
		d.used[0] = d.used[1]
		d.mask[0] = d.mask[1]
		d.used[1] = 0
		d.mask[1] = 0
		d.rehashIdx = -1
	}
	undoTx.Commit()

	return d.rehashIdx >= 0
}

func (d *dict) rehashStep(undoTx transaction.TX) bool {
	return d.rehash(undoTx, 1)
}

func (d *dict) nextPower(s int) int {
	i := DictInitSize
	for i < s {
		i *= 2
	}
	return i
}

func (d *dict) resize(undoTx transaction.TX, s int) error {
	s = d.nextPower(s) 
	if d.rehashing() {
		return errors.New("dict.resize: expanding while rehashing!")
	}
	var e *entry
	t := (*[1<<30]*entry)(heap.Alloc(undoTx, int(unsafe.Sizeof(e)) * s))[:s:s]
	undoTx.Begin()
	undoTx.Log(d)
	d.tables[1] = t
	d.mask[1] = s - 1
	d.rehashIdx = 0
	undoTx.Commit()
	return nil
}

func (d *dict) expandIfNeeded(undoTx transaction.TX) {
	if !d.rehashing() {
		if d.used[0] > len(d.tables[0]) * Ratio {
			d.resize(undoTx, d.used[0] * 2)
		}
	}
}

func (d *dict) shrinkIfNeeded(undoTx transaction.TX) {
	if !d.rehashing() {
		if len(d.tables[0]) > DictInitSize && d.used[0] < len(d.tables[0]) / Ratio {
			d.resize(undoTx, d.used[0])
		}
	}
}