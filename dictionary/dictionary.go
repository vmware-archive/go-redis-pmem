package dictionary

import (
	"pmem/tx"
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

func New() *dict {
	var (
		d *dict
		t []*entry
		e *entry
	)
	// should be replaced with pNew(dict)
	d = (*dict)(heap.Alloc(int(unsafe.Sizeof(*d))))
	// should be replaced with pMake([]entry, DictInitSize)
	t = (*[DictInitSize]*entry)(heap.Alloc(int(unsafe.Sizeof(e)) * DictInitSize))[:DictInitSize:DictInitSize]

	tx.Begin()
	tx.LogUndo(d)
	d.tables[0] = t
	d.mask[0] = DictInitSize - 1
	d.rehashIdx = -1
	tx.Commit()
	return d
}

func (d *dict) hashKey(key string) int { // may implement fast hashing in asm
	fnvHash.Write([]byte(key))
	h := int(fnvHash.Sum32())
	fnvHash.Reset()
	return h
}

func (d *dict) findKey(key string) (int, int, *entry, *entry) {
	d.rehashStep()
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


func (d *dict) Set(key, value string) {
	t, i, _, e := d.findKey(key)

	// copy volatile value into pmem heap
	v := (*[1<<30]byte)(heap.Alloc(len(value)))[:len(value):len(value)]
	copy(v, value)
	tx.Persist(unsafe.Pointer(&v[0]), len(v)) // shadow update

	tx.Begin()
	if e != nil { // note that gc cannot recycle e.value before commit.
		tx.LogUndo(&e.value)
		e.value = v
	} else {
		k := (*[1<<30]byte)(heap.Alloc(len(key)))[:len(key):len(key)]
		copy(k, key)
		tx.Persist(unsafe.Pointer(&k[0]), len(k)) // shadow update

		e2 := (*entry)(heap.Alloc(int(unsafe.Sizeof(*e))))
		tx.LogUndo(e2)
		e2.key = k
		e2.value = v
		e2.next = d.tables[t][i]
		tx.LogUndo(d.tables[t][i:i+1])
		d.tables[t][i] = e2
		tx.LogUndo(d.used[t:])
		d.used[t]++
	}
	tx.Commit()
	d.expandIfNeeded()
}

func (d *dict) Get(key string) string {
	_, _, _, e := d.findKey(key)
	if e != nil {
		return string(e.value)
	}
	return ""
}

func (d *dict) Del(key string) bool {
	t, i, p, e := d.findKey(key)
	deleted := false
	tx.Begin()
	if e != nil { // note that gc cannot recycle e before commit.
		if p != nil {
			tx.LogUndo(p)
			p.next = e.next
		} else {
			tx.LogUndo(d.tables[t][i:i+1])
			d.tables[t][i] = e.next
		}
		tx.LogUndo(d.used[t:])
		d.used[t]--
		deleted = true
	}
	tx.Commit()
	d.shrinkIfNeeded()
	return deleted
}

func (d *dict) rehashing() bool {
	return d.rehashIdx >= 0
}

func (d *dict) rehash(n int) bool {
	visit := n*10
	if !d.rehashing() {
		return false
	}

	tx.Begin()
	tx.LogUndo(d)
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
			tx.LogUndo(e)
			e.next = d.tables[1][i]
			tx.LogUndo(d.tables[1][i:i+1])
			d.tables[1][i] = e
			d.used[0]--
			d.used[1]++
			e = next
		}
		tx.LogUndo(d.tables[0][d.rehashIdx:d.rehashIdx+1])
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
	tx.Commit()

	return d.rehashIdx >= 0
}

func (d *dict) rehashStep() bool {
	return d.rehash(1)
}

func (d *dict) nextPower(s int) int {
	i := DictInitSize
	for i < s {
		i *= 2
	}
	return i
}

func (d *dict) resize(s int) error {
	s = d.nextPower(s) 
	if d.rehashing() {
		return errors.New("dict.resize: expanding while rehashing!")
	}
	var e *entry
	t := (*[1<<30]*entry)(heap.Alloc(int(unsafe.Sizeof(e)) * s))[:s:s]
	tx.Begin()
	tx.LogUndo(d)
	d.tables[1] = t
	d.mask[1] = s - 1
	d.rehashIdx = 0
	tx.Commit()
	return nil
}

func (d *dict) expandIfNeeded() {
	if !d.rehashing() {
		if d.used[0] > len(d.tables[0]) * Ratio {
			d.resize(d.used[0] * 2)
		}
	}
}

func (d *dict) shrinkIfNeeded() {
	if !d.rehashing() {
		if len(d.tables[0]) > DictInitSize && d.used[0] < len(d.tables[0]) / Ratio {
			d.resize(d.used[0])
		}
	}
}