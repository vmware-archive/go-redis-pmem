package redis

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"math/rand"
	"pmem/transaction"
	"sort"
	"strconv"
	"sync"
	"unsafe"
)

const (
	DictInitSize   = 1024
	Ratio          = 2
	BucketPerShard = 128
)

var (
	fnvHash hash.Hash32 = fnv.New32a()
)

type (
	dict struct {
		lock *sync.RWMutex
		tab  [2]table

		rehashlock *sync.RWMutex
		rehashIdx  int
	}

	table struct {
		bucketlock []sync.RWMutex // finegrained bucket locks
		bucket     []*entry
		used       []int
		mask       int
	}

	entry struct {
		key   []byte
		value []byte
		next  *entry
	}
)

func NewDict(tx transaction.TX) *dict {
	// should be replaced with pNew(dict)
	d := new(dict)

	tx.Begin()
	tx.Log(d)
	d.lock = new(sync.RWMutex)
	d.resetTable(tx, 0, DictInitSize)
	d.resetTable(tx, 1, 0)
	d.rehashlock = new(sync.RWMutex)
	d.rehashIdx = -1
	tx.Commit()
	return d
}

func (d *dict) resetTable(tx transaction.TX, i int, s int) {
	tx.Log(d.tab[i])
	if s == 0 {
		d.tab[i].bucketlock = nil
		d.tab[i].bucket = nil
		d.tab[i].used = nil
	} else {
		shards := shard(s)
		d.tab[i].bucketlock = make([]sync.RWMutex, shards)
		d.tab[i].bucket = make([]*entry, s)
		d.tab[i].used = make([]int, shards)
	}
	d.tab[i].mask = s - 1
}

// get the shard number of a bucket id
func shard(b int) int {
	return b / BucketPerShard
}

func newLocks(n int) (l []*sync.RWMutex) {
	l = make([]*sync.RWMutex, n)
	for i, _ := range l {
		l[i] = new(sync.RWMutex)
	}
	return l
}

func (d *dict) hashKey(key []byte) int {
	return memtierhash(key)
}

func fnvhash(key []byte) int {
	fnvHash.Write(key)
	h := int(fnvHash.Sum32())
	fnvHash.Reset()
	return h
}

func memtierhash(key []byte) int {
	h, _ := strconv.Atoi(string(key[8:]))
	return h
}

func (d *dict) findKey(undoTx transaction.TX, key []byte, readOnly bool) (int, int, *entry, *entry, float64) {
	h := d.hashKey(key)
	var (
		t, maxt, i int
		pre, curr  *entry
	)
	if d.tab[1].mask > 0 {
		maxt = 1
	} else {
		maxt = 0
	}
	var cmp float64 = 0
	for t = 0; t <= maxt; t++ {
		i = h & d.tab[t].mask
		if readOnly {
			undoTx.RLock(&d.tab[t].bucketlock[shard(i)])
			//undoTx.RLock(d.tab[t].lock)
		} else {
			undoTx.WLock(&d.tab[t].bucketlock[shard(i)])
			//undoTx.WLock(d.tab[t].lock)
		}
		curr = d.tab[t].bucket[i]
		for curr != nil {
			cmp++
			if bytes.Compare(curr.key, key) == 0 {
				return t, i, pre, curr, cmp
			}
			pre = curr
			curr = curr.next
		}
	}
	return maxt, i, pre, curr, cmp
}

func (d *dict) Used(undoTx transaction.TX) int {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.RLock(d.lock)
	u := 0
	for _, t := range d.tab {
		if t.used != nil {
			s := shard(t.mask + 1)
			for i := 0; i <= s; i++ {
				undoTx.RLock(&t.bucketlock[i])
				u += t.used[i]
			}
		}
	}
	return u
}

func (d *dict) Set(undoTx transaction.TX, key, value []byte) (int, int, float64) {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.RLock(d.lock)

	t, i, _, e, c := d.findKey(undoTx, key, false)

	// copy volatile value into pmem heap (need pmake and a helper function for copy)
	v := make([]byte, len(value)) //(*[1<<30]byte)(heap.Alloc(undoTx, len(value)))[:len(value):len(value)]
	copy(v, value)
	transaction.Persist(unsafe.Pointer(&v[0]), len(v)*int(unsafe.Sizeof(v[0]))) // shadow update

	if e != nil { // note that gc cannot recycle e.value before commit.
		undoTx.Log(&e.value)
		e.value = v
		return 0, 1, c
	} else {
		// copy volatile value into pmem heap (need pmake and a helper function for this copy)
		k := make([]byte, len(key)) //(*[1<<30]byte)(heap.Alloc(undoTx, len(key)))[:len(key):len(key)]
		copy(k, key)
		transaction.Persist(unsafe.Pointer(&k[0]), len(k)*int(unsafe.Sizeof(k[0]))) // shadow update

		// should be replaced with pNew
		e2 := new(entry) //(*entry)(heap.Alloc(undoTx, int(unsafe.Sizeof(*e))))
		e2.key = k
		e2.value = v
		e2.next = d.tab[t].bucket[i]
		transaction.Persist(unsafe.Pointer(e2), int(unsafe.Sizeof(*e2))) // shadow update
		undoTx.Log(d.tab[t].bucket[i : i+1])
		d.tab[t].bucket[i] = e2
		s := shard(i)
		undoTx.Log(d.tab[t].used[s : s+1])
		d.tab[t].used[s]++
		return 1, 0, c
	}
}

func (d *dict) Get(undoTx transaction.TX, key []byte) []byte {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.RLock(d.lock)

	_, _, _, e, _ := d.findKey(undoTx, key, true)
	if e != nil {
		return e.value
	}
	return []byte{}
}

func (d *dict) Del(undoTx transaction.TX, key []byte) bool {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.RLock(d.lock)

	t, i, p, e, _ := d.findKey(undoTx, key, false)
	deleted := false
	if e != nil { // note that gc cannot recycle e before commit.
		// update bucket (already locked when find key)
		if p != nil {
			undoTx.Log(p)
			p.next = e.next
		} else {
			undoTx.Log(d.tab[t].bucket[i : i+1])
			d.tab[t].bucket[i] = e.next
		}

		s := shard(i)
		undoTx.Log(d.tab[t].used[s : s+1])
		d.tab[t].used[s]--
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

	maxvisit := n * 10

	undoTx.Log(d)
	for n > 0 && d.rehashIdx <= d.tab[0].mask {
		if maxvisit--; maxvisit == 0 {
			break
		}

		e := d.tab[0].bucket[d.rehashIdx]
		if e != nil {
			undoTx.Log(d.tab[0].bucket[d.rehashIdx : d.rehashIdx+1])
			for n > 0 && e != nil {
				next := e.next
				i := d.hashKey(e.key) & d.tab[1].mask
				undoTx.Log(e)
				e.next = d.tab[1].bucket[i]
				undoTx.Log(d.tab[1].bucket[i : i+1])
				d.tab[1].bucket[i] = e

				s0 := shard(d.rehashIdx)
				s1 := shard(i)
				undoTx.Log(d.tab[0].used[s0 : s0+1])
				d.tab[0].used[s0]--
				undoTx.Log(d.tab[1].used[s1 : s1+1])
				d.tab[1].used[s1]++

				e = next
				n--
			}
			d.tab[0].bucket[d.rehashIdx] = e
		}
		if e == nil {
			d.rehashIdx++
		}
	}

	if d.rehashIdx > d.tab[0].mask {
		d.tab[0] = d.tab[1]
		d.resetTable(undoTx, 1, 0)
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

	undoTx.Log(d)
	d.resetTable(undoTx, 1, s)
	d.rehashIdx = 0
	//fmt.Println("Dict used", d.tab[0].used, "Resized to", s)
}

func (d *dict) ResizeIfNeeded(undoTx transaction.TX) {
	undoTx.Begin()
	defer undoTx.Commit()

	undoTx.WLock(d.lock)
	rehashIdx := d.rehashIdx

	if rehashIdx < 0 {
		size := d.tab[0].mask + 1
		used := 0
		for i := 0; i < shard(size); i++ {
			used += d.tab[0].used[i]
		}
		if used > size {
			fmt.Println("Dict size < used, expanding table!", size, used)
			d.resize(undoTx, used*2)
			return
		}
		if size > DictInitSize && used < size/Ratio {
			d.resize(undoTx, used)
			return
		}
	}
}

func (d *dict) lockKey(tx transaction.TX, key []byte) {
	tx.RLock(d.lock)

	maxt := 0
	if d.tab[1].mask > 0 {
		maxt = 1
	}

	for t := 0; t <= maxt; t++ {
		s := d.findShard(t, key)
		d.lockShard(tx, t, s)
	}
}

func (d *dict) lockKeys(tx transaction.TX, keys [][]byte, stride int) {
	tx.RLock(d.lock)

	maxt := 0
	if d.tab[1].mask > 0 {
		maxt = 1
	}
	shards := make([]int, len(keys)/stride)

	for t := 0; t <= maxt; t++ {
		for i, _ := range shards {
			shards[i] = d.findShard(t, keys[i*stride])
		}
		// make sure locks are acquired in the same order (ascending table and bucket id) to prevent deadlock!
		sort.Ints(shards)
		prev := -1
		for _, s := range shards {
			// only lock distinct shards
			if s != prev {
				d.lockShard(tx, t, s)
				prev = s
			}
		}
	}
}

func (d *dict) lockAllKeys(tx transaction.TX) {
	tx.RLock(d.lock)

	maxt := 0
	if d.tab[1].mask > 0 {
		maxt = 1
	}

	for t := 0; t <= maxt; t++ {
		for s := 0; s < shard(d.tab[t].mask+1); s++ {
			d.lockShard(tx, t, s)
		}
	}
}

func (d *dict) lockTables(tx transaction.TX) {
	tx.WLock(d.lock)
}

func (d *dict) findShard(t int, key []byte) int {
	return shard(d.hashKey(key) & d.tab[t].mask)
}

func (d *dict) lockShard(tx transaction.TX, t, s int) {
	// ReadOnly commands will aquire readOnly tx and read locks, otherwise WLock is aquired.
	tx.Lock(&d.tab[t].bucketlock[s])
}

func (d *dict) find(key []byte) (int, int, *entry, *entry) {
	h := d.hashKey(key)
	var (
		maxt, b   int
		pre, curr *entry
	)
	if d.tab[1].mask > 0 {
		maxt = 1
	} else {
		maxt = 0
	}
	for i := 0; i <= maxt; i++ {
		b = h & d.tab[i].mask
		pre = nil
		curr = d.tab[i].bucket[b]
		for curr != nil {
			if bytes.Compare(curr.key, key) == 0 {
				return i, b, pre, curr
			}
			pre = curr
			curr = curr.next
		}
	}
	return maxt, b, pre, curr
}

func (d *dict) set(tx transaction.TX, key, value []byte) (insert bool) {
	t, b, _, e := d.find(key)

	// copy volatile value into pmem heap (need pmake and a helper function for copy)
	v := make([]byte, len(value))
	copy(v, value)
	transaction.Persist(unsafe.Pointer(&v[0]), len(v)*int(unsafe.Sizeof(v[0]))) // shadow update

	if e != nil {
		tx.Log(&e.value)
		e.value = v
		return false
	} else {
		// copy volatile value into pmem heap (need pmake and a helper function for this copy)
		k := make([]byte, len(key)) //(*[1<<30]byte)(heap.Alloc(undoTx, len(key)))[:len(key):len(key)]
		copy(k, key)
		transaction.Persist(unsafe.Pointer(&k[0]), len(k)*int(unsafe.Sizeof(k[0]))) // shadow update

		// should be replaced with pNew
		e2 := new(entry) //(*entry)(heap.Alloc(undoTx, int(unsafe.Sizeof(*e))))
		e2.key = k
		e2.value = v
		e2.next = d.tab[t].bucket[b]
		transaction.Persist(unsafe.Pointer(e2), int(unsafe.Sizeof(*e2))) // shadow update
		tx.Log(d.tab[t].bucket[b : b+1])
		d.tab[t].bucket[b] = e2
		s := shard(b)
		tx.Log(d.tab[t].used[s : s+1])
		d.tab[t].used[s]++
		return true
	}
}

func (d *dict) delete(tx transaction.TX, key []byte) (deleted bool) {
	t, b, p, e := d.find(key)
	deleted = false
	if e != nil { // note that gc should not recycle e before commit.
		if p != nil {
			tx.Log(p)
			p.next = e.next
		} else {
			tx.Log(d.tab[t].bucket[b : b+1])
			d.tab[t].bucket[b] = e.next
		}

		s := shard(b)
		tx.Log(d.tab[t].used[s : s+1])
		d.tab[t].used[s]--
		deleted = true
	}
	return deleted
}

func (d *dict) size() int {
	s := 0
	for _, t := range d.tab {
		if t.used != nil {
			for _, u := range t.used {
				s += u
			}
		}
	}
	return s
}

func (d *dict) empty(tx transaction.TX) {
	d.resetTable(tx, 0, DictInitSize)
	d.resetTable(tx, 1, 0)
	tx.Log(d.rehashIdx)
	d.rehashIdx = -1
}

func (d *dict) randomKey() []byte {
	if d.size() == 0 {
		return nil
	}

	/* search from possible buckets */
	var e *entry = nil
	if d.rehashIdx >= 0 {
		for e == nil {
			h := d.rehashIdx + (rand.Int() % (d.tab[0].mask + d.tab[1].mask + 2 - d.rehashIdx))
			if h > d.tab[0].mask {
				e = d.tab[1].bucket[h-d.tab[0].mask-1]
			} else {
				e = d.tab[0].bucket[h]
			}
		}
	} else {
		for e == nil {
			h := rand.Int() & d.tab[0].mask
			e = d.tab[0].bucket[h]
		}
	}

	/* found a non empty bucket, search a random one from the entry list */
	ll := 0
	ee := e
	for ee != nil {
		ee = ee.next
		ll++
	}
	for i := 0; i < rand.Int()%ll; i++ {
		e = e.next
	}
	return e.key
}
