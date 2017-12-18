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
	"time"
	"unsafe"
)

const (
	Ratio = 2
)

var (
	fnvHash hash.Hash32 = fnv.New32a()
)

type (
	dict struct {
		lock *sync.RWMutex
		tab  [2]table

		rehashLock *sync.RWMutex
		rehashIdx  int

		initSize       int
		bucketPerShard int
	}

	table struct {
		bucketlock []sync.RWMutex // finegrained bucket locks
		bucket     []*entry
		used       []int
		mask       int
	}

	entry struct {
		key   []byte
		value interface{}
		next  *entry
	}

	dictIterator struct {
		d                *dict
		index            int
		table, safe      int
		entry, nextentry *entry
		fingerprint      int64
	}
)

func NewDict(tx transaction.TX, initSize, bucketPerShard int) *dict {
	// should be replaced with pNew(dict)
	d := pnew(dict)

	tx.Begin()
	tx.Log(d)
	d.initSize = nextPower(1, initSize)
	if bucketPerShard > initSize {
		d.bucketPerShard = initSize
	} else {
		d.bucketPerShard = bucketPerShard
	}
	d.lock = new(sync.RWMutex)
	d.rehashLock = new(sync.RWMutex)
	d.resetTable(tx, 0, d.initSize)
	d.resetTable(tx, 1, 0)
	d.rehashIdx = -1
	tx.Commit()
	return d
}

func (d *dict) resetTable(tx transaction.TX, i int, s int) {
	tx.Log(d.tab[i:])
	if s == 0 {
		d.tab[i].bucketlock = nil
		d.tab[i].bucket = nil
		d.tab[i].used = nil
	} else {
		shards := d.shard(s)
		d.tab[i].bucketlock = make([]sync.RWMutex, shards)
		d.tab[i].bucket = pmake([]*entry, s)
		d.tab[i].used = pmake([]int, shards)
	}
	d.tab[i].mask = s - 1
}

func inPMem(a unsafe.Pointer) {
	if a == nil {
		return
	}
	if uintptr(a) < pstart || uintptr(a) > pend {
		println(a)
		panic("Address not in pmem!")
	}
}

func (d *dict) swizzle(tx transaction.TX) {
	inPMem(unsafe.Pointer(d))
	d.lock = new(sync.RWMutex)
	d.rehashLock = new(sync.RWMutex)
	d.tab[0].swizzle(tx, d)
	d.tab[1].swizzle(tx, d)
}

func (t *table) swizzle(tx transaction.TX, d *dict) {
	s := t.mask + 1
	if s > 0 {
		shards := d.shard(s)
		t.bucketlock = make([]sync.RWMutex, shards)

		inPMem(unsafe.Pointer(&t.bucket[0]))
		inPMem(unsafe.Pointer(&t.used[0]))
		total := 0
		x := 0
		for _, u := range t.used {
			total += u
		}
		// fmt.Println("Total kv pairs:", total)
		for _, e := range t.bucket {
			for e != nil {
				x++
				//println(x, e, e.key, e.value, e.next)
				e.swizzle(tx)
				e = e.next
			}
		}
	}
}

func (e *entry) swizzle(tx transaction.TX) {
	inPMem(unsafe.Pointer(e))
	inPMem(unsafe.Pointer(&(e.key[0])))
	inPMem(unsafe.Pointer(e.next))
	//var tmp uintptr
	//word := uintptr(unsafe.Pointer(&e.value)) + uintptr(unsafe.Sizeof(tmp))
	//value := (**[]byte)(unsafe.Pointer(word))
	//inPMem(unsafe.Pointer(*value))
	switch v := e.value.(type) {
	case *[]byte:
		inPMem(unsafe.Pointer(&(*v)[0]))
	case *dict:
		v.swizzle(tx)
	case int64:
	default:
		fmt.Println(e)
		panic("unknown type!")
	}
}

// get the shard number of a bucket id
func (d *dict) shard(b int) int {
	return b / d.bucketPerShard
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

// rehash and resize
func (d *dict) Cron(sleep time.Duration) {
	tx := transaction.NewUndo()
	var used, size0, size1 int
	for {
		if size1 == 0 {
			time.Sleep(sleep) // reduce cpu consumption and lock contention
		}
		tx.Begin()
		tx.WLock(d.rehashLock)
		if d.rehashIdx < 0 {
			// check whether need to resize table when rehashIdx < 0
			tx.WLock(d.lock)
			used, size0, size1 = d.resizeIfNeeded(tx)
			if size1 > 0 {
				fmt.Println("Dictionary used", used, "Resize table to", size1)
			}
		} else if d.rehashIdx < size0 {
			// rehash, one key each time
			tx.RLock(d.lock)
			d.lockShard(tx, 0, d.shard(d.rehashIdx)) // lock bucket in tab[0]
			e := d.tab[0].bucket[d.rehashIdx]
			if e == nil {
				tx.Log(&d.rehashIdx)
				d.rehashIdx++
			} else {
				i0 := d.rehashIdx
				i1 := d.hashKey(e.key) & (size1 - 1)
				s0 := d.shard(i0)
				s1 := d.shard(i1)

				d.lockShard(tx, 1, s1)
				tx.Log(e)
				tx.Log(d.tab[0].bucket[i0 : i0+1])
				tx.Log(d.tab[1].bucket[i1 : i1+1])
				tx.Log(d.tab[0].used[s0 : s0+1])
				tx.Log(d.tab[1].used[s1 : s1+1])

				next := e.next
				e.next = d.tab[1].bucket[i1]
				d.tab[0].bucket[i0] = next
				d.tab[0].used[s0]--
				d.tab[1].bucket[i1] = e
				d.tab[1].used[s1]++
			}
		} else {
			// rehash finished, reset table
			tx.WLock(d.lock)
			tx.Log(d)
			d.tab[0] = d.tab[1]
			d.resetTable(tx, 1, 0)
			d.rehashIdx = -1
			size1 = 0
			fmt.Println("Rehash finished!")
		}
		tx.Commit()
	}
}

func (d *dict) shadowResize(tx transaction.TX) {
	size0 := len(d.tab[0].bucket)
	used := d.size()

	if used > size0 || (size0 > d.initSize && used < size0/Ratio) {
		// shadow rehash to a tmp table
		size1 := nextPower(d.initSize, used)
		shards := d.shard(size1)
		t1 := table{bucketlock: make([]sync.RWMutex, shards),
			bucket: pmake([]*entry, size1),
			used:   pmake([]int, shards),
			mask:   size1 - 1}
		for i := 0; i < size0; i++ {
			e := d.tab[0].bucket[i]
			for e != nil {
				i1 := d.hashKey(e.key) & (size1 - 1)
				s1 := d.shard(i1)
				e1 := pnew(entry)
				e1.key = e.key
				e1.value = e.value
				e1.next = t1.bucket[i1]
				transaction.Persist(unsafe.Pointer(e1), int(unsafe.Sizeof(*e1))) // shadow update
				t1.bucket[i1] = e1
				transaction.Persist(unsafe.Pointer(&t1.bucket[i1]), int(unsafe.Sizeof(e1))) // shadow update
				t1.used[s1] += 1
				transaction.Persist(unsafe.Pointer(&t1.used[s1]), int(unsafe.Sizeof(s1))) // shadow update
				e = e.next
			}
		}
		// swap table
		tx.Log(d.tab[0:1])
		d.tab[0] = t1
		//fmt.Println("rehash value to", size1, d.tab[0])
	}
}

func (d *dict) resizeIfNeeded(tx transaction.TX) (used, size0, size1 int) {
	size0 = len(d.tab[0].bucket)
	used = d.size()

	if used > size0 {
		return used, size0, d.resize(tx, used)
	} else if size0 > d.initSize && used < size0/Ratio {
		return used, size0, d.resize(tx, used)
	} else {
		return used, size0, 0
	}
}

func (d *dict) resize(tx transaction.TX, s int) int {
	s = nextPower(d.initSize, s)

	d.resetTable(tx, 1, s)
	tx.Log(&d.rehashIdx)
	d.rehashIdx = 0
	return s
}

func nextPower(s1, s2 int) int {
	if s1 < 1 {
		s1 = 1
	}
	for s1 < s2 {
		s1 *= 2
	}
	return s1
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
		for s := 0; s < d.shard(d.tab[t].mask+1); s++ {
			d.lockShard(tx, t, s)
		}
	}
}

func (d *dict) findShard(t int, key []byte) int {
	return d.shard(d.hashKey(key) & d.tab[t].mask)
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
	// fmt.Println("finding ", key)
	for i := 0; i <= maxt; i++ {
		b = h & d.tab[i].mask
		pre = nil
		curr = d.tab[i].bucket[b]
		for curr != nil {
			// fmt.Println("comparing with ", curr)
			if bytes.Compare(curr.key, key) == 0 {
				return i, b, pre, curr
			}
			pre = curr
			curr = curr.next
		}
	}
	return maxt, b, pre, curr
}

// key/value should be in pmem area, and appropriate locks should be already aquired at command level.
func (d *dict) set(tx transaction.TX, key []byte, value interface{}) (insert bool) {
	t, b, _, e := d.find(key)

	if e != nil {
		tx.Log(&e.value)
		e.value = value
		return false
	} else {
		e2 := pnew(entry)
		e2.key = key
		e2.value = value
		e2.next = d.tab[t].bucket[b]
		transaction.Persist(unsafe.Pointer(e2), int(unsafe.Sizeof(*e2))) // shadow update
		tx.Log(d.tab[t].bucket[b : b+1])
		d.tab[t].bucket[b] = e2
		s := d.shard(b)
		tx.Log(d.tab[t].used[s : s+1])
		d.tab[t].used[s]++
		// fmt.Println("set entry: ", e2)
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

		s := d.shard(b)
		tx.Log(d.tab[t].used[s : s+1])
		d.tab[t].used[s]--
		deleted = true
	}
	return deleted
}

func (d *dict) size() int {
	s := 0
	for _, t := range d.tab {
		s += t.size()
	}
	return s
}

func (t *table) size() int {
	s := 0
	if t.used != nil {
		for _, u := range t.used {
			s += u
		}
	}
	return s
}

func (d *dict) empty(tx transaction.TX) {
	d.resetTable(tx, 0, d.initSize)
	d.resetTable(tx, 1, 0)
	tx.Log(&d.rehashIdx)
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

func (d *dict) getIterator() *dictIterator {
	iter := &dictIterator{
		d:         d,
		table:     0,
		index:     -1,
		safe:      0,
		entry:     nil,
		nextentry: nil}
	return iter
}

func (i *dictIterator) next() *entry {
	for {
		if i.entry == nil {
			ht := i.d.tab[i.table]
			// TODO: implement safe iterator and fingerprint
			i.index++
			if i.index >= len(ht.bucket) {
				if i.table == 0 && len(i.d.tab[1].bucket) > 0 {
					i.table++
					i.index = 0
					ht = i.d.tab[1]
				} else {
					break
				}
			}
			i.entry = ht.bucket[i.index]
		} else {
			i.entry = i.nextentry
		}
		if i.entry != nil {
			i.nextentry = i.entry.next
			return i.entry
		}
	}
	return nil
}
