package redis

import (
	"fmt"
	"pmem/region"
	"pmem/transaction"
	"runtime"
	"testing"
)

func TestZiplistGetBasic(t *testing.T) {
	fmt.Println("Basic index/get tests.")
	offset := runtime.PmallocInit("testziplist", transaction.LOGSIZE, DATASIZE)
	region.InitMeta(offset, transaction.LOGSIZE, UUID)
	tx := transaction.NewUndo()
	tx.Begin()
	zl := createList(tx)
	tx.Commit()

	var p int
	var v interface{}

	fmt.Println("Get element at first index.")
	p = zl.Index(0)
	v = zl.Get(p)
	assertEqual(t, v, []byte("hello"))

	fmt.Println("Get element at second index.")
	p = zl.Index(1)
	v = zl.Get(p)
	assertEqual(t, v, []byte("foo"))

	fmt.Println("Get element at last index.")
	p = zl.Index(3)
	v = zl.Get(p)
	assertEqual(t, v, int64(1024))

	fmt.Println("Get element out of range.")
	p = zl.Index(4)
	v = zl.Get(p)
	assertEqual(t, p, -1)
	assertEqual(t, v, nil)

	fmt.Println("Get element at index -1 (last element).")
	p = zl.Index(-1)
	v = zl.Get(p)
	assertEqual(t, v, int64(1024))

	fmt.Println("Get element at index -4 (first element).")
	p = zl.Index(-4)
	v = zl.Get(p)
	assertEqual(t, v, []byte("hello"))

	fmt.Println("Get element at index -5 (reverse out of range).")
	p = zl.Index(-5)
	v = zl.Get(p)
	assertEqual(t, p, -1)
	assertEqual(t, v, nil)
}

func createList(tx transaction.TX) *ziplist {
	zl := ziplistNew(tx)
	zl.Push(tx, []byte("foo"), false)
	zl.Push(tx, []byte("quux"), false)
	zl.Push(tx, []byte("hello"), true)
	zl.Push(tx, int64(1024), false)
	return zl
}
