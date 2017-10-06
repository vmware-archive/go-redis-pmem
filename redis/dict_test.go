package redis

import (
	"testing"
	"pmem/tx"
	"pmem/heap"
	"fmt"
	"strconv"
)

var d *dict

func TestDict(t *testing.T) {
	undoTx := setup()

	for i:=0; i<1000; i++ {
		d.Set(undoTx, strconv.Itoa(i),strconv.Itoa(i))
	}
	for i:=0; i<1000; i++ {
		assertEqual(t, strconv.Itoa(i), d.Get(undoTx, strconv.Itoa(i)))
	}
	for i:=0; i<1000; i++ {
		d.Set(undoTx, strconv.Itoa(i),strconv.Itoa(i+1))
	}
	for i:=0; i<1000; i++ {
		assertEqual(t, strconv.Itoa(i+1), d.Get(undoTx, strconv.Itoa(i)))
	}
	for i:=0; i<1000; i++ {
		d.Del(undoTx, strconv.Itoa(i))
	}
	for i:=0; i<1000; i++ {
		assertEqual(t, "", d.Get(undoTx, strconv.Itoa(i)))
	}
}

func setup() tx.Transaction {
	logSlice := make([]byte, tx.LOGSIZE)
	heapSlice := make([]byte, 100000000)
	tx.Init(logSlice)
	undoTx := tx.NewUndo()
	heap.Init(undoTx, heapSlice, 100000000)
	d = New(undoTx)
	return undoTx
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatal(fmt.Sprintf("%v != %v", a, b))
	}
}

func BenchmarkDictSet(b *testing.B) {
	undoTx := setup()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.Set(undoTx, strconv.Itoa(i%10000),strconv.Itoa(i))
	}
}

func BenchmarkMapInsert(b *testing.B) {
	mbench := make(map[string]string)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mbench[strconv.Itoa(i%10000)] = strconv.Itoa(i)
	}
}

