package dictionary

import (
	"testing"
	"pmem/tx"
	"pmem/heap"
	"fmt"
	"strconv"
)

var d *dict

func TestDict(t *testing.T) {
	setup()

	for i:=0; i<1000; i++ {
		d.Set(strconv.Itoa(i),strconv.Itoa(i))
	}
	for i:=0; i<1000; i++ {
		assertEqual(t, strconv.Itoa(i), d.Get(strconv.Itoa(i)))
	}
	for i:=0; i<1000; i++ {
		d.Set(strconv.Itoa(i),strconv.Itoa(i+1))
	}
	for i:=0; i<1000; i++ {
		assertEqual(t, strconv.Itoa(i+1), d.Get(strconv.Itoa(i)))
	}
	for i:=0; i<1000; i++ {
		d.Del(strconv.Itoa(i))
	}
	for i:=0; i<1000; i++ {
		assertEqual(t, "", d.Get(strconv.Itoa(i)))
	}
}

func setup() {
	logSlice := make([]byte, tx.LOGSIZE)
	heapSlice := make([]byte, 100000000)
	tx.Init(logSlice, tx.LOGSIZE)
	heap.Init(heapSlice, 100000000)
	d = New()
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatal(fmt.Sprintf("%v != %v", a, b))
	}
}

func BenchmarkDictSet(b *testing.B) {
	setup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Set(strconv.Itoa(i%10000),strconv.Itoa(i))
	}
}

func BenchmarkMapInsert(b *testing.B) {
	mbench := make(map[string]string)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mbench[strconv.Itoa(i%10000)] = strconv.Itoa(i)
	}
}

