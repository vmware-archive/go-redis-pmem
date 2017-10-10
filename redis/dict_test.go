package redis

import (
	"testing"
	"runtime/debug"
	"pmem/transaction"
	"pmem/heap"
	"fmt"
	"strconv"
	"time"
)

var d *dict

func TestServer(t *testing.T) {
	s := new(server)
	go s.Start()
	time.Sleep(time.Duration(30)*time.Second) // give sometime to run memtier against server.
	conn := getClient()
	conn.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	time.Sleep(time.Duration(3)*time.Second)
	undoTx := transaction.NewUndo()
	assertEqual(t,"bar", s.db.Get(undoTx, "foo"))
	transaction.Release(undoTx)
}


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

func setup() transaction.TX {
	logSlice := make([]byte, transaction.LOGSIZE)
	heapSlice := make([]byte, 100000000)
	transaction.Init(logSlice)
	undoTx := transaction.NewUndo()
	heap.Init(undoTx, heapSlice, 100000000)
	d = NewDict(undoTx)
	return undoTx
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		debug.PrintStack()
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

