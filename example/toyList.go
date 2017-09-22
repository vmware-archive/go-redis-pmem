package main

import (
	"fmt"
	"pmem/heap"
	"pmem/region"
	"pmem/tx"
	"unsafe"
)

type Node struct {
	next *Node
	key  int
}

type List struct {
	head *Node
	len  int
}

func (l *List) Insert(t tx.Transaction, key int) {
	// put allocation outside tx as we do not support nested transaction yet.
	var node *Node
	node = (*Node)(heap.Alloc(t, int(unsafe.Sizeof(*node))))

	t.Begin()
	t.Log(node) // TODO: may shadow update node here.
	node.next = l.head
	node.key = key
	t.Log(l)
	l.head = node
	l.len += 1
	t.Commit()
}

func (l *List) Show() {
	node := l.head
	fmt.Print("HEAD->")
	for i := 0; i < l.len; i++ {
		fmt.Print(node.key, "->")
		node = node.next
	}
	fmt.Print("nil\n")
}

func (l *List) Swizzle(t tx.Transaction) {
	// fmt.Println("Swizzle list:", l)
	t.Begin()
	t.Log(l)
	l.head = (*Node)(region.Swizzle(unsafe.Pointer(l.head)))
	// fmt.Println(l)
	tmpnode := l.head
	for i := 0; i < l.len; i++ {
		t.Log(tmpnode)
		tmpnode.next = (*Node)(region.Swizzle(unsafe.Pointer(tmpnode.next)))
		tmpnode = tmpnode.next
	}
	t.Commit()
}

func main() {
	region.Init("pmem_toyList_example", 8*1024, 11111)
	var l *List
	lptr := region.GetRoot()
	t := tx.NewUndo()
	if lptr == nil {
		// no list created yet
		fmt.Println("Create an empty list in pmem.")
		lptr = heap.Alloc(t, int(unsafe.Sizeof(*l)))
		region.SetRoot(t, lptr)
	} else {
		l = (*List)(lptr)
		fmt.Println("Retrive list in pmem.")
		l.Swizzle(t)
		fmt.Println("Add one more node.")
		l.Insert(t, l.len)
		l.Show()
	}
	tx.Release(t)
}
