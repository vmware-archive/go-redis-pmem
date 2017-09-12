package main

import (
    "fmt"
    "unsafe"
    "pmem/region"
    "pmem/tx"
    "pmem/heap"
)

type Node struct {
    next *Node
    key int
}

type List struct {
    head *Node
    len int
}

func (l *List) Insert(key int) {
    // put allocation outside tx as we do not support nested transaction yet.
    var node *Node 
    node = (*Node)(heap.Alloc(int(unsafe.Sizeof(*node))))
    
    tx.Begin()
    tx.LogUndo(node) // TODO: may shadow update node here.
    node.next = l.head
    node.key = key
    tx.LogUndo(l)
    l.head = node
    l.len += 1
    tx.Commit()
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

func (l *List) Swizzle() {
    // fmt.Println("Swizzle list:", l)
    tx.Begin()
    tx.LogUndo(l)
    l.head = (*Node)(region.Swizzle(unsafe.Pointer(l.head)))
    // fmt.Println(l)
    tmpnode := l.head
    for i := 0; i < l.len; i++ {
        tx.LogUndo(tmpnode)
        tmpnode.next = (*Node)(region.Swizzle(unsafe.Pointer(tmpnode.next)))
        tmpnode = tmpnode.next
    }
    tx.Commit()
}


func main() {
    region.Init("pmem_toyList_example", 8*1024, 11111)
    var l *List
    lptr := region.GetRoot()
    if lptr == nil {
        // no list created yet
        fmt.Println("Create an empty list in pmem.")
        lptr = heap.Alloc(int(unsafe.Sizeof(*l)))
        region.SetRoot(lptr)
    } else {
        l = (*List)(lptr)
        fmt.Println("Retrive list in pmem.")
        l.Swizzle()
        fmt.Println("Add one more node.")
        l.Insert(l.len)
        l.Show()
    }
}