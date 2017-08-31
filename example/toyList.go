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
    tx.Begin()
    node := Node{l.head, key}
    nptr := heap.Alloc(int(unsafe.Sizeof(node)))
    tx.UpdateRedo(nptr, 
                  unsafe.Pointer(&node), 
                  unsafe.Sizeof(node))
    var tmpl List = *l
    tmpl.head = (*Node)(nptr)
    tmpl.len = l.len + 1
    tx.UpdateRedo(unsafe.Pointer(l), 
                  unsafe.Pointer(&tmpl), 
                  unsafe.Sizeof(tmpl))
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
    tmpl := *l
    tmpl.head = (*Node)(region.Swizzle(unsafe.Pointer(tmpl.head)))
    tx.UpdateRedo(unsafe.Pointer(l), 
                  unsafe.Pointer(&tmpl), 
                  unsafe.Sizeof(tmpl))
    nptr := tmpl.head
    // fmt.Println(l)
    for i := 0; i < l.len; i++ {
        // fmt.Println("Node before swizzle:",nptr)
        tmpnode := *nptr
        tmpnode.next = (*Node)(region.Swizzle(unsafe.Pointer(tmpnode.next)))
        tx.UpdateRedo(unsafe.Pointer(nptr), 
                      unsafe.Pointer(&tmpnode), 
                      unsafe.Sizeof(tmpnode))
        // fmt.Println("Node after swizzle:",nptr)
        nptr = tmpnode.next
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