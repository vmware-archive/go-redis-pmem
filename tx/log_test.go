package tx

import (
	"fmt"
	"unsafe"
	"testing"
)

var (
	b bool = false
	i int = 0
	ui64 uint64 = 0
)

type basic1 struct {
	b bool
	i int
}

type basic2 struct {
	i int
	iptr *int
	slice []int
}

func TestLog(t *testing.T) {
	fmt.Println("Testing basic data types.")
	UpdateUndo(&b, true)
	assertEqual(t, b, true)

	UpdateUndo(&i, -1)
	assertEqual(t, i, -1)

	UpdateUndo(&ui64, uint64(10))
	assertEqual(t, ui64, uint64(10))

	fmt.Println("Testing basic data structs and pointers.")
	s1 := basic1{}
	s2 := basic1{true, 1}
	UpdateUndo(&s1, &s2)
	assertEqual(t, s1, s2)

	UpdateUndo(&s1.b, false)
	UpdateUndo(&s1.i, 5)
	assertEqual(t, s1, basic1{false, 5})

	slice := make([]int, 5)
	slice[0] = 1
	s3 := basic2{}
	s4 := basic2{1, &i, slice}
	UpdateUndo(&s3, &s4)
	assertEqual(t, s3.i, s4.i)
	assertEqual(t, s3.iptr, s4.iptr)
	slice[1] = 2
	for i:=0; i<len(slice); i++ {
		assertEqual(t, s3.slice[i], slice[i])
	}
	UpdateUndo(&s3.iptr, nil)
	UpdateUndo(&s3.slice, nil)

	UpdateUndo(&s3.iptr, &s3.i)
	UpdateUndo(s3.iptr, 3)
	assertEqual(t, s3.i, 3)

	fmt.Println("Testing basic slices.")
	a1 := []int{1,2,3,4,5}
	a2 := []int{5,4,3,2,1}
	UpdateUndo(a1, a2)
	for i, a := range a1 {
		assertEqual(t, a, a2[i])
	}

	a2[1] = 100
	UpdateUndo(a1[1:3], a2[1:3])
	for i, a := range a1 {
		assertEqual(t, a, a2[i])
	}

	a3 := make([]basic2, 3)
	a4 := make([]basic2, 3)
	UpdateUndo(&a3[0], s3)
	assertEqual(t, a3[0].i, s3.i)
	a4[0] = s4
	UpdateUndo(a3, a4)
	assertEqual(t, a3[0].i, s4.i)
}

var (
	slice1 = make([]int, 100, 100)
	slice2 = make([]int, 100, 100)
	s1 = basic2{1, nil, slice1}
	s2 = basic2{2, nil, slice2}
)

func BenchmarkLogStruct(b *testing.B) {
	for i := 0; i < b.N; i++ {
        UpdateUndo(&s1, &s2)
    }
}

func BenchmarkLogStructRaw(b *testing.B) {
	for i := 0; i < b.N; i++ {
        UpdateRedo(unsafe.Pointer(&s1), unsafe.Pointer(&s2), unsafe.Sizeof(s1))
    }
}

func BenchmarkLogSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
        UpdateUndo(slice1, slice2)
    }
}

func BenchmarkLogSliceRaw(b *testing.B) {
	for i := 0; i < b.N; i++ {
        UpdateRedo(unsafe.Pointer(&slice1[0]), unsafe.Pointer(&slice2[0]), 100)
    }
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatal(fmt.Sprintf("%v != %v", a, b))
	}
}
