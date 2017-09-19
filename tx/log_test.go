package tx

import (
	"fmt"
	"testing"
)

type basic struct {
	i     int
	iptr  *int
	slice []int
}

var (
	b      = false
	i      = 0
	slice1 = make([]int, 100, 100)
	slice2 = make([]int, 100, 100)
	s1     = basic{1, nil, slice1}
	s2     = basic{2, nil, slice2}
)

func setup() {
	logSlice := make([]byte, LOGSIZE)
	Init(logSlice, LOGSIZE)
}

func TestLog(t *testing.T) {
	setup()

	fmt.Println("Testing basic data type commit.")
	Begin()
	LogUndo(&b)
	LogUndo(&i)
	b = true
	i = 10
	Commit()
	assertEqual(t, b, true)
	assertEqual(t, i, 10)

	fmt.Println("Testing basic data type abort.")
	Begin()
	LogUndo(&b)
	LogUndo(&i)
	b = false
	i = 0
	Abort()
	assertEqual(t, b, true)
	assertEqual(t, i, 10)

	fmt.Println("Testing data structure commit.")
	Begin()
	LogUndo(&s1)
	s1.i = 10
	s1.iptr = &s1.i
	s1.slice = slice1
	Commit()
	assertEqual(t, s1.i, 10)
	assertEqual(t, *s1.iptr, 10)
	slice1[0] = 11
	assertEqual(t, s1.slice[0], slice1[0])

	Begin()
	LogUndo(&s1)
	s1 = s2
	s1.iptr = &s1.i
	Commit()
	assertEqual(t, s1.i, 2)
	assertEqual(t, *s1.iptr, s2.i)
	slice2[0] = 22
	assertEqual(t, s1.slice[0], slice2[0])

	fmt.Println("Testing data structure abort.")
	Begin()
	LogUndo(&s1)
	s1.i = 10
	s1.iptr = nil
	s1.slice = slice1
	Abort()
	assertEqual(t, s1.i, 2)
	assertEqual(t, s1.iptr, &s1.i)
	assertEqual(t, s1.slice[0], slice2[0])

	Begin()
	LogUndo(s1.iptr)
	s1.i = 100
	Abort()
	assertEqual(t, s1.i, 2)

	s1.iptr = &i

	Begin()
	LogUndo(&s1)
	*s1.iptr = 1000 // redirect update will not rollback
	s2.i = 3
	s2.iptr = &s2.i
	s2.slice = slice1
	s1 = s2
	Abort()
	assertEqual(t, s1.i, 2)
	assertEqual(t, *s1.iptr, 1000)
	assertEqual(t, s1.slice[0], slice2[0])

	fmt.Println("Testing slice commit.")
	Begin()
	LogUndo(slice1)
	slice1[99] = 99
	Commit()
	assertEqual(t, slice1[99], 99)

	fmt.Println("Testing slice abort.")
	Begin()
	LogUndo(slice1[:10])
	slice2[9] = 9
	slice2[10] = 10 // out of range update will not rollback
	copy(slice1, slice2)
	Abort()
	assertEqual(t, slice1[9], 0)
	assertEqual(t, slice1[10], 10)
	assertEqual(t, slice1[99], 0)
}

func BenchmarkLogInt(b *testing.B) {
	setup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Begin()
		LogUndo(i)
		Commit()
	}
}

func BenchmarkLogStruct(b *testing.B) {
	setup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Begin()
		LogUndo(s1)
		Commit()
	}
}

func BenchmarkLogSlice(b *testing.B) {
	setup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Begin()
		LogUndo(slice1)
		Commit()
	}
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatal(fmt.Sprintf("%v != %v", a, b))
	}
}
