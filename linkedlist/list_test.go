package linkedlist_test

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"unsafe"

	"github.com/gouthamve/mvcc_array/linkedlist"
)

func TestInitial(t *testing.T) {
	ll := linkedlist.LinkedList{}

	assert(t, len(ll.Snapshot()) == 0, "Expected initial length to be 0")
}

func TestSerialInsert(t *testing.T) {
	ll := linkedlist.LinkedList{}
	dummy := 1

	table := []struct {
		elem int
		list []int
	}{
		{1, []int{1}},
		{2, []int{2, 1}},
		{30, []int{30, 2, 1}},
		{25, []int{30, 25, 2, 1}},
		{0, []int{30, 25, 2, 1, 0}},
		{-10, []int{30, 25, 2, 1, 0, -10}},
		{20, []int{30, 25, 20, 2, 1, 0, -10}},
	}

	for _, v := range table {
		ll.Insert(v.elem, unsafe.Pointer(&dummy))
		equals(t, v.list, ll.Snapshot())
	}

}

func TestSerialDelete(t *testing.T) {
	ll := linkedlist.LinkedList{}
	dummy := 1

	table := []struct {
		elem int
		list []int
	}{
		{20, []int{30, 25, 2, 1, 0, -10}},
		{-10, []int{30, 25, 2, 1, 0}},
		{0, []int{30, 25, 2, 1}},
		{25, []int{30, 2, 1}},
		{30, []int{2, 1}},
		{2, []int{1}},
		{1, []int(nil)},
	}

	for _, v := range []int{30, 25, 20, 2, 1, 0, -10} {
		ll.Insert(v, unsafe.Pointer(&dummy))
	}

	equals(t, []int{30, 25, 20, 2, 1, 0, -10}, ll.Snapshot())

	for _, v := range table {
		ll.Delete(v.elem)
		equals(t, v.list, ll.Snapshot())
	}

}

func TestParallelInsert(t *testing.T) {
	dummy := 1

	table := []struct {
		elem int
		list []int
	}{
		{1, []int{1}},
		{2, []int{2, 1}},
		{30, []int{30, 2, 1}},
		{25, []int{30, 25, 2, 1}},
		{0, []int{30, 25, 2, 1, 0}},
		{-10, []int{30, 25, 2, 1, 0, -10}},
		{20, []int{30, 25, 20, 2, 1, 0, -10}},
	}

	for i := 0; i < len(table); i++ {
		ll := linkedlist.LinkedList{}
		c := make(chan bool)
		for j := 0; j <= i; j++ {
			go func(j int) {
				ll.Insert(table[j].elem, unsafe.Pointer(&dummy))
				c <- true
			}(j)
		}

		for j := 0; j <= i; j++ {
			<-c
		}

		equals(t, table[i].list, ll.Snapshot())
	}
}

func TestParallelDelete(t *testing.T) {
	dummy := 1

	table := []struct {
		elem int
		list []int
	}{
		{20, []int{30, 25, 2, 1, 0, -10}},
		{-10, []int{30, 25, 2, 1, 0}},
		{0, []int{30, 25, 2, 1}},
		{25, []int{30, 2, 1}},
		{30, []int{2, 1}},
		{2, []int{1}},
		{1, []int(nil)},
	}
	// equals(t, []int{30, 25, 20, 2, 1, 0, -10}, ll.Snapshot())

	for i := range table {
		ll := linkedlist.LinkedList{}
		for _, val := range []int{30, 25, 20, 2, 1, 0, -10} {
			ll.Insert(val, unsafe.Pointer(&dummy))
		}
		ch := make(chan bool)
		for j := 0; j <= i; j++ {
			go func(d int) {
				ll.Delete(table[d].elem)
				ch <- true
			}(j)
		}

		for j := 0; j <= i; j++ {
			<-ch
		}

		//fmt.Printf("%d\n", i)
		equals(t, table[i].list, ll.Snapshot())
	}
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
