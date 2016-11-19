package hashtable_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/gouthamve/mvcc_array/hashtable"
)

func TestInit(t *testing.T) {
	h := hashtable.NewDefaultHT()
	assert(t, h != nil, "Expected init to return a decent map")
}

func TestInsert(t *testing.T) {
	mp := make(map[hashtable.KeyType]hashtable.ValType)
	h := hashtable.NewDefaultHT()

	for i := 0; i < 150; i++ {
		kv := hashtable.KVType{
			Key: hashtable.KeyType(rand.Int()),
			Val: hashtable.ValType(rand.Int()),
		}

		mp[kv.Key] = kv.Val
		ok(t, h.Put(kv))
	}

	for k, v := range mp {
		exists, val := h.Get(k)
		assert(t, exists, "Expected key %d to exist", k)
		equals(t, v, val)
	}
}

func TestDelete(t *testing.T) {
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
