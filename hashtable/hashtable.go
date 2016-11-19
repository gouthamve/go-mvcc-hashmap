package hashtable

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/gouthamve/mvcc_array/linkedlist"
)

// Hashtable implements a cuckoo hashing based table
type Hashtable struct {
	size     int
	maxReach int

	// TODO: Make pointer. The comparision takes time??
	values []*linkedlist.LinkedList
	txn    uint64
}

// KeyType is the key
type KeyType int

// ValType is the val
type ValType int

// KVType is the keyvalue type
type KVType struct {
	Key KeyType
	Val ValType
}

func newHT(size, maxReach int) *Hashtable {
	h := Hashtable{
		size:     size,
		maxReach: maxReach,
	}

	h.values = make([]*linkedlist.LinkedList, size)
	for i := range h.values {
		h.values[i] = &linkedlist.LinkedList{}

		h.insert(0, &KVType{}, i)
	}
	return &h
}

// NewDefaultHT returns a new Hashtable with sensible defaults
func NewDefaultHT() *Hashtable {
	return newHT(512, 16)
}

// TODO: EDIT THIS
func (h *Hashtable) hash1(i KeyType) int {
	return int(i) % h.size
}

// TODO: EDIT THIS
func (h *Hashtable) hash2(i KeyType) int {
	return (int(i) / 11) % h.size
}

// TODO: Do we need a KeyType
func (h *Hashtable) insert(txn uint64, kv *KVType, idx int) error {
	h.values[idx].Insert(txn, unsafe.Pointer(kv))
	return nil
}

// Put puts the kv into the hashtable
// ANNY: This is the key part. See how the rollback happens
func (h *Hashtable) Put(kv KVType) error {
	// NOTE: The rollback will be via the abandoning of the txn
	txn := atomic.AddUint64(&h.txn, 1)
	current := &kv

	for i := 0; i < h.maxReach; i++ {
		idx := h.hash1(current.Key)
		temp := (*KVType)(h.values[idx].Head())
		if (*temp == KVType{}) {
			h.insert(txn, current, idx)
			return nil
		}

		idx = h.hash2(current.Key)
		temp = (*KVType)(h.values[idx].Head())
		if (*temp == KVType{}) {
			h.insert(txn, current, idx)
			return nil
		}

		// Take the key from the second slot
		h.insert(txn, current, idx)
		current = temp
	}
	// Abandon the txn

	return fmt.Errorf("Key %d couldn't be inserted due to a tight table", kv.Key)
}

// Get gets the keyvalue pair back
func (h *Hashtable) Get(k KeyType) (bool, ValType) {
	idx := h.hash1(k)
	val := (*KVType)(h.values[idx].Head())
	if (*val != KVType{} && val.Key == k) {
		return true, val.Val
	}

	idx = h.hash2(k)
	val = (*KVType)(h.values[idx].Head())
	if (*val != KVType{} && val.Key == k) {
		return true, val.Val
	}

	return false, 0
}

// DeleteUni deletes the single KV pair given
//func (h *Hashtable) DeleteUni(k KVType) (bool, error) {
//idx := h.hash1(k.Key)
//val := h.values[idx]
//if (val != KVType{} && val.Key == k.Key && val.Val == k.Val) {
//h.values[idx] = KVType{}
//return true, nil
//}

//idx = h.hash2(k)
//val = (*KVType) (h.values[idx].Head())
//if (*val != KVType{} && val.Key == k.Key && val.Val == k.Val) {
//h.values[idx] = KVType{}
//return true, nil
//}

//return false, nil
//}

//// Delete deletes all the elements with the key
//func (h *Hashtable) Delete(k KeyType) (bool, error) {
//idx := h.hash1(k)
//val := h.values[idx]
//if (val != KVType{} && val.Key == k.Key) {
//h.values[idx] = KVType{}
//return true, nil
//}

//idx = h.hash2(k)
//val = h.values[idx]
//if (val != KVType{} && val.Key == k.Key) {
//h.values[idx] = KVType{}
//return true, nil
//}

//return false, nil
//}
