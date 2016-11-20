package hashtable

import (
	"fmt"
	"sync"
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

	txnCtr uint64

	// Last Successful Txn
	lsTxn uint64
	// TODO: For mulitple writers
	//txnTable map[uint64]bool

	sync.Mutex
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

	h.lsTxn = 0
	h.txnCtr = 0
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
	h.Lock()
	defer h.Unlock()
	// NOTE: The rollback will be via the abandoning of the txn
	txn := atomic.AddUint64(&h.txnCtr, 1)
	current := &kv
	idxMod := make([]int, h.maxReach)

	for i := 0; i < h.maxReach; i++ {
		idx := h.hash1(current.Key)
		temp := (*KVType)(h.values[idx].Head())
		if (*temp == KVType{}) {
			h.insert(txn, current, idx)
			h.lsTxn = txn
			return nil
		}

		idx = h.hash2(current.Key)
		temp = (*KVType)(h.values[idx].Head())
		if (*temp == KVType{}) {
			h.insert(txn, current, idx)
			h.lsTxn = txn
			return nil
		}

		// Take the key from the second slot
		h.insert(txn, current, idx)
		idxMod[i] = idx
		current = temp
	}
	// Abandon the txn
	// Delete the elements of the current txn
	for i := 0; i < h.maxReach; i++ {
		h.values[idxMod[i]].Delete(txn)
	}

	return fmt.Errorf("Key %d couldn't be inserted due to a tight table", kv.Key)
}

// Get gets the keyvalue pair back
func (h *Hashtable) Get(k KeyType) (bool, ValType) {
	// NOTE: Make this serialised when we deal with mulitple
	// writers
	version := h.lsTxn

	idx := h.hash1(k)
	// No nil check needed as we are filling version 0
	val := (*KVType)(h.values[idx].LatestVersion(version))
	if (*val != KVType{} && val.Key == k) {
		return true, val.Val
	}

	idx = h.hash2(k)
	val = (*KVType)(h.values[idx].LatestVersion(version))
	if (*val != KVType{} && val.Key == k) {
		return true, val.Val
	}

	return false, 0
}

// Delete deletes all the elements with the key
func (h *Hashtable) Delete(k KeyType) (bool, error) {
	h.Lock()
	defer h.Unlock()
	txn := atomic.AddUint64(&h.txnCtr, 1)
	idx := h.hash1(k)
	val := (*KVType)(h.values[idx].LatestVersion(h.lsTxn))
	if (*val != KVType{} && val.Key == k) {
		h.insert(txn, &KVType{}, idx)
		return true, nil
	}

	idx = h.hash2(k)
	val = (*KVType)(h.values[idx].LatestVersion(h.lsTxn))
	if (*val != KVType{} && val.Key == k) {
		h.insert(txn, &KVType{}, idx)
		return true, nil
	}

	return false, nil
}
