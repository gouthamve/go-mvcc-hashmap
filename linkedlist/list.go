package linkedlist

import (
	"sync/atomic"
	"unsafe"
)

// Node is a Linked List Node
type Node struct {
	next    *Node
	version uint64
	deleted *bool // Fucking atomic replace!

	object unsafe.Pointer
}

// LinkedList is a linked list?
type LinkedList struct {
	head *Node
}

// Insert Inserts a node into the list
func (ll *LinkedList) Insert(version uint64, data unsafe.Pointer) *Node {
	currentHead := ll.head
	f := false

	if currentHead == nil || version > currentHead.version {
		newNode := &Node{
			version: version,
			deleted: &f,
			next:    currentHead,

			object: data,
		}

		if !atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&ll.head)),
			unsafe.Pointer(currentHead),
			unsafe.Pointer(newNode),
		) {
			return ll.Insert(version, data)
		}

		return newNode
	}

	cursor := ll.head

	for {
		if cursor.next == nil || version > cursor.next.version {
			next := cursor.next

			// WTF are we spinning on this?
			if next != nil && *next.deleted {
				continue
			}

			newNode := &Node{
				version: version,
				deleted: &f,
				next:    next,

				object: data,
			}

			if !atomic.CompareAndSwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&cursor.next)),
				unsafe.Pointer(next),
				unsafe.Pointer(newNode),
			) {
				return ll.Insert(version, data)
			}

			return newNode
		}

		cursor = cursor.next
	}
}

func assignTrue() *bool {
	b := true
	return &b
}

// Delete deletes the shit out bruh
func (ll *LinkedList) Delete(version uint64) {
	var prev *Node
	currentHead := ll.head
	cursor := ll.head
	var t *bool
	t = assignTrue()

	for {
		if cursor == nil {
			break
		}

		if cursor.version == version {
			if !atomic.CompareAndSwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&cursor.deleted)),
				unsafe.Pointer(cursor.deleted),
				unsafe.Pointer(t),
			) {
				ll.Delete(version)
				return
			}

			rt := false

			if prev != nil {
				rt = atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&(prev.next))),
					unsafe.Pointer(prev.next),
					unsafe.Pointer(cursor.next),
				)
			} else {
				// HEAD!
				rt = atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&currentHead)),
					unsafe.Pointer(currentHead),
					unsafe.Pointer(cursor.next),
				)
			}

			if !rt {
				ll.Delete(version)
			}

			break
		}

		prev = cursor
		cursor = cursor.next
	}
}

// Head returns the object stored in the first node
func (ll *LinkedList) Head() unsafe.Pointer {
	return ll.head.object
}

// LatestVersion returns the node that has a version equal to
// or less than the version given
func (ll *LinkedList) LatestVersion(v uint64) unsafe.Pointer {
	cur := ll.head
	for cur != nil && cur.version > v {
		cur = cur.next
	}

	if cur == nil {
		return nil
	}

	return cur.object
}

// Snapshot gets the current Snapshot.
// For debugging only
func (ll *LinkedList) Snapshot() (s []uint64) {
	cursor := ll.head

	for cursor != nil {
		if !*cursor.deleted {
			s = append(s, cursor.version)
		}

		cursor = cursor.next
	}

	return
}
