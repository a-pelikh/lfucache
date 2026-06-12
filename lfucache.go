// Package lfucache provides a generic LFU cache.
package lfucache

import (
	"errors"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/a-pelikh/linkedlist"
)

// Cache is a fixed-capacity least-frequently-used cache.
//
// Implementations evict the least frequently used entry when capacity is
// reached. If several entries have the same frequency, the oldest entry in that
// frequency bucket is evicted first.
type Cache[K comparable, V any] interface {
	// Get returns the value for key and increments its frequency.
	//
	// It returns ErrKeyNotFound if key is not present.
	Get(key K) (V, error)

	// Put adds or updates key with value.
	//
	// Updating an existing key also increments its frequency.
	Put(key K, value V)

	// All returns an iterator over cache entries from highest frequency to
	// lowest frequency. Entries with the same frequency are returned from newest
	// to oldest.
	All() iter.Seq2[K, V]

	// Size returns the current number of entries in the cache.
	Size() int

	// Capacity returns the maximum number of entries the cache can hold.
	Capacity() int

	// GetKeyFrequency returns the current frequency for key.
	//
	// It returns ErrKeyNotFound if key is not present.
	GetKeyFrequency(key K) (int, error)
}

type (
	valueNode[K, V any] = *linkedlist.Node[data[K, V]]
	freqNode[K, V any]  = *linkedlist.Node[*linkedlist.List[data[K, V]]]
)

// ErrKeyNotFound is returned when a key is not present in the cache.
var ErrKeyNotFound = errors.New("key not found")

// DefaultCapacity is used by New when no capacity is provided.
const DefaultCapacity = 5

type data[K, V any] struct {
	key   K
	value V
	freq  int
}

type cacheImpl[K comparable, V any] struct {
	mux              sync.RWMutex
	freeFreqListNode freqNode[K, V]
	keyToNode        map[K]valueNode[K, V]
	freqToListNode   map[int]freqNode[K, V]
	orderedList      *linkedlist.List[*linkedlist.List[data[K, V]]]
	capacity         int
	size             atomic.Int64
}

// New returns a new LFU cache.
//
// If capacity is omitted, DefaultCapacity is used. New panics when capacity is
// non-positive.
func New[K comparable, V any](capacity ...int) *cacheImpl[K, V] {
	var c int

	if len(capacity) == 0 {
		c = DefaultCapacity
	} else if capacity[0] <= 0 {
		panic("invalid capacity")
	} else {
		c = capacity[0]
	}

	cache := &cacheImpl[K, V]{
		keyToNode:      make(map[K]valueNode[K, V]),
		freqToListNode: make(map[int]freqNode[K, V]),
		orderedList:    linkedlist.New[*linkedlist.List[data[K, V]]](),
		capacity:       c,
	}

	return cache
}

// Get returns the value for key and increments its frequency.
func (c *cacheImpl[K, V]) Get(key K) (V, error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	node, ok := c.keyToNode[key]
	if !ok {
		var zero V
		return zero, ErrKeyNotFound
	}

	c.moveToNextFreq(node)
	return node.Value.value, nil
}

// Put adds or updates key with value.
//
// Updating an existing key also increments its frequency.
func (c *cacheImpl[K, V]) Put(key K, value V) {
	c.mux.Lock()
	defer c.mux.Unlock()
	if node, ok := c.keyToNode[key]; ok {
		node.Value.value = value
		c.moveToNextFreq(node)
		return
	}

	var nodeValue valueNode[K, V]
	var reusableFreqNode freqNode[K, V]

	if c.Size() < c.capacity {
		c.size.Add(1)
	} else {
		nodeValue, reusableFreqNode = c.remove()
	}

	if nodeValue == nil {
		nodeValue = new(linkedlist.Node[data[K, V]])
	}

	nodeValue.Value.key = key
	nodeValue.Value.value = value
	nodeValue.Value.freq = 1

	firstFreqNode := c.getOrCreateFreqOneNode(reusableFreqNode)
	nodeValue = firstFreqNode.Value.PushNodeFront(nodeValue)
	c.keyToNode[key] = nodeValue
}

// getOrCreateFreqOneNode returns the bucket for frequency 1, reusing a detached
// frequency node when possible.
func (c *cacheImpl[K, V]) getOrCreateFreqOneNode(reuse freqNode[K, V]) freqNode[K, V] {
	if firstFreqNode, ok := c.freqToListNode[1]; ok {
		if reuse != nil {
			c.freeFreqListNode = reuse
		}
		return firstFreqNode
	}

	var firstFreqNode freqNode[K, V]

	switch {
	case reuse != nil:
		firstFreqNode = c.orderedList.PushNodeBack(reuse)

	case c.freeFreqListNode != nil:
		firstFreqNode = c.orderedList.PushNodeBack(c.freeFreqListNode)
		c.freeFreqListNode = nil

	default:
		list := linkedlist.New[data[K, V]]()
		firstFreqNode = c.orderedList.PushBack(list)
	}

	c.freqToListNode[1] = firstFreqNode
	return firstFreqNode
}

// All returns an iterator over cache entries from highest frequency to lowest
// frequency.
//
// The iterator snapshots entries under a read lock and yields them after
// unlocking.
func (c *cacheImpl[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		c.mux.RLock()

		items := make([]data[K, V], 0, c.Size())

		for freqList := range c.orderedList.Values() {
			for value := range freqList.Values() {
				items = append(items, data[K, V]{
					key:   value.key,
					value: value.value,
				})
			}
		}

		c.mux.RUnlock()

		for _, item := range items {
			if !yield(item.key, item.value) {
				return
			}
		}
	}
}

// moveToNextFreq moves node from its current frequency bucket to the next one.
//
// The caller must hold c.mux for writing.
func (c *cacheImpl[K, V]) moveToNextFreq(node valueNode[K, V]) {
	oldFreqNode, ok := c.freqToListNode[node.Value.freq]
	if !ok {
		panic("frequency not found")
	}

	oldFreq := node.Value.freq
	node.Value.freq++
	oldFreqNode.Value.Remove(node)

	if newFreqNode, ok := c.freqToListNode[node.Value.freq]; ok {
		newFreqNode.Value.PushNodeFront(node)
		if oldFreqNode.Value.Len() == 0 && c.freeFreqListNode == nil {
			delete(c.freqToListNode, oldFreq)
			c.orderedList.Remove(oldFreqNode)
			c.freeFreqListNode = oldFreqNode
		}
	} else {
		if oldFreqNode.Value.Len() == 0 {
			oldFreqNode.Value.PushNodeFront(node)
			delete(c.freqToListNode, oldFreq)
			c.freqToListNode[node.Value.freq] = oldFreqNode
		} else if c.freeFreqListNode != nil {
			newFreqNode = c.freeFreqListNode
			c.freeFreqListNode = nil

			newFreqNode.Value.PushNodeFront(node)
			newFreqNode = c.orderedList.InsertNodeBefore(newFreqNode, oldFreqNode)
			c.freqToListNode[node.Value.freq] = newFreqNode
		} else {
			newFreqList := linkedlist.New[data[K, V]]()
			newFreqList.PushNodeFront(node)
			newFreqNode = c.orderedList.InsertBefore(newFreqList, oldFreqNode)
			c.freqToListNode[node.Value.freq] = newFreqNode
		}
	}
}

// remove evicts and returns the oldest node from the lowest non-empty frequency
// bucket. It also returns a detached frequency node that can be reused by the
// caller.
func (c *cacheImpl[K, V]) remove() (valueNode[K, V], freqNode[K, V]) {
	minFreqNode := c.orderedList.Back()
	for minFreqNode != nil && minFreqNode.Value.Len() == 0 {
		minFreqNode = minFreqNode.Prev()
	}

	if minFreqNode == nil {
		return nil, nil
	}

	lastInsertedNode := minFreqNode.Value.Back()
	if lastInsertedNode == nil {
		return nil, nil
	}

	var freeFreqNode freqNode[K, V]
	if c.freeFreqListNode != nil {
		freeFreqNode = c.freeFreqListNode
		c.freeFreqListNode = nil
	}

	minFreqNode.Value.Remove(lastInsertedNode)
	delete(c.keyToNode, lastInsertedNode.Value.key)

	if minFreqNode.Value.Len() == 0 {
		if freeFreqNode == nil {
			freeFreqNode = minFreqNode
			c.orderedList.Remove(minFreqNode)
			delete(c.freqToListNode, lastInsertedNode.Value.freq)
		} else {
			c.freeFreqListNode = minFreqNode
			c.orderedList.Remove(minFreqNode)
			delete(c.freqToListNode, lastInsertedNode.Value.freq)
		}
	}

	return lastInsertedNode, freeFreqNode
}

// Size returns the current number of entries in the cache.
func (c *cacheImpl[K, V]) Size() int {
	return int(c.size.Load())
}

// Capacity returns the maximum number of entries the cache can hold.
func (c *cacheImpl[K, V]) Capacity() int {
	return c.capacity
}

// GetKeyFrequency returns the current frequency for key.
func (c *cacheImpl[K, V]) GetKeyFrequency(key K) (int, error) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	node, ok := c.keyToNode[key]
	if !ok {
		return 0, ErrKeyNotFound
	}

	return node.Value.freq, nil
}
