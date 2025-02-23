package main

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxTryRehashing            = 3
	writeBackoffWhileRehashing = time.Millisecond
)

// CuckooHashTable represents the cuckoo hash table structure
type CuckooHashTable struct {
	table1    []string
	table2    []string
	size      int
	maxKicks  int
	count     int          // Track number of items in the table
	mu        sync.RWMutex // Add RWMutex for thread safety
	rehashing atomic.Bool  // Flag to indicate rehashing in progress
}

// NewCuckooHashTable creates a new cuckoo hash table with given size
func NewCuckooHashTable(size int) *CuckooHashTable {
	return &CuckooHashTable{
		table1:   make([]string, size),
		table2:   make([]string, size),
		size:     size,
		maxKicks: size * 2, // Prevent infinite loops
		count:    0,
	}
}

// hash1 is the first hash function
func (c *CuckooHashTable) hash1(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7FFFFFFF) % c.size // Use bitwise AND to make positive
}

// hash2 is the second hash function
func (c *CuckooHashTable) hash2(key string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64()&0x7FFFFFFFFFFFFFFF) % c.size // Use bitwise AND to make positive
}

// insertWithoutRehash is a helper function that attempts insertion without triggering rehash
func (c *CuckooHashTable) insertWithoutRehash(key string) bool {
	for c.rehashing.Load() {
		fmt.Println("Waiting for rehash to complete")
		time.Sleep(time.Millisecond)
	}

	if c.Contains(key) {
		return true
	}

	// Keep track of the path of displacements
	type displacement struct {
		key      string
		isTable1 bool
		position int
	}
	path := make([]displacement, 0, 2*c.maxKicks)

	currentKey := key
	for range c.maxKicks {
		// Try table1
		pos1 := c.hash1(currentKey)
		path = append(path, displacement{
			key:      currentKey,
			isTable1: true,
			position: pos1,
		})
		currentKey, c.table1[pos1] = c.table1[pos1], currentKey
		if currentKey == "" {
			return true
		}

		// Try table2
		pos2 := c.hash2(currentKey)
		path = append(path, displacement{
			key:      currentKey,
			isTable1: false,
			position: pos2,
		})
		currentKey, c.table2[pos2] = c.table2[pos2], currentKey
		if currentKey == "" {
			return true
		}
	}

	// Insertion failed, restore the original state by walking back the path
	for i := len(path) - 1; i >= 0; i-- {
		d := path[i]
		if d.isTable1 {
			currentKey, c.table1[d.position] = c.table1[d.position], currentKey
		} else {
			currentKey, c.table2[d.position] = c.table2[d.position], currentKey
		}
	}

	return false
}

// prepareRehash checks if rehashing is needed and prepares a new table if so
func (c *CuckooHashTable) prepareRehash(size int) *CuckooHashTable {
	// Take a snapshot of the current table while holding the lock
	c.mu.RLock()
	// Create copies of the current tables
	items := make([]string, 0, c.count)
	for _, item := range c.table1 {
		if item != "" {
			items = append(items, item)
		}
	}
	for _, item := range c.table2 {
		if item != "" {
			items = append(items, item)
		}
	}
	c.mu.RUnlock()

	// Create new table with double size
	newTable := NewCuckooHashTable(size)

	// Try to insert all items into new table
	for _, item := range items {
		if !newTable.insertWithoutRehash(item) {
			return nil
		}
		newTable.count++
	}

	return newTable
}

// doRehash performs the rehashing operation
func (c *CuckooHashTable) doRehash() bool {
	// If already rehashing, wait for it to complete
	for c.rehashing.Load() {
		time.Sleep(time.Millisecond)
	}

	fmt.Println("Starting new rehash")
	c.rehashing.Store(true)
	defer c.rehashing.Store(false)

	// prepare until success
	var prepared *CuckooHashTable
	size := c.size
	for c.GetLoadFactor() >= 0.5 || prepared == nil {
		size *= 2
		// double the size until success
		fmt.Println("Preparing rehash to size", size)
		prepared = c.prepareRehash(size)
		if prepared == nil {
			time.Sleep(time.Millisecond)
		} else {
			c.swapTables(prepared)
		}
	}

	return true
}

func (c *CuckooHashTable) swapTables(prepared *CuckooHashTable) {
	fmt.Println("Rehashing to size", prepared.size)
	// Acquire write lock only when ready to swap tables
	c.mu.Lock()
	c.table1 = prepared.table1
	c.table2 = prepared.table2
	c.size = prepared.size
	c.maxKicks = prepared.maxKicks
	c.count = prepared.count
	c.mu.Unlock()
	fmt.Println("Rehash completed")
}

// Insert adds a key to the hash table
func (c *CuckooHashTable) Insert(key string) bool {
	backoff := writeBackoffWhileRehashing
	for c.rehashing.Load() {
		time.Sleep(backoff)
		backoff *= 2
	}

	for range maxTryRehashing {
		if c.insertWithoutRehash(key) {
			c.count += 1
			return true
		}
		c.doRehash()
	}

	return false
}

// Contains checks if a key exists in the hash table
func (c *CuckooHashTable) Contains(key string) bool {
	pos1 := c.hash1(key)
	pos2 := c.hash2(key)
	return c.table1[pos1] == key || c.table2[pos2] == key
}

// Remove deletes a key from the hash table
func (c *CuckooHashTable) Remove(key string) bool {
	backoff := writeBackoffWhileRehashing
	for c.rehashing.Load() {
		time.Sleep(backoff)
		backoff *= 2
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	pos1 := c.hash1(key)
	if c.table1[pos1] == key {
		c.table1[pos1] = ""
		c.count--
		return true
	}

	pos2 := c.hash2(key)
	if c.table2[pos2] == key {
		c.table2[pos2] = ""
		c.count--
		return true
	}

	return false
}

// GetLoadFactor returns the current load factor of the hash table
func (c *CuckooHashTable) GetLoadFactor() float64 {
	c.mu.RLock() // Use read lock for reading stats
	defer c.mu.RUnlock()
	return float64(c.count) / float64(c.size*2)
}

// GetSize returns the current size of each table
func (c *CuckooHashTable) GetSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}

// GetCount returns the number of items in the hash table
func (c *CuckooHashTable) GetCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.count
}

func main() {
	// Create a new cuckoo hash table
	table := NewCuckooHashTable(4)

	// try testing the rehashing concurrently
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		i := i // Create new variable for goroutine
		go func() {
			defer wg.Done()
			table.Insert(fmt.Sprintf("test%d", i))
		}()
	}

	// Wait for concurrent insertions to complete
	wg.Wait()

	// Insert more values to trigger rehashing
	keys := []string{"apple", "banana", "orange", "grape", "mango", "pear", "kiwi", "plum"}
	for _, key := range keys {
		fmt.Printf("Current size: %d, Load factor: %.2f\n",
			table.GetSize(), table.GetLoadFactor())
		if table.Insert(key) {
			fmt.Printf("Successfully inserted: %s\n", key)
		} else {
			fmt.Printf("Failed to insert: %s\n", key)
		}
	}

	// Check if keys exist
	fmt.Printf("\nChecking containment:\n")
	checkKeys := append(keys, "pear")
	for _, key := range checkKeys {
		if table.Contains(key) {
			fmt.Printf("%s is in the table\n", key)
		} else {
			fmt.Printf("%s is not in the table\n", key)
		}
	}

	// Remove some keys
	fmt.Printf("\nRemoving keys:\n")
	removeKeys := []string{"apple", "pear"}
	for _, key := range removeKeys {
		if table.Remove(key) {
			fmt.Printf("Successfully removed: %s\n", key)
		} else {
			fmt.Printf("Failed to remove: %s\n", key)
		}
	}
}
