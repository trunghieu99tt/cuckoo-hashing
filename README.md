# Concurrent Cuckoo Hash Table

A thread-safe implementation of a Cuckoo Hash Table in Go, featuring automatic rehashing and concurrent operations.

## Overview

This implementation uses two hash tables with different hash functions to resolve collisions. When inserting a new item, if a collision occurs in the first table, the existing item is moved to the second table. This process continues until either an empty slot is found or the maximum number of moves is reached.

## Features

-   Thread-safe operations using read-write mutex
-   Automatic table resizing when load factor exceeds 0.5
-   Concurrent access support
-   Two independent hash functions (FNV-32a and FNV-64a)
-   Automatic collision resolution

## Usage

## Implementation Details

### Hash Functions

The implementation uses two different hash functions to minimize collisions:

-   FNV-32a for the first table
-   FNV-64a for the second table

### Rehashing Process

Rehashing is automatically triggered when:

1. Load factor exceeds 0.5
2. Insertion fails after maximum number of kicks

The rehashing process:

1. Creates new tables with double the size
2. Transfers all existing items to new tables
3. Rolls back if transfer fails
4. Supports concurrent rehashing with proper synchronization

### Thread Safety

Thread safety is achieved through:

1. Read-Write mutex for table operations
2. Atomic boolean for rehashing state
3. Safe concurrent access during rehashing

## Error Handling

The Insert operation returns false when:

-   Maximum kicks are reached without finding an empty slot
-   Rehashing fails to resolve the collision

## Performance Considerations

-   Load factor is kept below 0.5 to maintain performance
-   Maximum kicks is set to 2 \* table size to prevent infinite loops
-   Rehashing doubles the table size to amortize the cost

## License

MIT License
