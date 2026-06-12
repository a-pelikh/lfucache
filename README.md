# lfucache

Generic LFU cache for Go.

`lfucache` stores the most frequently used keys and evicts the least frequently
used entry when capacity is reached. If several entries have the same frequency,
the oldest entry in that frequency bucket is evicted first.

The implementation is built around hash maps and linked frequency buckets. Hot
paths reuse detached cache nodes and frequency-list nodes where possible, keeping
updates predictable and allocation-light.

## Install

```sh
go get github.com/a-pelikh/lfucache
```

## Usage

```go
package main

import (
	"errors"
	"fmt"

	"github.com/a-pelikh/lfucache"
)

func main() {
	cache := lfucache.New[string, int](2)

	cache.Put("one", 1)
	cache.Put("two", 2)

	value, err := cache.Get("one")
	if err != nil {
		panic(err)
	}

	fmt.Println(value) // 1

	cache.Put("three", 3) // evicts "two": it has lower frequency than "one"

	_, err = cache.Get("two")
	fmt.Println(errors.Is(err, lfucache.ErrKeyNotFound)) // true
}
```

## API

```go
type Cache[K comparable, V any] interface {
	Get(key K) (V, error)
	Put(key K, value V)
	All() iter.Seq2[K, V]
	Size() int
	Capacity() int
	GetKeyFrequency(key K) (int, error)
}
```

### `New`

```go
cache := lfucache.New[string, int](100)
```

If capacity is omitted, `DefaultCapacity` is used. Non-positive capacity panics.

### `Get`

Returns the value for a key and increments its frequency.

```go
value, err := cache.Get("key")
if errors.Is(err, lfucache.ErrKeyNotFound) {
	// missing key
}
```

### `Put`

Adds or updates a value.

```go
cache.Put("key", 42)
```

Updating an existing key also increments its frequency. Adding a new key to a
full cache evicts one LFU entry.

### `All`

Iterates entries from highest frequency to lowest frequency. Within the same
frequency bucket, newer entries are returned first.

```go
for key, value := range cache.All() {
	fmt.Println(key, value)
}
```

The iterator uses a snapshot, so the cache lock is not held while user code runs
inside the loop body.

### `Size` and `Capacity`

```go
fmt.Println(cache.Size())
fmt.Println(cache.Capacity())
```

`Size` returns the current number of cached entries. `Capacity` returns the fixed
maximum size configured at construction time.

### `GetKeyFrequency`

Returns the current frequency counter for a key.

```go
freq, err := cache.GetKeyFrequency("key")
```

This method is mainly useful for tests, diagnostics, and observability.

## Concurrency

Cache methods are guarded by a mutex. `Get` and `Put` take the write lock because
both can modify frequency state. Read-style methods use read locking where the
implementation can do so safely.

`All` snapshots entries under a read lock and then yields them after unlocking,
which avoids deadlocks when code inside the loop calls back into the cache.

## Behavior

- `Get` increments frequency.
- `Put` on an existing key updates the value and increments frequency.
- `Put` on a new key starts with frequency `1`.
- Eviction chooses the lowest frequency bucket.
- Ties are resolved by removing the oldest entry in that bucket.
- Missing keys return `ErrKeyNotFound`.

## Tests

Regular package compile:

```sh
go test ./...
```

Functional model tests:

```sh
go test -tags model_test ./...
```

Performance and allocation tests:

```sh
go test -tags performance_test ./...
```
