package index

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
)

const (
	INDEX_ENTRY_SIZE = 64
	LOAD_FACTOR      = 0.75
)

type IndexEntry struct {
	KeyHash   uint64
	SegmentID uint32
	Offset    uint64
	Size      uint32
	Timestamp uint64
	Type      uint8
	Reserved  [31]byte
}

type HashTable struct {
	buckets    [][]IndexEntry
	size       uint64
	count      uint64
	loadFactor float64
	mu         sync.RWMutex
}

func NewHashTable(initialSize uint64) *HashTable {
	return &HashTable{
		buckets:    make([][]IndexEntry, initialSize),
		size:       initialSize,
		loadFactor: LOAD_FACTOR,
	}
}

func (ht *HashTable) hash(keyHash uint64) uint64 {
	return keyHash % ht.size
}

func (ht *HashTable) Insert(entry IndexEntry) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if float64(ht.count)/float64(ht.size) > ht.loadFactor {
		ht.resize()
	}

	bucket := ht.hash(entry.KeyHash)

	for i, existing := range ht.buckets[bucket] {
		if existing.KeyHash == entry.KeyHash {
			ht.buckets[bucket][i] = entry
			return nil
		}
	}

	ht.buckets[bucket] = append(ht.buckets[bucket], entry)
	ht.count++
	return nil
}

func (ht *HashTable) Lookup(keyHash uint64) (*IndexEntry, error) {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	bucket := ht.hash(keyHash)

	for _, entry := range ht.buckets[bucket] {
		if entry.KeyHash == keyHash {
			return &entry, nil
		}
	}

	return nil, ErrNotFound
}

func (ht *HashTable) Delete(keyHash uint64) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	bucket := ht.hash(keyHash)

	for i, entry := range ht.buckets[bucket] {
		if entry.KeyHash == keyHash {
			ht.buckets[bucket] = append(
				ht.buckets[bucket][:i],
				ht.buckets[bucket][i+1:]...,
			)
			ht.count--
			return nil
		}
	}

	return ErrNotFound
}

func (ht *HashTable) resize() {
	oldBuckets := ht.buckets
	ht.size *= 2
	ht.buckets = make([][]IndexEntry, ht.size)
	ht.count = 0

	// Réinsérer toutes les entrées
	for _, bucket := range oldBuckets {
		for _, entry := range bucket {
			newBucket := ht.hash(entry.KeyHash)
			ht.buckets[newBucket] = append(ht.buckets[newBucket], entry)
			ht.count++
		}
	}
}

func HashKey(key string) uint64 {
	hash := sha256.Sum256([]byte(key))
	return binary.LittleEndian.Uint64(hash[:8])
}

var ErrNotFound = errors.New("key not found")
