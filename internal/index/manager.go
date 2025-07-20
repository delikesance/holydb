package index

import (
"sync"
"fmt"
)


type IndexManager struct {
index      map[string]IndexEntry
bloom      *BloomFilter
mu         sync.RWMutex
}

// NewIndexManager creates an IndexManager with a Bloom filter
func NewIndexManager(config BloomFilterConfig) *IndexManager {
	return &IndexManager{
		index: make(map[string]IndexEntry),
		bloom: NewBloomFilter(config),
	}
}

func (im *IndexManager) Insert(entry IndexEntry) error {
    im.mu.Lock()
    defer im.mu.Unlock()
    key := fmt.Sprintf("%d", entry.KeyHash)
    im.index[key] = entry
    im.bloom.Add([]byte(key))
    return nil
}

func (im *IndexManager) Lookup(keyHash uint64) (IndexEntry, bool) {
    im.mu.RLock()
    defer im.mu.RUnlock()
    key := fmt.Sprintf("%d", keyHash)
    if !im.bloom.Contains([]byte(key)) {
        var empty IndexEntry
        return empty, false
    }
    entry, ok := im.index[key]
    return entry, ok
}

// Delete removes a key from the index (Bloom filter cannot remove)
func (im *IndexManager) Delete(key string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	delete(im.index, key)
	// Bloom filter does not support removal; may rebuild periodically if needed
}

// Stats returns Bloom filter statistics
func (im *IndexManager) Stats() BloomStats {
	return im.bloom.Stats()
}
