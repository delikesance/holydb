package index

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

const (
	INDEX_ENTRY_SIZE = 32
	LOAD_FACTOR      = 0.75
)

type IndexEntry struct {
	KeyHash   uint64
	SegmentID uint32
	Offset    uint64
	Size      uint32
	Timestamp uint64
}

type HashTable struct {
	buckets     [][]IndexEntry
	size        uint64
	count       uint64
	loadFactor  float64
	bloom       *BloomFilter  // Bloom filter pour optimiser les lookups
	mu          sync.RWMutex
}

func NewHashTable(initialSize uint64) *HashTable {
	// Configuration du bloom filter basée sur la taille attendue
	bloomConfig := BloomFilterConfig{
		ExpectedElements:  initialSize * 4, // 4x la taille initiale
		FalsePositiveRate: 0.01,           // 1% de faux positifs
	}
	
	return &HashTable{
		buckets:     make([][]IndexEntry, initialSize),
		size:        initialSize,
		loadFactor:  LOAD_FACTOR,
		bloom:       NewBloomFilter(bloomConfig),
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
			return nil // Pas besoin de re-ajouter au bloom filter
		}
	}

	ht.buckets[bucket] = append(ht.buckets[bucket], entry)
	ht.count++
	
	// Ajouter au bloom filter pour optimiser les futures recherches
	ht.bloom.AddHash(entry.KeyHash)
	
	return nil
}

func (ht *HashTable) Lookup(keyHash uint64) (*IndexEntry, error) {
	// Première vérification: bloom filter (très rapide)
	if !ht.bloom.ContainsHash(keyHash) {
		// Si le bloom filter dit "absent", c'est définitivement absent
		return nil, ErrNotFound
	}
	
	// Le bloom filter dit "peut-être présent", on fait le lookup réel
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	bucket := ht.hash(keyHash)

	for _, entry := range ht.buckets[bucket] {
		if entry.KeyHash == keyHash {
			return &entry, nil
		}
	}

	// C'était un faux positif du bloom filter
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
			// Note: on ne peut pas supprimer du bloom filter (pas supporté par design)
			// Cela peut causer quelques faux positifs après suppression, mais c'est acceptable
			return nil
		}
	}

	return ErrNotFound
}

func (ht *HashTable) resize() {
	oldBuckets := ht.buckets
	ht.size *= 2
	ht.buckets = make([][]IndexEntry, ht.size)
	newCount := uint64(0)

	// Recréer le bloom filter avec la nouvelle taille
	bloomConfig := BloomFilterConfig{
		ExpectedElements:  ht.size * 4,
		FalsePositiveRate: 0.01,
	}
	ht.bloom = NewBloomFilter(bloomConfig)

	// Réinsérer toutes les entrées
	for _, bucket := range oldBuckets {
		for _, entry := range bucket {
			newBucket := ht.hash(entry.KeyHash)
			ht.buckets[newBucket] = append(ht.buckets[newBucket], entry)
			// Re-ajouter au nouveau bloom filter
			ht.bloom.AddHash(entry.KeyHash)
			newCount++
		}
	}
	
	// Mettre à jour le compteur seulement après réinsertion complète
	ht.count = newCount
}

func HashKey(key string) uint64 {
	hash := sha256.Sum256([]byte(key))
	return binary.LittleEndian.Uint64(hash[:8])
}

var ErrNotFound = errors.New("key not found")

// GetBloomStats retourne les statistiques du bloom filter
func (ht *HashTable) GetBloomStats() BloomStats {
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	return ht.bloom.Stats()
}

// NewIndexEntry crée une nouvelle entrée d'index
func NewIndexEntry(keyHash uint64, segmentID uint32, offset uint64, size uint32) IndexEntry {
	return IndexEntry{
		KeyHash:   keyHash,
		SegmentID: segmentID,
		Offset:    offset,
		Size:      size,
		Timestamp: uint64(time.Now().UnixNano()),
	}
}
