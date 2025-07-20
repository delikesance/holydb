package index

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

const (
	INDEX_ENTRY_SIZE = 64
	LOAD_FACTOR      = 0.75
)

// Types de données supportés par le système de stockage universel
type DataType uint8

const (
	TypeKeyValue DataType = iota // Données clé-valeur simples
	TypeDocument                 // Documents JSON, XML, YAML
	TypeFile                     // Fichiers binaires, images, vidéos
	TypeSchema                   // Métadonnées et définitions de schéma
	TypeIndex                    // Index secondaires et métadonnées d'index
	TypeStream                   // Données de streaming/logs
	TypeGraph                    // Données de graphe/relations
	TypeTimeSeries              // Données temporelles/métriques
)

type IndexEntry struct {
	KeyHash   uint64
	SegmentID uint32
	Offset    uint64
	Size      uint32
	Timestamp uint64
	Type      uint8     // Utilise DataType
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

// LookupByType retourne toutes les entrées d'un type spécifique
func (ht *HashTable) LookupByType(dataType DataType) []IndexEntry {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	var results []IndexEntry
	for _, bucket := range ht.buckets {
		for _, entry := range bucket {
			if DataType(entry.Type) == dataType {
				results = append(results, entry)
			}
		}
	}
	return results
}

// CountByType retourne le nombre d'entrées par type
func (ht *HashTable) CountByType() map[DataType]uint64 {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	counts := make(map[DataType]uint64)
	for _, bucket := range ht.buckets {
		for _, entry := range bucket {
			dataType := DataType(entry.Type)
			counts[dataType]++
		}
	}
	return counts
}

func (ht *HashTable) resize() {
	oldBuckets := ht.buckets
	ht.size *= 2
	ht.buckets = make([][]IndexEntry, ht.size)
	newCount := uint64(0)

	// Réinsérer toutes les entrées
	for _, bucket := range oldBuckets {
		for _, entry := range bucket {
			newBucket := ht.hash(entry.KeyHash)
			ht.buckets[newBucket] = append(ht.buckets[newBucket], entry)
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

// Fonctions utilitaires pour les types de données
func (dt DataType) String() string {
	switch dt {
	case TypeKeyValue:
		return "KeyValue"
	case TypeDocument:
		return "Document"
	case TypeFile:
		return "File"
	case TypeSchema:
		return "Schema"
	case TypeIndex:
		return "Index"
	case TypeStream:
		return "Stream"
	case TypeGraph:
		return "Graph"
	case TypeTimeSeries:
		return "TimeSeries"
	default:
		return "Unknown"
	}
}

// Vérifie si un type nécessite une compression spéciale
func (dt DataType) NeedsCompression() bool {
	switch dt {
	case TypeDocument, TypeStream, TypeTimeSeries:
		return true // Données textuelles/répétitives
	case TypeFile:
		return false // Fichiers potentiellement déjà compressés
	default:
		return true
	}
}

// Vérifie si un type supporte le chunking pour gros volumes
func (dt DataType) SupportsChunking() bool {
	switch dt {
	case TypeFile, TypeStream, TypeTimeSeries:
		return true
	default:
		return false
	}
}

var ErrNotFound = errors.New("key not found")

// NewIndexEntry crée une nouvelle entrée d'index avec le type spécifié
func NewIndexEntry(keyHash uint64, segmentID uint32, offset uint64, size uint32, dataType DataType) IndexEntry {
	return IndexEntry{
		KeyHash:   keyHash,
		SegmentID: segmentID,
		Offset:    offset,
		Size:      size,
		Timestamp: uint64(time.Now().UnixNano()),
		Type:      uint8(dataType),
	}
}
