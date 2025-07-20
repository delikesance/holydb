package index

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"
	"sync"

	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	bitArray []uint64
	size     uint64
	hashFunc []hash.Hash64
	numHash  uint
	count    uint64
	mu       sync.RWMutex
}

type BloomFilterConfig struct {
	ExpectedElements  uint64
	FalsePositiveRate float64
}

func DefaultBloomConfig() BloomFilterConfig {
	return BloomFilterConfig{
		ExpectedElements:  1000000,
		FalsePositiveRate: 0.01,
	}
}

func NewBloomFilter(config BloomFilterConfig) *BloomFilter {
	m := optimalBitArraySize(config.ExpectedElements, config.FalsePositiveRate)

	k := optimalHashFunctions(m, config.ExpectedElements)

	hashFuncs := make([]hash.Hash64, k)
	for i := uint(0); i < k; i++ {
		if i == 0 {
			hashFuncs[i] = murmur3.New64WithSeed(uint32(i))
		} else {
			hashFuncs[i] = fnv.New64a()
		}
	}

	return &BloomFilter{
		bitArray: make([]uint64, (m+63)/64),
		size:     m,
		hashFunc: hashFuncs,
		numHash:  k,
		count:    0,
	}
}

func (bf *BloomFilter) Add(key []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i, hashFunc := range bf.hashFunc {
		hashFunc.Reset()
		hashFunc.Write(key)
		if i > 0 {
			binary.Write(hashFunc, binary.LittleEndian, uint64(i))
		}

		hash := hashFunc.Sum64()
		bitIndex := hash % bf.size

		arrayIndex := bitIndex / 64
		bitOffset := bitIndex % 64
		bf.bitArray[arrayIndex] |= (1 << bitOffset)
	}

	bf.count++
}

func (bf *BloomFilter) AddHash(keyHash uint64) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := uint(0); i < bf.numHash; i++ {
		hash := keyHash + uint64(i)*0x9e3779b9
		bitIndex := hash % bf.size

		arrayIndex := bitIndex / 64
		bitOffset := bitIndex % 64
		bf.bitArray[arrayIndex] |= (1 << bitOffset)
	}

	bf.count++
}

func (bf *BloomFilter) Contains(key []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for i, hashFunc := range bf.hashFunc {
		hashFunc.Reset()
		hashFunc.Write(key)
		if i > 0 {
			binary.Write(hashFunc, binary.LittleEndian, uint64(i))
		}

		hash := hashFunc.Sum64()
		bitIndex := hash % bf.size

		arrayIndex := bitIndex / 64
		bitOffset := bitIndex % 64

		if (bf.bitArray[arrayIndex] & (1 << bitOffset)) == 0 {
			return false
		}
	}

	return true
}

func (bf *BloomFilter) ContainsHash(keyHash uint64) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for i := uint(0); i < bf.numHash; i++ {
		hash := keyHash + uint64(i)*0x9e3779b9
		bitIndex := hash % bf.size

		arrayIndex := bitIndex / 64
		bitOffset := bitIndex % 64

		if (bf.bitArray[arrayIndex] & (1 << bitOffset)) == 0 {
			return false
		}
	}

	return true
}

func (bf *BloomFilter) Stats() BloomStats {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setBits := uint64(0)
	for _, word := range bf.bitArray {
		setBits += uint64(popcount(word))
	}

	fillRatio := float64(setBits) / float64(bf.size)
	currentFPR := math.Pow(fillRatio, float64(bf.numHash))

	return BloomStats{
		Size:              bf.size,
		BitsSet:           setBits,
		FillRatio:         fillRatio,
		EstimatedElements: bf.count,
		NumHashFunctions:  bf.numHash,
		FalsePositiveRate: currentFPR,
	}
}

type BloomStats struct {
	Size              uint64
	BitsSet           uint64
	FillRatio         float64
	EstimatedElements uint64
	NumHashFunctions  uint
	FalsePositiveRate float64
}

func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := range bf.bitArray {
		bf.bitArray[i] = 0
	}
	bf.count = 0
}

func (bf *BloomFilter) Serialize() []byte {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	headerSize := 28
	dataSize := len(bf.bitArray) * 8
	result := make([]byte, headerSize+dataSize)

	offset := 0

	binary.LittleEndian.PutUint32(result[offset:], 1)
	offset += 4

	binary.LittleEndian.PutUint64(result[offset:], bf.size)
	offset += 8

	binary.LittleEndian.PutUint32(result[offset:], uint32(bf.numHash))
	offset += 4

	binary.LittleEndian.PutUint64(result[offset:], bf.count)
	offset += 8

	binary.LittleEndian.PutUint32(result[offset:], uint32(len(bf.bitArray)))
	offset += 4

	for _, word := range bf.bitArray {
		binary.LittleEndian.PutUint64(result[offset:], word)
		offset += 8
	}

	return result
}

func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 28 {
		return nil, ErrInvalidBloomData
	}

	offset := 0

	version := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	if version != 1 {
		return nil, ErrUnsupportedBloomVersion
	}

	size := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	numHash := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	count := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	arrayLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if len(data) < offset+int(arrayLen)*8 {
		return nil, ErrInvalidBloomData
	}

	bitArray := make([]uint64, arrayLen)
	for i := uint32(0); i < arrayLen; i++ {
		bitArray[i] = binary.LittleEndian.Uint64(data[offset:])
		offset += 8
	}

	hashFuncs := make([]hash.Hash64, numHash)
	for i := uint32(0); i < numHash; i++ {
		if i == 0 {
			hashFuncs[i] = murmur3.New64WithSeed(i)
		} else {
			hashFuncs[i] = fnv.New64a()
		}
	}

	return &BloomFilter{
		bitArray: bitArray,
		size:     size,
		hashFunc: hashFuncs,
		numHash:  uint(numHash),
		count:    count,
	}, nil
}

func optimalBitArraySize(n uint64, p float64) uint64 {
	return uint64(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
}

func optimalHashFunctions(m, n uint64) uint {
	k := float64(m) / float64(n) * math.Log(2)
	return uint(math.Ceil(k))
}

func popcount(x uint64) int {
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}

var (
	ErrInvalidBloomData        = NewBloomError("données de bloom filter invalides")
	ErrUnsupportedBloomVersion = NewBloomError("version de bloom filter non supportée")
)

type BloomError struct {
	message string
}

func (e BloomError) Error() string {
	return e.message
}

func NewBloomError(message string) BloomError {
	return BloomError{message: message}
}
