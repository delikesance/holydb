package index

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"
	"sync"

	"github.com/spaolacci/murmur3"
)

// BloomFilter implémente un filtre de Bloom pour éviter les I/O inutiles
type BloomFilter struct {
	bitArray []uint64     // Array de bits compact (64 bits par élément)
	size     uint64       // Taille du bit array en bits
	hashFunc []hash.Hash64 // Fonctions de hachage
	numHash  uint         // Nombre de fonctions de hachage
	count    uint64       // Nombre approximatif d'éléments ajoutés
	mu       sync.RWMutex // Protège les opérations concurrentes
}

// BloomFilterConfig contient la configuration du bloom filter
type BloomFilterConfig struct {
	ExpectedElements uint64  // Nombre d'éléments attendus
	FalsePositiveRate float64 // Taux de faux positifs souhaité (ex: 0.01 = 1%)
}

// DefaultBloomConfig retourne une configuration par défaut
func DefaultBloomConfig() BloomFilterConfig {
	return BloomFilterConfig{
		ExpectedElements:  1000000, // 1M d'éléments
		FalsePositiveRate: 0.01,    // 1% de faux positifs
	}
}

// NewBloomFilter crée un nouveau filtre de Bloom optimisé
func NewBloomFilter(config BloomFilterConfig) *BloomFilter {
	// Calculer la taille optimale du bit array
	m := optimalBitArraySize(config.ExpectedElements, config.FalsePositiveRate)
	
	// Calculer le nombre optimal de fonctions de hachage
	k := optimalHashFunctions(m, config.ExpectedElements)
	
	// Créer les fonctions de hachage
	hashFuncs := make([]hash.Hash64, k)
	for i := uint(0); i < k; i++ {
		if i == 0 {
			hashFuncs[i] = murmur3.New64WithSeed(uint32(i))
		} else {
			hashFuncs[i] = fnv.New64a()
		}
	}
	
	return &BloomFilter{
		bitArray: make([]uint64, (m+63)/64), // Arrondir au multiple de 64
		size:     m,
		hashFunc: hashFuncs,
		numHash:  k,
		count:    0,
	}
}

// Add ajoute un élément au filtre de Bloom
func (bf *BloomFilter) Add(key []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	
	for i, hashFunc := range bf.hashFunc {
		hashFunc.Reset()
		hashFunc.Write(key)
		if i > 0 {
			// Ajouter un salt pour différencier les hash functions
			binary.Write(hashFunc, binary.LittleEndian, uint64(i))
		}
		
		hash := hashFunc.Sum64()
		bitIndex := hash % bf.size
		
		// Définir le bit correspondant
		arrayIndex := bitIndex / 64
		bitOffset := bitIndex % 64
		bf.bitArray[arrayIndex] |= (1 << bitOffset)
	}
	
	bf.count++
}

// AddHash ajoute un hash de clé directement (plus efficace si on a déjà le hash)
func (bf *BloomFilter) AddHash(keyHash uint64) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	
	for i := uint(0); i < bf.numHash; i++ {
		// Générer différents hash en combinant avec l'index de la fonction
		hash := keyHash + uint64(i)*0x9e3779b9 // Constante de multiplication dorée
		bitIndex := hash % bf.size
		
		arrayIndex := bitIndex / 64
		bitOffset := bitIndex % 64
		bf.bitArray[arrayIndex] |= (1 << bitOffset)
	}
	
	bf.count++
}

// Contains vérifie si un élément pourrait être dans le set
// Retourne false si définitivement absent, true si potentiellement présent
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
			return false // Définitivement absent
		}
	}
	
	return true // Potentiellement présent
}

// ContainsHash vérifie un hash de clé directement
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

// Stats retourne les statistiques du filtre
func (bf *BloomFilter) Stats() BloomStats {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	
	// Compter les bits définis
	setBits := uint64(0)
	for _, word := range bf.bitArray {
		setBits += uint64(popcount(word))
	}
	
	// Calculer le taux de faux positifs actuel
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

// BloomStats contient les statistiques du bloom filter
type BloomStats struct {
	Size              uint64  // Taille totale en bits
	BitsSet           uint64  // Nombre de bits définis
	FillRatio         float64 // Ratio de remplissage (0.0 à 1.0)
	EstimatedElements uint64  // Nombre d'éléments ajoutés
	NumHashFunctions  uint    // Nombre de fonctions de hachage
	FalsePositiveRate float64 // Taux de faux positifs estimé
}

// Clear remet le filtre à zéro
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	
	for i := range bf.bitArray {
		bf.bitArray[i] = 0
	}
	bf.count = 0
}

// Serialize sérialise le bloom filter pour la persistance
func (bf *BloomFilter) Serialize() []byte {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	
	// Header: version(4) + size(8) + numHash(4) + count(8) + arrayLen(4)
	headerSize := 28
	dataSize := len(bf.bitArray) * 8
	result := make([]byte, headerSize+dataSize)
	
	offset := 0
	
	// Version (pour compatibilité future)
	binary.LittleEndian.PutUint32(result[offset:], 1)
	offset += 4
	
	// Taille du bit array
	binary.LittleEndian.PutUint64(result[offset:], bf.size)
	offset += 8
	
	// Nombre de fonctions de hachage
	binary.LittleEndian.PutUint32(result[offset:], uint32(bf.numHash))
	offset += 4
	
	// Nombre d'éléments
	binary.LittleEndian.PutUint64(result[offset:], bf.count)
	offset += 8
	
	// Longueur du tableau
	binary.LittleEndian.PutUint32(result[offset:], uint32(len(bf.bitArray)))
	offset += 4
	
	// Données du bit array
	for _, word := range bf.bitArray {
		binary.LittleEndian.PutUint64(result[offset:], word)
		offset += 8
	}
	
	return result
}

// Deserialize charge un bloom filter depuis les données sérialisées
func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 28 {
		return nil, ErrInvalidBloomData
	}
	
	offset := 0
	
	// Version
	version := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	if version != 1 {
		return nil, ErrUnsupportedBloomVersion
	}
	
	// Taille
	size := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	
	// Nombre de hash functions
	numHash := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	
	// Count
	count := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	
	// Longueur du tableau
	arrayLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	
	// Vérifier que nous avons assez de données
	if len(data) < offset+int(arrayLen)*8 {
		return nil, ErrInvalidBloomData
	}
	
	// Lire le bit array
	bitArray := make([]uint64, arrayLen)
	for i := uint32(0); i < arrayLen; i++ {
		bitArray[i] = binary.LittleEndian.Uint64(data[offset:])
		offset += 8
	}
	
	// Recréer les fonctions de hachage
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

// Fonctions utilitaires

// optimalBitArraySize calcule la taille optimale du bit array
func optimalBitArraySize(n uint64, p float64) uint64 {
	// m = -n * ln(p) / (ln(2)^2)
	return uint64(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
}

// optimalHashFunctions calcule le nombre optimal de fonctions de hachage
func optimalHashFunctions(m, n uint64) uint {
	// k = (m/n) * ln(2)
	k := float64(m) / float64(n) * math.Log(2)
	return uint(math.Ceil(k))
}

// popcount compte le nombre de bits définis (population count)
func popcount(x uint64) int {
	count := 0
	for x != 0 {
		count++
		x &= x - 1 // Supprime le bit le plus bas
	}
	return count
}

// Erreurs
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
