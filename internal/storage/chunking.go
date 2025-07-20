package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// Taille maximum d'un chunk (4MB par défaut)
	DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024
	// Taille minimum pour déclencher le chunking (8MB)
	MIN_CHUNKING_SIZE = 8 * 1024 * 1024
	// Nombre maximum de chunks par fichier
	MAX_CHUNKS = 65536
)

// ChunkMetadata contient les informations d'un chunk individuel
type ChunkMetadata struct {
	ChunkID   uint32 // ID du chunk (0, 1, 2, ...)
	Offset    uint64 // Position dans le segment
	Size      uint32 // Taille du chunk
	SegmentID uint32 // ID du segment contenant le chunk
	Checksum  uint32 // Checksum du chunk pour vérification
}

// FileMetadata contient les métadonnées d'un fichier chunké
type FileMetadata struct {
	OriginalSize uint64          // Taille totale du fichier original
	ChunkSize    uint32          // Taille des chunks utilisée
	ChunkCount   uint32          // Nombre total de chunks
	FileHash     [32]byte        // Hash SHA256 du fichier complet
	Chunks       []ChunkMetadata // Métadonnées de chaque chunk
}

// ChunkManager gère le chunking et dechunking des données
type ChunkManager struct {
	chunkSize uint32
}

// NewChunkManager crée un nouveau gestionnaire de chunks
func NewChunkManager(chunkSize uint32) *ChunkManager {
	if chunkSize == 0 {
		chunkSize = DEFAULT_CHUNK_SIZE
	}
	return &ChunkManager{
		chunkSize: chunkSize,
	}
}

// ShouldChunk détermine si les données doivent être chunkées
func (cm *ChunkManager) ShouldChunk(dataSize uint64) bool {
	return dataSize > MIN_CHUNKING_SIZE
}

// SplitIntoChunks divise les données en chunks
func (cm *ChunkManager) SplitIntoChunks(data []byte) ([][]byte, *FileMetadata, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("données vides")
	}

	// Calculer le hash du fichier complet
	fileHash := sha256.Sum256(data)
	
	// Calculer le nombre de chunks nécessaires
	chunkCount := uint32((uint64(len(data)) + uint64(cm.chunkSize) - 1) / uint64(cm.chunkSize))
	
	if chunkCount > MAX_CHUNKS {
		return nil, nil, fmt.Errorf("fichier trop gros: %d chunks nécessaires, maximum %d", chunkCount, MAX_CHUNKS)
	}

	chunks := make([][]byte, 0, chunkCount)
	metadata := &FileMetadata{
		OriginalSize: uint64(len(data)),
		ChunkSize:    cm.chunkSize,
		ChunkCount:   chunkCount,
		FileHash:     fileHash,
		Chunks:       make([]ChunkMetadata, 0, chunkCount),
	}

	// Diviser les données en chunks
	for i := uint32(0); i < chunkCount; i++ {
		start := uint64(i) * uint64(cm.chunkSize)
		end := start + uint64(cm.chunkSize)
		
		if end > uint64(len(data)) {
			end = uint64(len(data))
		}

		chunk := data[start:end]
		chunks = append(chunks, chunk)

		// Créer les métadonnées du chunk (offset et segmentID seront mis à jour lors du stockage)
		chunkMeta := ChunkMetadata{
			ChunkID:  i,
			Size:     uint32(len(chunk)),
			Checksum: calculateChunkChecksum(chunk),
		}
		metadata.Chunks = append(metadata.Chunks, chunkMeta)
	}

	return chunks, metadata, nil
}

// ReassembleChunks reconstruit le fichier original à partir des chunks
func (cm *ChunkManager) ReassembleChunks(chunks [][]byte, metadata *FileMetadata) ([]byte, error) {
	if metadata == nil {
		return nil, errors.New("métadonnées manquantes")
	}

	if len(chunks) != int(metadata.ChunkCount) {
		return nil, fmt.Errorf("nombre de chunks incorrect: attendu %d, reçu %d", metadata.ChunkCount, len(chunks))
	}

	// Vérifier l'ordre des chunks et leur intégrité
	for i, chunk := range chunks {
		expectedChecksum := metadata.Chunks[i].Checksum
		actualChecksum := calculateChunkChecksum(chunk)
		
		if actualChecksum != expectedChecksum {
			return nil, fmt.Errorf("checksum incorrect pour le chunk %d", i)
		}
	}

	// Réassembler les données
	result := make([]byte, 0, metadata.OriginalSize)
	for _, chunk := range chunks {
		result = append(result, chunk...)
	}

	// Vérifier le hash du fichier complet
	fileHash := sha256.Sum256(result)
	if fileHash != metadata.FileHash {
		return nil, errors.New("hash du fichier reconstruit incorrect")
	}

	return result, nil
}

// SerializeMetadata sérialise les métadonnées en binaire
func (metadata *FileMetadata) Serialize() []byte {
	// Calculer la taille nécessaire
	size := 8 + 4 + 4 + 32 + 4 + (len(metadata.Chunks) * 20) // ChunkMetadata = 20 bytes chacun
	result := make([]byte, size)
	
	offset := 0
	
	// OriginalSize (8 bytes)
	binary.LittleEndian.PutUint64(result[offset:], metadata.OriginalSize)
	offset += 8
	
	// ChunkSize (4 bytes)
	binary.LittleEndian.PutUint32(result[offset:], metadata.ChunkSize)
	offset += 4
	
	// ChunkCount (4 bytes)
	binary.LittleEndian.PutUint32(result[offset:], metadata.ChunkCount)
	offset += 4
	
	// FileHash (32 bytes)
	copy(result[offset:], metadata.FileHash[:])
	offset += 32
	
	// Nombre de chunks (4 bytes)
	binary.LittleEndian.PutUint32(result[offset:], uint32(len(metadata.Chunks)))
	offset += 4
	
	// Chunks metadata
	for _, chunk := range metadata.Chunks {
		binary.LittleEndian.PutUint32(result[offset:], chunk.ChunkID)
		offset += 4
		binary.LittleEndian.PutUint64(result[offset:], chunk.Offset)
		offset += 8
		binary.LittleEndian.PutUint32(result[offset:], chunk.Size)
		offset += 4
		binary.LittleEndian.PutUint32(result[offset:], chunk.SegmentID)
		offset += 4
	}
	
	return result
}

// DeserializeMetadata désérialise les métadonnées depuis le binaire
func DeserializeMetadata(data []byte) (*FileMetadata, error) {
	if len(data) < 48 { // Minimum sans chunks
		return nil, errors.New("données de métadonnées trop courtes")
	}
	
	offset := 0
	metadata := &FileMetadata{}
	
	// OriginalSize (8 bytes)
	metadata.OriginalSize = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	
	// ChunkSize (4 bytes)
	metadata.ChunkSize = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	
	// ChunkCount (4 bytes)
	metadata.ChunkCount = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	
	// FileHash (32 bytes)
	copy(metadata.FileHash[:], data[offset:])
	offset += 32
	
	// Nombre de chunks (4 bytes)
	chunkCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	
	// Vérifier que nous avons assez de données
	expectedSize := 48 + (int(chunkCount) * 20)
	if len(data) < expectedSize {
		return nil, errors.New("données de métadonnées incomplètes")
	}
	
	// Chunks metadata
	metadata.Chunks = make([]ChunkMetadata, chunkCount)
	for i := uint32(0); i < chunkCount; i++ {
		metadata.Chunks[i].ChunkID = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		metadata.Chunks[i].Offset = binary.LittleEndian.Uint64(data[offset:])
		offset += 8
		metadata.Chunks[i].Size = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		metadata.Chunks[i].SegmentID = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
	}
	
	return metadata, nil
}

// calculateChunkChecksum calcule le checksum d'un chunk
func calculateChunkChecksum(data []byte) uint32 {
	hash := sha256.Sum256(data)
	return binary.LittleEndian.Uint32(hash[:4])
}

// Erreurs spécifiques au chunking
var (
	ErrFileTooLarge    = errors.New("fichier trop volumineux pour le chunking")
	ErrInvalidChunk    = errors.New("chunk invalide")
	ErrChunkCorrupted  = errors.New("chunk corrompu")
	ErrMissingChunk    = errors.New("chunk manquant")
)
