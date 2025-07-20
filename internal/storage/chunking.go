package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024
	MIN_CHUNKING_SIZE  = 8 * 1024 * 1024
	MAX_CHUNKS         = 65536
)

type ChunkMetadata struct {
	ChunkID   uint32
	Offset    uint64
	Size      uint32
	SegmentID uint32
	Checksum  uint32
}

type FileMetadata struct {
	OriginalSize uint64
	ChunkSize    uint32
	ChunkCount   uint32
	FileHash     [32]byte
	Chunks       []ChunkMetadata
}

type ChunkManager struct {
	chunkSize uint32
}

func NewChunkManager(chunkSize uint32) *ChunkManager {
	if chunkSize == 0 {
		chunkSize = DEFAULT_CHUNK_SIZE
	}
	return &ChunkManager{
		chunkSize: chunkSize,
	}
}

func (cm *ChunkManager) ShouldChunk(dataSize uint64) bool {
	return dataSize > MIN_CHUNKING_SIZE
}

func (cm *ChunkManager) SplitIntoChunks(data []byte) ([][]byte, *FileMetadata, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("données vides")
	}

	fileHash := sha256.Sum256(data)

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

	for i := uint32(0); i < chunkCount; i++ {
		start := uint64(i) * uint64(cm.chunkSize)
		end := start + uint64(cm.chunkSize)

		if end > uint64(len(data)) {
			end = uint64(len(data))
		}

		chunk := data[start:end]
		chunks = append(chunks, chunk)

		chunkMeta := ChunkMetadata{
			ChunkID:  i,
			Size:     uint32(len(chunk)),
			Checksum: calculateChunkChecksum(chunk),
		}
		metadata.Chunks = append(metadata.Chunks, chunkMeta)
	}

	return chunks, metadata, nil
}

func (cm *ChunkManager) ReassembleChunks(chunks [][]byte, metadata *FileMetadata) ([]byte, error) {
	if metadata == nil {
		return nil, errors.New("métadonnées manquantes")
	}

	if len(chunks) != int(metadata.ChunkCount) {
		return nil, fmt.Errorf("nombre de chunks incorrect: attendu %d, reçu %d", metadata.ChunkCount, len(chunks))
	}

	for i, chunk := range chunks {
		expectedChecksum := metadata.Chunks[i].Checksum
		actualChecksum := calculateChunkChecksum(chunk)

		if actualChecksum != expectedChecksum {
			return nil, fmt.Errorf("checksum incorrect pour le chunk %d", i)
		}
	}

	result := make([]byte, 0, metadata.OriginalSize)
	for _, chunk := range chunks {
		result = append(result, chunk...)
	}

	fileHash := sha256.Sum256(result)
	if fileHash != metadata.FileHash {
		return nil, errors.New("hash du fichier reconstruit incorrect")
	}

	return result, nil
}

func (metadata *FileMetadata) Serialize() []byte {
	size := 8 + 4 + 4 + 32 + 4 + (len(metadata.Chunks) * 20)
	result := make([]byte, size)

	offset := 0

	binary.LittleEndian.PutUint64(result[offset:], metadata.OriginalSize)
	offset += 8

	binary.LittleEndian.PutUint32(result[offset:], metadata.ChunkSize)
	offset += 4

	binary.LittleEndian.PutUint32(result[offset:], metadata.ChunkCount)
	offset += 4

	copy(result[offset:], metadata.FileHash[:])
	offset += 32

	binary.LittleEndian.PutUint32(result[offset:], uint32(len(metadata.Chunks)))
	offset += 4

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

func DeserializeMetadata(data []byte) (*FileMetadata, error) {
	if len(data) < 48 {
		return nil, errors.New("données de métadonnées trop courtes")
	}

	offset := 0
	metadata := &FileMetadata{}

	metadata.OriginalSize = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	metadata.ChunkSize = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	metadata.ChunkCount = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	copy(metadata.FileHash[:], data[offset:])
	offset += 32

	chunkCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	expectedSize := 48 + (int(chunkCount) * 20)
	if len(data) < expectedSize {
		return nil, errors.New("données de métadonnées incomplètes")
	}

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

func calculateChunkChecksum(data []byte) uint32 {
	hash := sha256.Sum256(data)
	return binary.LittleEndian.Uint32(hash[:4])
}

var (
	ErrFileTooLarge   = errors.New("fichier trop volumineux pour le chunking")
	ErrInvalidChunk   = errors.New("chunk invalide")
	ErrChunkCorrupted = errors.New("chunk corrompu")
	ErrMissingChunk   = errors.New("chunk manquant")
)
