package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	indexpkg "github.com/delikesance/holydb/internal/index"
)

type FileManager struct {
	chunkManager *ChunkManager
	mu           sync.RWMutex
}

func NewFileManager(chunkSize uint32) *FileManager {
	return &FileManager{
		chunkManager: NewChunkManager(chunkSize),
	}
}

func (fm *FileManager) StoreFile(key string, data []byte, segments []*Segment, index indexpkg.IndexInterface) (*FileMetadata, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if !fm.chunkManager.ShouldChunk(uint64(len(data))) {
		return fm.storeSmallFile(key, data, segments[0], index)
	}

	return fm.storeLargeFile(key, data, segments, index)
}

func (fm *FileManager) storeSmallFile(key string, data []byte, segment *Segment, index indexpkg.IndexInterface) (*FileMetadata, error) {
	keyHash := HashKey(key)

	offset, err := segment.WriteRecord(keyHash, data)
	if err != nil {
		return nil, fmt.Errorf("erreur lors de l'écriture: %w", err)
	}

	metadata := &FileMetadata{
		OriginalSize: uint64(len(data)),
		ChunkSize:    0,
		ChunkCount:   1,
		Chunks: []ChunkMetadata{{
			ChunkID:   0,
			Offset:    offset,
			Size:      uint32(len(data)),
			SegmentID: uint32(segment.ID),
		}},
	}

	if err := index.Insert(indexpkg.IndexEntry{
		KeyHash:   keyHash,
		SegmentID: uint32(segment.ID),
		Offset:    offset,
		Size:      uint32(len(data)),
		Timestamp: 0,
	}); err != nil {
		return nil, fmt.Errorf("erreur lors de la mise à jour de l'index: %w", err)
	}

	return metadata, nil
}

func (fm *FileManager) storeLargeFile(key string, data []byte, segments []*Segment, index indexpkg.IndexInterface) (*FileMetadata, error) {
	chunks, metadata, err := fm.chunkManager.SplitIntoChunks(data)
	if err != nil {
		return nil, fmt.Errorf("erreur lors du chunking: %w", err)
	}

	segmentIndex := 0
	for i, chunk := range chunks {
		for segmentIndex < len(segments) {
			segment := segments[segmentIndex]
			chunkSize := uint64(len(chunk)) + 12

			if segment.Size+chunkSize <= MAX_SEGMENT_SIZE {
				chunkKeyHash := HashChunkKey(key, uint32(i))
				offset, err := segment.WriteRecord(chunkKeyHash, chunk)
				if err != nil {
					return nil, fmt.Errorf("erreur lors de l'écriture du chunk %d: %w", i, err)
				}

				metadata.Chunks[i].Offset = offset
				metadata.Chunks[i].SegmentID = uint32(segment.ID)
				if err := index.Insert(indexpkg.IndexEntry{
					KeyHash:   chunkKeyHash,
					SegmentID: uint32(segment.ID),
					Offset:    offset,
					Size:      uint32(len(chunk)),
					Timestamp: 0,
				}); err != nil {
					return nil, fmt.Errorf("erreur lors de la mise à jour de l'index pour le chunk %d: %w", i, err)
				}

				break
			}
			segmentIndex++
		}

		if segmentIndex >= len(segments) {
			return nil, errors.New("pas assez de segments disponibles pour stocker tous les chunks")
		}
	}

	metadataKeyHash := HashMetadataKey(key)
	metadataSegment := segments[0]

	metadataBytes := metadata.Serialize()
	metadataOffset, err := metadataSegment.WriteRecord(metadataKeyHash, metadataBytes)
	if err != nil {
		return nil, fmt.Errorf("erreur lors de l'écriture des métadonnées: %w", err)
	}

	if err := index.Insert(indexpkg.IndexEntry{
		KeyHash:   metadataKeyHash,
		SegmentID: uint32(metadataSegment.ID),
		Offset:    metadataOffset,
		Size:      uint32(len(metadataBytes)),
		Timestamp: 0,
	}); err != nil {
		return nil, fmt.Errorf("erreur lors de la mise à jour de l'index pour les métadonnées: %w", err)
	}

	return metadata, nil
}

func (fm *FileManager) RetrieveFile(key string, segments map[uint32]*Segment, index indexpkg.IndexInterface) ([]byte, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	metadataKeyHash := HashMetadataKey(key)
	metadataEntry, err := index.Lookup(metadataKeyHash)
	if err == nil {
		return fm.retrieveLargeFile(key, metadataEntry, segments, index)
	}

	keyHash := HashKey(key)
	entry, err := index.Lookup(keyHash)
	if err != nil {
		return nil, fmt.Errorf("fichier non trouvé: %w", err)
	}

	segment, exists := segments[entry.SegmentID]
	if !exists {
		return nil, fmt.Errorf("segment %d non trouvé", entry.SegmentID)
	}

	record, err := segment.ReadRecord(entry.Offset, entry.Size)
	if err != nil {
		return nil, err
	}

	return record.Data, nil
}

func (fm *FileManager) retrieveLargeFile(key string, metadataEntry *indexpkg.IndexEntry, segments map[uint32]*Segment, index indexpkg.IndexInterface) ([]byte, error) {
	metadataSegment, exists := segments[metadataEntry.SegmentID]
	if !exists {
		return nil, fmt.Errorf("segment de métadonnées %d non trouvé", metadataEntry.SegmentID)
	}

	metadataRecord, err := metadataSegment.ReadRecord(metadataEntry.Offset, metadataEntry.Size)
	if err != nil {
		return nil, fmt.Errorf("erreur lors de la lecture des métadonnées: %w", err)
	}

	metadata, err := DeserializeMetadata(metadataRecord.Data)
	if err != nil {
		return nil, fmt.Errorf("erreur lors de la désérialisation des métadonnées: %w", err)
	}

	chunks := make([][]byte, metadata.ChunkCount)
	for i := uint32(0); i < metadata.ChunkCount; i++ {
		chunkKeyHash := HashChunkKey(key, i)
		chunkEntry, err := index.Lookup(chunkKeyHash)
		if err != nil {
			return nil, fmt.Errorf("chunk %d non trouvé: %w", i, err)
		}

		chunkSegment, exists := segments[chunkEntry.SegmentID]
		if !exists {
			return nil, fmt.Errorf("segment %d pour le chunk %d non trouvé", chunkEntry.SegmentID, i)
		}

		chunkData, err := chunkSegment.ReadRecord(chunkEntry.Offset, chunkEntry.Size)
		if err != nil {
			return nil, fmt.Errorf("erreur lors de la lecture du chunk %d: %w", i, err)
		}

		chunks[i] = chunkData.Data
	}

	return fm.chunkManager.ReassembleChunks(chunks, metadata)
}

func HashKey(key string) uint64 {
	hash := sha256.Sum256([]byte(key))
	return binary.LittleEndian.Uint64(hash[:8])
}

func HashChunkKey(key string, chunkID uint32) uint64 {
	chunkKey := fmt.Sprintf("%s#chunk_%d", key, chunkID)
	return HashKey(chunkKey)
}

func HashMetadataKey(key string) uint64 {
	metadataKey := fmt.Sprintf("%s#metadata", key)
	return HashKey(metadataKey)
}
