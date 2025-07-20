package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

// IndexEntry représente une entrée d'index (dupliquée temporairement depuis index package)
type IndexEntry struct {
	KeyHash   uint64
	SegmentID uint32
	Offset    uint64
	Size      uint32
	Timestamp uint64
	Type      uint8
	Reserved  [31]byte
}

// DataType pour les types de données (dupliqué temporairement)
type DataType uint8

const (
	TypeKeyValue DataType = iota
	TypeDocument
	TypeFile
	TypeSchema
	TypeIndex
	TypeStream
	TypeGraph
	TypeTimeSeries
)

// FileManager gère le stockage et la récupération de fichiers avec chunking
type FileManager struct {
	chunkManager   *ChunkManager
	segmentManager *SegmentManager // À implémenter
	mu             sync.RWMutex
}

// NewFileManager crée un nouveau gestionnaire de fichiers
func NewFileManager(chunkSize uint32) *FileManager {
	return &FileManager{
		chunkManager: NewChunkManager(chunkSize),
	}
}

// StoreFile stocke un fichier, avec chunking automatique si nécessaire
func (fm *FileManager) StoreFile(key string, data []byte, segments []*Segment, index IndexInterface) (*FileMetadata, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Déterminer si le chunking est nécessaire
	if !fm.chunkManager.ShouldChunk(uint64(len(data))) {
		// Fichier petit, stockage direct
		return fm.storeSmallFile(key, data, segments[0], index)
	}

	// Fichier volumineux, chunking nécessaire
	return fm.storeLargeFile(key, data, segments, index)
}

// storeSmallFile stocke un fichier petit sans chunking
func (fm *FileManager) storeSmallFile(key string, data []byte, segment *Segment, index IndexInterface) (*FileMetadata, error) {
	keyHash := HashKey(key)
	
	// Stocker directement dans le segment
	offset, err := segment.WriteRecord(keyHash, data)
	if err != nil {
		return nil, fmt.Errorf("erreur lors de l'écriture: %w", err)
	}

	// Créer une métadonnée simple (pas de chunks)
	metadata := &FileMetadata{
		OriginalSize: uint64(len(data)),
		ChunkSize:    0, // Pas de chunking
		ChunkCount:   1,
		Chunks: []ChunkMetadata{{
			ChunkID:   0,
			Offset:    offset,
			Size:      uint32(len(data)),
			SegmentID: uint32(segment.ID),
		}},
	}

	// Mettre à jour l'index
	if err := index.Insert(IndexEntry{
		KeyHash:   keyHash,
		SegmentID: uint32(segment.ID),
		Offset:    offset,
		Size:      uint32(len(data)),
		Type:      uint8(TypeFile),
	}); err != nil {
		return nil, fmt.Errorf("erreur lors de la mise à jour de l'index: %w", err)
	}

	return metadata, nil
}

// storeLargeFile stocke un fichier volumineux avec chunking
func (fm *FileManager) storeLargeFile(key string, data []byte, segments []*Segment, index IndexInterface) (*FileMetadata, error) {
	keyHash := HashKey(key)

	// Découper en chunks
	chunks, metadata, err := fm.chunkManager.SplitIntoChunks(data)
	if err != nil {
		return nil, fmt.Errorf("erreur lors du chunking: %w", err)
	}

	// Stocker chaque chunk
	segmentIndex := 0
	for i, chunk := range chunks {
		// Trouver un segment avec assez d'espace
		for segmentIndex < len(segments) {
			segment := segments[segmentIndex]
			chunkSize := uint64(len(chunk)) + 13 // 4 + 8 + 1 bytes de header
			
			if segment.Size+chunkSize <= MAX_SEGMENT_SIZE {
				// Stocker le chunk
				chunkKeyHash := HashChunkKey(key, uint32(i))
				offset, err := segment.WriteChunk(chunkKeyHash, chunk, uint32(i))
				if err != nil {
					return nil, fmt.Errorf("erreur lors de l'écriture du chunk %d: %w", i, err)
				}

				// Mettre à jour les métadonnées du chunk
				metadata.Chunks[i].Offset = offset
				metadata.Chunks[i].SegmentID = uint32(segment.ID)

				// Mettre à jour l'index pour le chunk
				if err := index.Insert(IndexEntry{
					KeyHash:   chunkKeyHash,
					SegmentID: uint32(segment.ID),
					Offset:    offset,
					Size:      uint32(len(chunk)),
					Type:      uint8(TypeFile),
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

	// Stocker les métadonnées du fichier
	metadataKeyHash := HashMetadataKey(key)
	metadataSegment := segments[0] // Stocker les métadonnées dans le premier segment
	
	metadataOffset, err := metadataSegment.WriteMetadata(metadataKeyHash, metadata)
	if err != nil {
		return nil, fmt.Errorf("erreur lors de l'écriture des métadonnées: %w", err)
	}

	// Mettre à jour l'index pour les métadonnées
	metadataBytes := metadata.Serialize()
	if err := index.Insert(IndexEntry{
		KeyHash:   metadataKeyHash,
		SegmentID: uint32(metadataSegment.ID),
		Offset:    metadataOffset,
		Size:      uint32(len(metadataBytes)),
		Type:      uint8(TypeSchema), // Métadonnées stockées comme schéma
	}); err != nil {
		return nil, fmt.Errorf("erreur lors de la mise à jour de l'index pour les métadonnées: %w", err)
	}

	return metadata, nil
}

// RetrieveFile récupère un fichier, avec réassemblage automatique si chunké
func (fm *FileManager) RetrieveFile(key string, segments map[uint32]*Segment, index IndexInterface) ([]byte, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	// Essayer de récupérer les métadonnées du fichier
	metadataKeyHash := HashMetadataKey(key)
	metadataEntry, err := index.Lookup(metadataKeyHash)
	if err == nil {
		// Fichier chunké, récupérer via les métadonnées
		return fm.retrieveLargeFile(key, metadataEntry, segments, index)
	}

	// Pas de métadonnées trouvées, essayer récupération directe
	keyHash := HashKey(key)
	entry, err := index.Lookup(keyHash)
	if err != nil {
		return nil, fmt.Errorf("fichier non trouvé: %w", err)
	}

	// Récupérer directement depuis le segment
	segment, exists := segments[entry.SegmentID]
	if !exists {
		return nil, fmt.Errorf("segment %d non trouvé", entry.SegmentID)
	}

	return segment.ReadRecord(entry.Offset, entry.Size)
}

// retrieveLargeFile récupère et réassemble un fichier chunké
func (fm *FileManager) retrieveLargeFile(key string, metadataEntry *IndexEntry, segments map[uint32]*Segment, index IndexInterface) ([]byte, error) {
	// Récupérer les métadonnées
	metadataSegment, exists := segments[metadataEntry.SegmentID]
	if !exists {
		return nil, fmt.Errorf("segment de métadonnées %d non trouvé", metadataEntry.SegmentID)
	}

	metadata, err := metadataSegment.ReadMetadata(metadataEntry.Offset, metadataEntry.Size)
	if err != nil {
		return nil, fmt.Errorf("erreur lors de la lecture des métadonnées: %w", err)
	}

	// Récupérer chaque chunk
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

		chunks[i] = chunkData
	}

	// Réassembler le fichier
	return fm.chunkManager.ReassembleChunks(chunks, metadata)
}

// Interface pour l'index (à implémenter dans hashtable.go)
type IndexInterface interface {
	Insert(entry IndexEntry) error
	Lookup(keyHash uint64) (*IndexEntry, error)
}

// Fonctions utilitaires pour générer les clés de chunks et métadonnées
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
