package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"gopkg.in/yaml.v2"

	"github.com/delikesance/holydb/internal/index"
	"github.com/delikesance/holydb/internal/wal"
)

const (
	DefaultStoragePath = "/data/core"
	SegmentDirName     = "segments"
	SegmentFilePrefix  = "segment_"
	SegmentFileExt     = ".dat"
	SegmentSize        = 1 * 1024 * 1024 * 1024 // 1GB
	LargeFileThreshold = 8 * 1024 * 1024        // 8MB
	MaxDocumentSize    = 16 * 1024 * 1024       // 16MB
	DefaultWALFile     = "wal.log"
)

type StorageConfig struct {
	StoragePath string `yaml:"storage_path"`
}

type StorageManager struct {
	chunkManager *ChunkManager
	fileManager  *FileManager
	segments     map[uint64]*Segment
	indexManager *index.HashTable
	walManager   *wal.WAL
	config       StorageConfig
	mu           sync.RWMutex
}

// NewStorageManager initializes the manager with config from YAML (if present)
func NewStorageManager(configPath string) (*StorageManager, error) {
	config := StorageConfig{StoragePath: DefaultStoragePath}
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err == nil {
			_ = yaml.Unmarshal(data, &config)
		}
	}
	segmentDir := filepath.Join(config.StoragePath, SegmentDirName)
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		return nil, err
	}
	walPath := filepath.Join(config.StoragePath, DefaultWALFile)
	walManager, err := wal.NewWal(walPath)
	if err != nil {
		return nil, err
	}
	sm := &StorageManager{
		chunkManager: NewChunkManager(LargeFileThreshold),
		fileManager:  NewFileManager(LargeFileThreshold),
		segments:     make(map[uint64]*Segment),
		indexManager: index.NewHashTable(1024),
		walManager:   walManager,
		config:       config,
	}
	if err := sm.RestoreFromWAL(); err != nil {
		return nil, err
	}
	return sm, nil
}

func (sm *StorageManager) RestoreFromWAL() error {
	// Enumerate segment files
	segmentDir := filepath.Join(sm.config.StoragePath, SegmentDirName)
	files, err := os.ReadDir(segmentDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if len(name) >= len(SegmentFilePrefix)+6+len(SegmentFileExt) && name[:len(SegmentFilePrefix)] == SegmentFilePrefix {
			id := uint64(0)
			fmt.Sscanf(name[len(SegmentFilePrefix):len(SegmentFilePrefix)+6], "%06d", &id)
			path := filepath.Join(segmentDir, name)
			seg, err := NewSegment(id, path)
			if err == nil {
				sm.segments[id] = seg
			}
		}
	}

	// Replay WAL entries
	walPath := filepath.Join(sm.config.StoragePath, DefaultWALFile)
	f, err := os.Open(walPath)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		var entry wal.WALEntry
		err := binary.Read(f, binary.LittleEndian, &entry.Timestamp)
		if err != nil {
			break
		}
		err = binary.Read(f, binary.LittleEndian, &entry.OpType)
		if err != nil {
			break
		}
		err = binary.Read(f, binary.LittleEndian, &entry.KeyHash)
		if err != nil {
			break
		}
		err = binary.Read(f, binary.LittleEndian, &entry.DataSize)
		if err != nil {
			break
		}
		entry.Data = make([]byte, entry.DataSize)
		_, err = f.Read(entry.Data)
		if err != nil {
			break
		}
		err = binary.Read(f, binary.LittleEndian, &entry.Checksum)
		if err != nil {
			break
		}

		switch entry.OpType {
		case wal.OpPut, wal.OpUpdate:
			// Use FileManager to store file
			key := fmt.Sprintf("%d", entry.KeyHash)
			var segments []*Segment
for _, seg := range sm.segments {
    segments = append(segments, seg)
}
_, _ = sm.fileManager.StoreFile(key, entry.Data, segments, sm.indexManager)
		case wal.OpDelete:
			// Use FileManager to delete file (assume DeleteFile exists)
			key := fmt.Sprintf("%d", entry.KeyHash)
			if del, ok := interface{}(sm.fileManager).(interface{ DeleteFile(string) error }); ok {
				_ = del.DeleteFile(key)
			}
		case wal.OpCompact:
			// Skip or handle compaction
		}
	}
	return nil
}

func (sm *StorageManager) StoreFile(key string, data []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if len(data) > MaxDocumentSize {
		return errors.New("document exceeds maximum allowed size (16MB)")
	}
	keyHash := HashKey(key)
	walEntry := wal.NewWALEntry(wal.OpPut, keyHash, data)
	if err := sm.walManager.Append(walEntry); err != nil {
		return err
	}
	var segments []*Segment
	for _, seg := range sm.segments {
		segments = append(segments, seg)
	}
	_, err := sm.fileManager.StoreFile(key, data, segments, sm.indexManager)
	if err != nil {
		return err
	}

	return nil
}

// DeleteFile removes a file and logs to WAL
func (sm *StorageManager) DeleteFile(key string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	keyHash := HashKey(key)
	entry := wal.NewWALEntry(wal.OpDelete, keyHash, nil)
	if err := sm.walManager.Append(entry); err != nil {
		return err
	}
	if del, ok := interface{}(sm.fileManager).(interface{ DeleteFile(string) error }); ok {
		if err := del.DeleteFile(key); err != nil {
			return err
		}
	}
	sm.indexManager.Delete(HashKey(key))
	return nil
}

// UpdateFile deletes then stores a file
func (sm *StorageManager) UpdateFile(key string, data []byte) error {
	if err := sm.DeleteFile(key); err != nil {
		return err
	}
	return sm.StoreFile(key, data)
}

// DeleteData removes arbitrary data and logs to WAL
func (sm *StorageManager) DeleteData(key string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	keyHash := HashKey(key)
	entry := wal.NewWALEntry(wal.OpDelete, keyHash, nil)
	if err := sm.walManager.Append(entry); err != nil {
		return err
	}
	keyHash = HashKey(key)
	// Attempt to delete data from all segments (real impl should track location)
	for _, seg := range sm.segments {
		if del, ok := interface{}(seg).(interface{ DeleteRecord(uint64) error }); ok {
			_ = del.DeleteRecord(keyHash)
		}
	}
	sm.indexManager.Delete(HashKey(key))
	return nil
}

// UpdateData deletes then stores data
func (sm *StorageManager) UpdateData(key string, data []byte) error {
	if err := sm.DeleteData(key); err != nil {
		return err
	}
	return sm.StoreData(key, data)
}

// StoreData stores arbitrary data (not a file) and logs to WAL
func (sm *StorageManager) StoreData(key string, data []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if len(data) > MaxDocumentSize {
		return errors.New("data exceeds maximum allowed size (16MB)")
	}
	keyHash := HashKey(key)
	entry := wal.NewWALEntry(wal.OpPut, keyHash, data)
	if err := sm.walManager.Append(entry); err != nil {
		return err
	}
	// Store data in the first segment with enough space
	for segID, seg := range sm.segments {
		// Check if segment has enough space
		if hasSpace, ok := interface{}(seg).(interface{ HasSpace(size uint32) bool }); ok {
			if !hasSpace.HasSpace(uint32(len(data))) {
				continue
			}
		} else if segSize, ok := interface{}(seg).(interface{ CurrentSize() uint64 }); ok {
			if segSize.CurrentSize()+uint64(len(data)) > SegmentSize {
				continue
			}
		}
		offset, err := seg.WriteRecord(keyHash, data)
		if err != nil {
			return err
		}
		entry := index.IndexEntry{
			KeyHash:   keyHash,
			Size:      uint32(len(data)),
			SegmentID: uint32(segID),
			Offset:    offset,
			Timestamp: 0, // TODO: set actual timestamp if needed
		}
		sm.indexManager.Insert(entry)
		return nil
	}
	return errors.New("no segment with enough space available")
}

// RetrieveData retrieves arbitrary data (not a file)
func (sm *StorageManager) RetrieveData(key string) ([]byte, error) {
sm.mu.RLock()
defer sm.mu.RUnlock()
keyHash := HashKey(key)
entry, err := sm.indexManager.Lookup(keyHash)
if err != nil || entry == nil {
return nil, errors.New("data not found")
}
segment, ok := sm.segments[uint64(entry.SegmentID)]
if !ok {
return nil, errors.New("segment not found")
}
record, err := segment.ReadRecord(entry.Offset, entry.Size)
if err != nil {
return nil, err
}
return record.Data, nil
}

func (sm *StorageManager) RetrieveFile(key string) ([]byte, error) {
sm.mu.RLock()
defer sm.mu.RUnlock()
keyHash := HashKey(key)
entry, err := sm.indexManager.Lookup(keyHash)
if err != nil || entry == nil {
return nil, errors.New("file not found in index")
}
segment, ok := sm.segments[uint64(entry.SegmentID)]
if !ok {
return nil, errors.New("segment not found")
}
record, err := segment.ReadRecord(entry.Offset, entry.Size)
if err != nil {
return nil, err
}
return record.Data, nil
}

// AddSegment creates a new segment file
func (sm *StorageManager) AddSegment(id uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	segmentDir := filepath.Join(sm.config.StoragePath, SegmentDirName)
	segmentName := SegmentFilePrefix + formatSegmentID(id) + SegmentFileExt
	segmentPath := filepath.Join(segmentDir, segmentName)
	seg, err := NewSegment(id, segmentPath)
	if err != nil {
		return err
	}
	sm.segments[id] = seg
	return nil
}

// RemoveSegment removes a segment
func (sm *StorageManager) RemoveSegment(id uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.segments[id]
	if !ok {
		return errors.New("segment not found")
	}
	delete(sm.segments, id)
	return nil
}

// formatSegmentID formats segment ID as zero-padded string
func formatSegmentID(id uint64) string {
	return formatUint(id, 6)
}

// formatUint formats uint64 as zero-padded string
func formatUint(n uint64, width int) string {
	s := ""
	for i := 0; i < width; i++ {
		s = fmt.Sprint('0'+(n%10)) + s
		n /= 10
	}
	return s
}
