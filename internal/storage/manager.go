package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
			if _, err := fmt.Sscanf(name[len(SegmentFilePrefix):len(SegmentFilePrefix)+6], "%06d", &id); err != nil {
				fmt.Printf("Warning: failed to parse segment ID from filename %s: %v\n", name, err)
				continue
			}
			path := filepath.Join(segmentDir, name)
			seg, err := NewSegment(id, path)
			if err != nil {
				fmt.Printf("Warning: failed to open segment %s: %v\n", path, err)
				continue
			}
			sm.segments[id] = seg
		}
	}

	// Replay WAL entries
	walPath := filepath.Join(sm.config.StoragePath, DefaultWALFile)
	f, err := os.Open(walPath)
	if err != nil {
		// If WAL file doesn't exist, it's not an error - just means no recovery needed
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer f.Close()
	
	entryCount := 0
	for {
		var entry wal.WALEntry
		err := binary.Read(f, binary.LittleEndian, &entry.Timestamp)
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			return fmt.Errorf("failed to read WAL entry timestamp at entry %d: %w", entryCount, err)
		}
		err = binary.Read(f, binary.LittleEndian, &entry.OpType)
		if err != nil {
			return fmt.Errorf("failed to read WAL entry optype at entry %d: %w", entryCount, err)
		}
		err = binary.Read(f, binary.LittleEndian, &entry.KeyHash)
		if err != nil {
			return fmt.Errorf("failed to read WAL entry keyhash at entry %d: %w", entryCount, err)
		}
		err = binary.Read(f, binary.LittleEndian, &entry.DataSize)
		if err != nil {
			return fmt.Errorf("failed to read WAL entry datasize at entry %d: %w", entryCount, err)
		}
		
		// Validate data size to prevent potential memory issues
		if entry.DataSize > MaxDocumentSize {
			return fmt.Errorf("WAL entry %d has invalid data size %d (exceeds max %d)", entryCount, entry.DataSize, MaxDocumentSize)
		}
		
		entry.Data = make([]byte, entry.DataSize)
		n, err := f.Read(entry.Data)
		if err != nil {
			return fmt.Errorf("failed to read WAL entry data at entry %d: %w", entryCount, err)
		}
		if n != int(entry.DataSize) {
			return fmt.Errorf("WAL entry %d: incomplete data read (expected %d, got %d)", entryCount, entry.DataSize, n)
		}
		if err != nil {
			break
		}
		err = binary.Read(f, binary.LittleEndian, &entry.Checksum)
		if err != nil {
			return fmt.Errorf("failed to read WAL entry checksum at entry %d: %w", entryCount, err)
		}

		// TODO: Validate checksum here to ensure data integrity

		switch entry.OpType {
		case wal.OpPut, wal.OpUpdate:
			// Restore data using the same logic as StoreData (without WAL logging)
			key := fmt.Sprintf("%d", entry.KeyHash)
			
			// Store data in the first segment with enough space
			stored := false
			for segID, seg := range sm.segments {
				// Check if segment has enough space
				if hasSpace, ok := interface{}(seg).(interface{ HasSpace(size uint32) bool }); ok {
					if !hasSpace.HasSpace(uint32(len(entry.Data))) {
						continue
					}
				} else if segSize, ok := interface{}(seg).(interface{ CurrentSize() uint64 }); ok {
					if segSize.CurrentSize()+uint64(len(entry.Data)) > SegmentSize {
						continue
					}
				}
				offset, err := seg.WriteRecord(entry.KeyHash, entry.Data)
				if err != nil {
					fmt.Printf("Error during WAL recovery - failed to write record %s: %v\n", key, err)
					continue
				}
				indexEntry := index.IndexEntry{
					KeyHash:   entry.KeyHash,
					Size:      uint32(len(entry.Data)),
					SegmentID: uint32(segID),
					Offset:    offset,
					Timestamp: entry.Timestamp,
				}
				if err := sm.indexManager.Insert(indexEntry); err != nil {
					fmt.Printf("Error during WAL recovery - failed to insert into index %s: %v\n", key, err)
					continue
				}
				stored = true
				break
			}
			if !stored {
				fmt.Printf("Error during WAL recovery - no segment with enough space for key %s\n", key)
			}
		case wal.OpDelete:
			// Use FileManager to delete file (assume DeleteFile exists)
			key := fmt.Sprintf("%d", entry.KeyHash)
			if del, ok := interface{}(sm.fileManager).(interface{ DeleteFile(string) error }); ok {
				if err := del.DeleteFile(key); err != nil {
					// Log error but continue recovery process
					fmt.Printf("Error during WAL recovery - failed to delete file %s: %v\n", key, err)
				}
			}
		case wal.OpCompact:
			// Skip or handle compaction
		}
		
		entryCount++
	}
	
	fmt.Printf("WAL recovery completed: processed %d entries\n", entryCount)
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
			return fmt.Errorf("failed to delete file from file manager: %w", err)
		}
	}
	if err := sm.indexManager.Delete(HashKey(key)); err != nil {
		return fmt.Errorf("failed to delete from index: %w", err)
	}
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
	var lastError error
	for _, seg := range sm.segments {
		if del, ok := interface{}(seg).(interface{ DeleteRecord(uint64) error }); ok {
			if err := del.DeleteRecord(keyHash); err != nil {
				lastError = err // Keep track of the last error but continue
			}
		}
	}
	if err := sm.indexManager.Delete(HashKey(key)); err != nil {
		return fmt.Errorf("failed to delete from index: %w", err)
	}
	// Return the last segment deletion error if any occurred
	return lastError
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
		if err := sm.indexManager.Insert(entry); err != nil {
			return fmt.Errorf("failed to insert into index: %w", err)
		}
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
