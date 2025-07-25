package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

const (
	MAGIC_NUMBER     = 0x484F4C59 // "HOLY" in hex
	VERSION          = 1
	HEADER_SIZE      = 64
	MAX_SEGMENT_SIZE = 256 * 1024 * 1024 // 256MB as per README
	MIN_RECORD_SIZE  = 12                // Size + KeyHash + minimal data
)

// SegmentHeader represents the fixed 64-byte header of each segment file.
type SegmentHeader struct {
	Magic       uint32   // Magic number for format validation
	Version     uint32   // Format version
	SegmentID   uint64   // Unique segment identifier
	Timestamp   uint64   // Creation timestamp (Unix nanoseconds)
	RecordCount uint32   // Number of records in segment
	DataSize    uint64   // Total size of data (excluding header)
	Checksum    uint32   // CRC32 checksum of header (excluding this field)
	Reserved    [24]byte // Reserved for future extensions
}

// Record represents a single data record within a segment.
type Record struct {
	Size    uint32 // Size of the data portion
	KeyHash uint64 // Hash of the key for this record
	Data    []byte // The actual data
}

// Segment represents an append-only segment file with concurrent access support.
type Segment struct {
	ID     uint64
	Path   string
	File   *os.File
	Header SegmentHeader
	Size   uint64 // Current size including header
	mu     sync.RWMutex
}

// NewSegment creates a new segment file or opens an existing one.
func NewSegment(id uint64, filepath string) (*Segment, error) {
	if id == 0 {
		return nil, errors.New("segment ID cannot be zero")
	}

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file %s: %w", filepath, err)
	}

	segment := &Segment{
		ID:   id,
		Path: filepath,
		File: file,
		Header: SegmentHeader{
			Magic:     MAGIC_NUMBER,
			Version:   VERSION,
			SegmentID: id,
			Timestamp: uint64(time.Now().UnixNano()),
		},
	}

	// Check if file exists and has content
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat segment file: %w", err)
	}

	if stat.Size() == 0 {
		// New file - write header
		if err := segment.writeHeader(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to write initial header: %w", err)
		}
		segment.Size = HEADER_SIZE
	} else {
		// Existing file - read and validate header
		if err := segment.readAndValidateHeader(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read/validate header: %w", err)
		}
		segment.Size = uint64(stat.Size())
	}

	return segment, nil
}

// WriteRecord appends a new record to the segment.
// Returns the offset where the record was written.
func (s *Segment) WriteRecord(keyHash uint64, data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, errors.New("cannot write empty data")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record := Record{
		Size:    uint32(len(data)),
		KeyHash: keyHash,
		Data:    data,
	}

	recordSize := 4 + 8 + uint64(len(data)) // Size + KeyHash + Data

	// Check if segment has enough space
	if s.Size+recordSize > MAX_SEGMENT_SIZE {
		return 0, ErrSegmentFull
	}

	offset := s.Size

	// Seek to end of segment
	if _, err := s.File.Seek(int64(offset), 0); err != nil {
		return 0, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}

	// Write record size
	if err := binary.Write(s.File, binary.LittleEndian, record.Size); err != nil {
		return 0, fmt.Errorf("failed to write record size: %w", err)
	}

	// Write key hash
	if err := binary.Write(s.File, binary.LittleEndian, record.KeyHash); err != nil {
		return 0, fmt.Errorf("failed to write key hash: %w", err)
	}

	// Write data
	if _, err := s.File.Write(record.Data); err != nil {
		return 0, fmt.Errorf("failed to write record data: %w", err)
	}

	// Update segment metadata
	s.Size += recordSize
	s.Header.RecordCount++
	s.Header.DataSize += recordSize

	// Update header on disk
	if err := s.updateHeader(); err != nil {
		return 0, fmt.Errorf("failed to update header after write: %w", err)
	}

	// Ensure data is written to disk
	if err := s.File.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync segment file: %w", err)
	}

	return offset, nil
}

// ReadRecord reads a record from the segment at the specified offset.
func (s *Segment) ReadRecord(offset uint64, expectedSize uint32) (*Record, error) {
	if offset < HEADER_SIZE {
		return nil, errors.New("invalid offset: cannot read from header area")
	}

	if expectedSize == 0 {
		return nil, errors.New("expected size cannot be zero")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Validate offset is within segment bounds
	if offset >= s.Size {
		return nil, fmt.Errorf("offset %d exceeds segment size %d", offset, s.Size)
	}

	// Seek to record position
	if _, err := s.File.Seek(int64(offset), 0); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}

	// Read record size
	var recordSize uint32
	if err := binary.Read(s.File, binary.LittleEndian, &recordSize); err != nil {
		return nil, fmt.Errorf("failed to read record size: %w", err)
	}

	// Validate record size
	if recordSize != expectedSize {
		return nil, fmt.Errorf("record size mismatch: expected %d, got %d", expectedSize, recordSize)
	}

	if recordSize > MAX_SEGMENT_SIZE {
		return nil, fmt.Errorf("invalid record size %d", recordSize)
	}

	// Read key hash
	var keyHash uint64
	if err := binary.Read(s.File, binary.LittleEndian, &keyHash); err != nil {
		return nil, fmt.Errorf("failed to read key hash: %w", err)
	}

	// Read data
	data := make([]byte, recordSize)
	if n, err := s.File.Read(data); err != nil {
		return nil, fmt.Errorf("failed to read record data: %w", err)
	} else if n != int(recordSize) {
		return nil, fmt.Errorf("incomplete read: expected %d bytes, got %d", recordSize, n)
	}

	return &Record{
		Size:    recordSize,
		KeyHash: keyHash,
		Data:    data,
	}, nil
}

// GetStats returns current segment statistics.
func (s *Segment) GetStats() SegmentStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return SegmentStats{
		ID:          s.ID,
		Size:        s.Size,
		RecordCount: s.Header.RecordCount,
		DataSize:    s.Header.DataSize,
		FreeSpace:   MAX_SEGMENT_SIZE - s.Size,
		Utilization: float64(s.Size) / float64(MAX_SEGMENT_SIZE),
		CreatedAt:   time.Unix(0, int64(s.Header.Timestamp)),
	}
}

// SegmentStats contains statistical information about a segment.
type SegmentStats struct {
	ID          uint64
	Size        uint64
	RecordCount uint32
	DataSize    uint64
	FreeSpace   uint64
	Utilization float64
	CreatedAt   time.Time
}

// IsFull returns true if the segment cannot accommodate more data.
func (s *Segment) IsFull(additionalSize uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Size+additionalSize > MAX_SEGMENT_SIZE
}

// Close closes the segment file and ensures all data is synced.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.File == nil {
		return nil // Already closed
	}

	// Final sync before closing
	if err := s.File.Sync(); err != nil {
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	if err := s.File.Close(); err != nil {
		return fmt.Errorf("failed to close segment file: %w", err)
	}

	s.File = nil
	return nil
}

// readAndValidateHeader reads the segment header and validates it.
func (s *Segment) readAndValidateHeader() error {
	if _, err := s.File.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to header: %w", err)
	}

	if err := binary.Read(s.File, binary.LittleEndian, &s.Header); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Validate magic number
	if s.Header.Magic != MAGIC_NUMBER {
		return fmt.Errorf("invalid magic number: expected %x, got %x", MAGIC_NUMBER, s.Header.Magic)
	}

	// Validate version
	if s.Header.Version != VERSION {
		return fmt.Errorf("unsupported version: expected %d, got %d", VERSION, s.Header.Version)
	}

	// Validate segment ID matches
	if s.Header.SegmentID != s.ID {
		return fmt.Errorf("segment ID mismatch: expected %d, got %d", s.ID, s.Header.SegmentID)
	}

	// TODO: Validate checksum
	expectedChecksum := s.calculateChecksum()
	if s.Header.Checksum != expectedChecksum {
		return fmt.Errorf("header checksum mismatch: expected %x, got %x", expectedChecksum, s.Header.Checksum)
	}

	return nil
}

// writeHeader writes the segment header to disk.
func (s *Segment) writeHeader() error {
	if _, err := s.File.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to header position: %w", err)
	}

	// Calculate checksum before writing
	s.Header.Checksum = s.calculateChecksum()

	if err := binary.Write(s.File, binary.LittleEndian, s.Header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	return nil
}

// updateHeader updates the segment header on disk.
func (s *Segment) updateHeader() error {
	s.Header.Checksum = s.calculateChecksum()
	return s.writeHeader()
}

// calculateChecksum computes CRC32 checksum of header fields (excluding checksum itself).
func (s *Segment) calculateChecksum() uint32 {
	// Create a byte array with all header fields except checksum
	headerBytes := make([]byte, 36) // All fields before checksum field

	binary.LittleEndian.PutUint32(headerBytes[0:4], s.Header.Magic)
	binary.LittleEndian.PutUint32(headerBytes[4:8], s.Header.Version)
	binary.LittleEndian.PutUint64(headerBytes[8:16], s.Header.SegmentID)
	binary.LittleEndian.PutUint64(headerBytes[16:24], s.Header.Timestamp)
	binary.LittleEndian.PutUint32(headerBytes[24:28], s.Header.RecordCount)
	binary.LittleEndian.PutUint64(headerBytes[28:36], s.Header.DataSize)

	return crc32.ChecksumIEEE(headerBytes)
}

// Error definitions
var (
	ErrSegmentFull     = errors.New("segment is full")
	ErrInvalidOffset   = errors.New("invalid offset")
	ErrInvalidRecord   = errors.New("invalid record")
	ErrCorruptedHeader = errors.New("corrupted segment header")
)
