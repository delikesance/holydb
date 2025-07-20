package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

const (
	MAGIC_NUMBER     = 0x434F5245 // CORE
	VERSION          = 1
	HEADER_SIZE      = 64
	MAX_SEGMENT_SIZE = 1 << 30 // 1GB
)

type SegmentHeader struct {
	Magic       uint32
	Version     uint32
	SegmentID   uint64
	Timestamp   uint64
	RecordCount uint32
	DataSize    uint64
	Checksum    uint32
	Reserved    [24]byte
}

type Record struct {
	Size    uint32
	KeyHash uint64
	Data    []byte
}

type Segment struct {
	ID     uint64
	File   *os.File
	Header SegmentHeader
	Size   uint64
	mu     sync.RWMutex
}

func NewSegment(id uint64, filepath string) (*Segment, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	segment := &Segment{
		ID:   id,
		File: file,
		Header: SegmentHeader{
			Magic:     MAGIC_NUMBER,
			Version:   VERSION,
			SegmentID: id,
			Timestamp: uint64(time.Now().Unix()),
		},
	}

	if err := segment.writeHeader(); err != nil {
		return nil, err
	}

	segment.Size = HEADER_SIZE
	return segment, nil
}

func (s *Segment) WriteRecord(keyHash uint64, data []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := Record{
		Size:    uint32(len(data)),
		KeyHash: keyHash,
		Data:    data,
	}

	recordSize := 4 + 8 + uint64(len(data)) // Size + KeyHash + Data
	if s.Size+recordSize > MAX_SEGMENT_SIZE {
		return 0, ErrSegmentFull
	}

	offset := s.Size
	if err := binary.Write(s.File, binary.LittleEndian, record.Size); err != nil {
		return 0, err
	}

	if err := binary.Write(s.File, binary.LittleEndian, record.KeyHash); err != nil {
		return 0, err
	}

	if _, err := s.File.Write(record.Data); err != nil {
		return 0, err
	}

	s.Size += recordSize
	s.Header.RecordCount++
	s.Header.DataSize += recordSize

	if err := s.updateHeader(); err != nil {
		return 0, err
	}

	return offset, nil
}

func (s *Segment) ReadRecord(offset uint64, size uint32) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Se positionner au début du record
	if _, err := s.File.Seek(int64(offset), 0); err != nil {
		return nil, err
	}

	// Lire les métadonnées du record
	var recordSize uint32
	var keyHash uint64

	if err := binary.Read(s.File, binary.LittleEndian, &recordSize); err != nil {
		return nil, err
	}

	if err := binary.Read(s.File, binary.LittleEndian, &keyHash); err != nil {
		return nil, err
	}

	// Lire les données
	data := make([]byte, recordSize)
	if _, err := s.File.Read(data); err != nil {
		return nil, err
	}

	return &Record{
		Size:    recordSize,
		KeyHash: keyHash,
		Data:    data,
	}, nil
}

func (s *Segment) writeHeader() error {
	if _, err := s.File.Seek(0, 0); err != nil {
		return err
	}

	return binary.Write(s.File, binary.LittleEndian, s.Header)
}

func (s *Segment) updateHeader() error {
	s.Header.Checksum = s.calculateChecksum()
	return s.writeHeader()
}

func (s *Segment) calculateChecksum() uint32 {
	// Calculer CRC32 du header (sans le checksum lui-même)
	headerBytes := make([]byte, 36) // Magic(4) + Version(4) + SegmentID(8) + Timestamp(8) + RecordCount(4) + DataSize(8)
	
	binary.LittleEndian.PutUint32(headerBytes[0:4], s.Header.Magic)
	binary.LittleEndian.PutUint32(headerBytes[4:8], s.Header.Version)
	binary.LittleEndian.PutUint64(headerBytes[8:16], s.Header.SegmentID)
	binary.LittleEndian.PutUint64(headerBytes[16:24], s.Header.Timestamp)
	binary.LittleEndian.PutUint32(headerBytes[24:28], s.Header.RecordCount)
	binary.LittleEndian.PutUint64(headerBytes[28:36], s.Header.DataSize)
	
	return crc32.ChecksumIEEE(headerBytes)
}

var ErrSegmentFull = errors.New("segment is full")
