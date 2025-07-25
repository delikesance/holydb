package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

// OpType represents type of operation logged in WAL.
type OpType uint8

const (
	OpPut OpType = iota
	OpDelete
	OpUpdate
	OpCompact
)

// WALEntry represents a single WAL log entry.
type WALEntry struct {
	Timestamp uint64 // Unix nanoseconds timestamp of operation.
	OpType    OpType
	KeyHash   uint64
	DataSize  uint32
	Data      []byte
	Checksum  uint32
}

// WAL encapsulates the Write-Ahead Log.
type WAL struct {
	file       *os.File
	writer     *bufio.Writer
	syncTicker *time.Ticker
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex // Protects writer and file.
}

// NewWal creates or opens a WAL file at given path.
func NewWal(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	wal := &WAL{
		file:       file,
		writer:     bufio.NewWriter(file),
		syncTicker: time.NewTicker(1 * time.Second),
		ctx:        ctx,
		cancel:     cancel,
	}
	go wal.periodicSync()
	return wal, nil
}

// Append writes a WALEntry to the log, calculating checksum automatically.
func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry.Timestamp = uint64(time.Now().UnixNano())
	entry.Checksum = w.calculateChecksum(entry)

	// Write fields in order: Timestamp, OpType, KeyHash, DataSize, Data bytes, Checksum.
	if err := binary.Write(w.writer, binary.LittleEndian, entry.Timestamp); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, entry.OpType); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, entry.KeyHash); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, entry.DataSize); err != nil {
		return err
	}
	if _, err := w.writer.Write(entry.Data); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, entry.Checksum); err != nil {
		return err
	}
	return nil
}

// Sync flushes buffered data and syncs it to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// periodicSync is a background goroutine periodically syncing the WAL.
func (w *WAL) periodicSync() {
	for {
		select {
		case <-w.syncTicker.C:
			_ = w.Sync() // Ignore sync error, optionally log.
		case <-w.ctx.Done():
			return
		}
	}
}

// calculateChecksum returns a CRC32 checksum of the entry's Data field.
func (w *WAL) calculateChecksum(entry WALEntry) uint32 {
	return crc32.ChecksumIEEE(entry.Data)
}

// Close stops the periodic sync and closes the file.
func (w *WAL) Close() error {
	w.cancel()
	w.syncTicker.Stop()

	if err := w.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

// NewWALEntry constructs a WALEntry with given details.
func NewWALEntry(opType OpType, keyHash uint64, data []byte) WALEntry {
	return WALEntry{
		OpType:   opType,
		KeyHash:  keyHash,
		DataSize: uint32(len(data)),
		Data:     data,
	}
}
