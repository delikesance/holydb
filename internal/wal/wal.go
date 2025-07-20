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

type OpType uint8

const (
	OpPut OpType = iota
	OpDelete
	OpUpdate   // Mise à jour d'une entrée existante
	OpCompact  // Opération de compaction
)

type WALEntry struct {
	Timestamp uint64
	OpType    OpType
	KeyHash   uint64
	DataSize  uint32
	Data      []byte
	Checksum  uint32
}

type WAL struct {
	file       *os.File
	writer     *bufio.Writer
	syncTicker *time.Ticker
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
}

func NewWal(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	wal := &WAL{
		file:       file,
		writer:     bufio.NewWriter(file),
		syncTicker: time.NewTicker(time.Second),
		ctx:        ctx,
		cancel:     cancel,
	}

	go wal.periodicSync()
	return wal, nil
}

func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry.Timestamp = uint64(time.Now().UnixNano())
	entry.Checksum = w.calculateChecksum(entry)

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

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) periodicSync() {
	for {
		select {
		case <-w.syncTicker.C:
			w.Sync()
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WAL) calculateChecksum(entry WALEntry) uint32 {
	return crc32.ChecksumIEEE(entry.Data)
}

func (w *WAL) Close() error {
	w.cancel() // Arrêter la goroutine
	w.syncTicker.Stop()
	w.Sync()
	return w.file.Close()
}

// NewWALEntry crée une nouvelle entrée WAL
func NewWALEntry(opType OpType, keyHash uint64, data []byte) WALEntry {
	return WALEntry{
		OpType:   opType,
		KeyHash:  keyHash,
		DataSize: uint32(len(data)),
		Data:     data,
	}
}
