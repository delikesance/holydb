package wal

import (
	"bufio"
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
	mu         sync.Mutex
}

func NewWal(filepath string) (*WAL, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:       file,
		writer:     bufio.NewWriter(file),
		syncTicker: time.NewTicker(time.Second),
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
	for range w.syncTicker.C {
		w.Sync()
	}
}

func (w *WAL) calculateChecksum(entry WALEntry) uint32 {
	return crc32.ChecksumIEEE(entry.Data)
}

func (w *WAL) Close() error {
	w.syncTicker.Stop()
	w.Sync()
	return w.file.Close()
}
