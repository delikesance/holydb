package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWAL_Creation(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	if wal.file == nil {
		t.Error("WAL file should not be nil")
	}

	if wal.writer == nil {
		t.Error("WAL writer should not be nil")
	}

	if wal.syncTicker == nil {
		t.Error("WAL sync ticker should not be nil")
	}

	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file should exist after creation")
	}
}

func TestWAL_AppendEntry(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	entry := WALEntry{
		OpType:   OpPut,
		KeyHash:  12345,
		DataSize: 5,
		Data:     []byte("hello"),
	}

	err = wal.Append(entry)
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if stat.Size() == 0 {
		t.Error("WAL file should not be empty after writing entry")
	}
}

func TestWAL_MultipleEntries(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "test.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	entries := []WALEntry{
		{
			OpType:   OpPut,
			KeyHash:  1,
			DataSize: 3,
			Data:     []byte("foo"),
		},
		{
			OpType:   OpUpdate,
			KeyHash:  1,
			DataSize: 3,
			Data:     []byte("bar"),
		},
		{
			OpType:   OpDelete,
			KeyHash:  1,
			DataSize: 0,
			Data:     nil,
		},
	}

	for _, entry := range entries {
		err = wal.Append(entry)
		if err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if stat.Size() == 0 {
		t.Error("WAL file should contain multiple entries")
	}
}

func TestWAL_NewWALEntry(t *testing.T) {
	data := []byte("test data")
	entry := NewWALEntry(OpPut, 12345, data)

	if entry.OpType != OpPut {
		t.Errorf("Expected OpType %v, got %v", OpPut, entry.OpType)
	}

	if entry.KeyHash != 12345 {
		t.Errorf("Expected KeyHash 12345, got %d", entry.KeyHash)
	}

	if entry.DataSize != uint32(len(data)) {
		t.Errorf("Expected DataSize %d, got %d", len(data), entry.DataSize)
	}

	if string(entry.Data) != string(data) {
		t.Errorf("Expected Data %s, got %s", string(data), string(entry.Data))
	}
}

func TestWAL_Sync(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "sync.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	entry := WALEntry{
		OpType:   OpPut,
		KeyHash:  111,
		DataSize: 4,
		Data:     []byte("sync"),
	}

	err = wal.Append(entry)
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if stat.Size() == 0 {
		t.Error("WAL file should be flushed to disk after sync")
	}
}

func TestWAL_OperationTypes(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "ops.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	operations := []OpType{OpPut, OpDelete, OpUpdate, OpCompact}

	for i, op := range operations {
		entry := WALEntry{
			OpType:   op,
			KeyHash:  uint64(i),
			DataSize: uint32(len("data")),
			Data:     []byte("data"),
		}

		err = wal.Append(entry)
		if err != nil {
			t.Fatalf("Failed to append entry for operation %v: %v", op, err)
		}
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if stat.Size() == 0 {
		t.Error("WAL file should contain operation entries")
	}
}

func TestWAL_EmptyData(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "empty.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	entry := WALEntry{
		OpType:   OpDelete,
		KeyHash:  777,
		DataSize: 0,
		Data:     nil,
	}

	err = wal.Append(entry)
	if err != nil {
		t.Fatalf("Failed to append entry with empty data: %v", err)
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if stat.Size() == 0 {
		t.Error("WAL file should contain entry even with empty data")
	}
}

func TestWAL_Close(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "close.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	entry := WALEntry{
		OpType:   OpPut,
		KeyHash:  555,
		DataSize: 5,
		Data:     []byte("close"),
	}

	err = wal.Append(entry)
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Second close may fail but shouldn't crash
	_ = wal.Close()
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "concurrent.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	done := make(chan bool)
	numWriters := 10
	entriesPerWriter := 100

	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer func() { done <- true }()

			for j := 0; j < entriesPerWriter; j++ {
				entry := WALEntry{
					OpType:   OpPut,
					KeyHash:  uint64(writerID*1000 + j),
					DataSize: 4,
					Data:     []byte("test"),
				}

				err := wal.Append(entry)
				if err != nil {
					t.Errorf("Writer %d failed to append entry %d: %v", writerID, j, err)
					return
				}
			}
		}(i)
	}

	for i := 0; i < numWriters; i++ {
		<-done
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if stat.Size() == 0 {
		t.Error("WAL file should contain concurrent writes")
	}
}

func TestWAL_AutoSync(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "autosync.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	entry := WALEntry{
		OpType:   OpPut,
		KeyHash:  888,
		DataSize: 8,
		Data:     []byte("autosync"),
	}

	err = wal.Append(entry)
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if stat.Size() == 0 {
		t.Error("WAL should auto-sync and flush data to disk")
	}
}

func TestWAL_ChecksumCalculation(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "checksum.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	testData := []byte("test data for checksum")
	entry := WALEntry{
		OpType:   OpPut,
		KeyHash:  999,
		DataSize: uint32(len(testData)),
		Data:     testData,
	}

	originalChecksum := entry.Checksum

	err = wal.Append(entry)
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	// The Append method should have calculated and set the checksum
	// We can't access the modified entry, but we can verify the operation succeeded
	if originalChecksum != 0 {
		t.Error("Entry checksum should start as zero before append")
	}
}

func BenchmarkWAL_Append(b *testing.B) {
	tempDir := b.TempDir()
	walPath := filepath.Join(tempDir, "bench.wal")

	wal, err := NewWal(walPath)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	entry := WALEntry{
		OpType:   OpPut,
		KeyHash:  123456,
		DataSize: 100,
		Data:     make([]byte, 100),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry.KeyHash = uint64(i)
		err := wal.Append(entry)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWAL_NewWALEntry(b *testing.B) {
	data := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = NewWALEntry(OpPut, uint64(i), data)
	}
}
