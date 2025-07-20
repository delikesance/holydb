package storage

import (
	"hash/fnv"
	"path/filepath"
	"testing"
)

func TestSegment_BasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	segmentPath := filepath.Join(tempDir, "test_segment.seg")

	segment, err := NewSegment(1, segmentPath)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.File.Close()

	if segment.Size != HEADER_SIZE {
		t.Errorf("New segment should have header size %d, got %d", HEADER_SIZE, segment.Size)
	}

	testData := []byte("test-value")
	testKeyHash := hashKey([]byte("test-key"))

	offset, err := segment.WriteRecord(testKeyHash, testData)
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	if offset == 0 {
		t.Error("Offset should be greater than zero")
	}

	record, err := segment.ReadRecord(offset, uint32(len(testData)))
	if err != nil {
		t.Fatalf("Failed to read record: %v", err)
	}

	if record.KeyHash != testKeyHash {
		t.Errorf("Key hash mismatch. Expected %d, got %d", testKeyHash, record.KeyHash)
	}

	if segment.Size <= HEADER_SIZE {
		t.Error("Segment size should be greater than header size after writing data")
	}
}

func TestSegment_MultipleRecords(t *testing.T) {
	tempDir := t.TempDir()
	segmentPath := filepath.Join(tempDir, "test_multiple.seg")

	segment, err := NewSegment(2, segmentPath)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.File.Close()

	records := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	offsets := make([]uint64, len(records))

	for i, record := range records {
		keyHash := hashKey(record.key)
		offset, err := segment.WriteRecord(keyHash, record.value)
		if err != nil {
			t.Fatalf("Failed to write record %d: %v", i, err)
		}
		offsets[i] = offset
	}

	for i, record := range records {
		recordSize := 4 + 8 + uint32(len(record.value)) // Size + KeyHash + Data
		readRecord, err := segment.ReadRecord(offsets[i], recordSize)
		if err != nil {
			t.Fatalf("Failed to read record %d: %v", i, err)
		}

		expectedHash := hashKey(record.key)
		if readRecord.KeyHash != expectedHash {
			t.Errorf("Key hash mismatch for record %d. Expected %d, got %d", i, expectedHash, readRecord.KeyHash)
		}

		if string(readRecord.Data) != string(record.value) {
			t.Errorf("Data mismatch for record %d. Expected %s, got %s", i, record.value, readRecord.Data)
		}
	}
}

func TestSegment_SegmentFull(t *testing.T) {
	tempDir := t.TempDir()
	segmentPath := filepath.Join(tempDir, "test_full.seg")

	segment, err := NewSegment(3, segmentPath)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.File.Close()

	largeData := make([]byte, MAX_SEGMENT_SIZE/2)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	keyHash := hashKey([]byte("large-key"))
	_, err = segment.WriteRecord(keyHash, largeData)
	if err != nil {
		t.Fatalf("Failed to write first large record: %v", err)
	}

	_, err = segment.WriteRecord(keyHash, largeData)
	if err != ErrSegmentFull {
		t.Errorf("Expected ErrSegmentFull, got %v", err)
	}
}

func TestSegment_Header(t *testing.T) {
	tempDir := t.TempDir()
	segmentPath := filepath.Join(tempDir, "test_header.seg")

	segmentID := uint64(99)
	segment, err := NewSegment(segmentID, segmentPath)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.File.Close()

	if segment.Header.Magic != MAGIC_NUMBER {
		t.Errorf("Magic number mismatch. Expected %x, got %x", MAGIC_NUMBER, segment.Header.Magic)
	}

	if segment.Header.Version != VERSION {
		t.Errorf("Version mismatch. Expected %d, got %d", VERSION, segment.Header.Version)
	}

	if segment.Header.SegmentID != segmentID {
		t.Errorf("Segment ID mismatch. Expected %d, got %d", segmentID, segment.Header.SegmentID)
	}

	if segment.Header.Timestamp == 0 {
		t.Error("Timestamp should not be zero")
	}

	initialRecordCount := segment.Header.RecordCount

	keyHash := hashKey([]byte("test"))
	_, err = segment.WriteRecord(keyHash, []byte("value"))
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	if segment.Header.RecordCount != initialRecordCount+1 {
		t.Errorf("Record count should increase. Expected %d, got %d", initialRecordCount+1, segment.Header.RecordCount)
	}

	if segment.Header.DataSize == 0 {
		t.Error("Data size should be greater than zero after writing")
	}
}

func TestSegment_ReadInvalidRecord(t *testing.T) {
	tempDir := t.TempDir()
	segmentPath := filepath.Join(tempDir, "test_invalid.seg")

	segment, err := NewSegment(4, segmentPath)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.File.Close()

	_, err = segment.ReadRecord(999999, 100)
	if err == nil {
		t.Error("Should return error when reading from invalid offset")
	}

	_, err = segment.ReadRecord(HEADER_SIZE, 0)
	if err == nil {
		t.Error("Should return error when reading zero size")
	}
}

func TestSegment_ConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()
	segmentPath := filepath.Join(tempDir, "test_concurrent.seg")

	segment, err := NewSegment(5, segmentPath)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.File.Close()

	const numGoroutines = 10
	const recordsPerGoroutine = 10

	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < recordsPerGoroutine; j++ {
				key := []byte{byte(id), byte(j)}
				value := []byte{byte(id * 100), byte(j * 10)}
				keyHash := hashKey(key)

				_, err := segment.WriteRecord(keyHash, value)
				if err != nil {
					errChan <- err
					return
				}
			}
			errChan <- nil
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent write failed: %v", err)
		}
	}

	expectedRecords := uint32(numGoroutines * recordsPerGoroutine)
	if segment.Header.RecordCount != expectedRecords {
		t.Errorf("Expected %d records, got %d", expectedRecords, segment.Header.RecordCount)
	}
}

func hashKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

func BenchmarkSegment_WriteRecord(b *testing.B) {
	tempDir := b.TempDir()
	segmentPath := filepath.Join(tempDir, "bench_write.seg")

	segment, err := NewSegment(100, segmentPath)
	if err != nil {
		b.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.File.Close()

	data := []byte("benchmark-value-for-testing-performance")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		keyHash := uint64(i)
		_, err := segment.WriteRecord(keyHash, data)
		if err != nil {
			if err == ErrSegmentFull {
				break
			}
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkSegment_ReadRecord(b *testing.B) {
	tempDir := b.TempDir()
	segmentPath := filepath.Join(tempDir, "bench_read.seg")

	segment, err := NewSegment(101, segmentPath)
	if err != nil {
		b.Fatalf("Failed to create segment: %v", err)
	}
	defer segment.File.Close()

	data := []byte("benchmark-value-for-testing-performance")
	offsets := make([]uint64, 1000)

	for i := 0; i < 1000; i++ {
		keyHash := uint64(i)
		offset, err := segment.WriteRecord(keyHash, data)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		offsets[i] = offset
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := offsets[i%1000]
		_, err := segment.ReadRecord(offset, uint32(len(data)))
		if err != nil {
			b.Fatalf("Read failed: %v", err)
		}
	}
}
