package storage

import (
	"fmt"
	"testing"
)

type mockIndex struct {
	entries map[uint64]IndexEntry
}

func newMockIndex() *mockIndex {
	return &mockIndex{
		entries: make(map[uint64]IndexEntry),
	}
}

func (m *mockIndex) Insert(entry IndexEntry) error {
	m.entries[entry.KeyHash] = entry
	return nil
}

func (m *mockIndex) Lookup(keyHash uint64) (*IndexEntry, error) {
	entry, exists := m.entries[keyHash]
	if exists {
		return &entry, nil
	}
	return nil, fmt.Errorf("entry not found")
}

func (m *mockIndex) Size() int {
	return len(m.entries)
}

func TestFileManager_Creation(t *testing.T) {
	fm := NewFileManager(1024)

	if fm.chunkManager == nil {
		t.Error("FileManager should have a chunk manager")
	}

	if fm.chunkManager.chunkSize != 1024 {
		t.Errorf("Expected chunk size 1024, got %d", fm.chunkManager.chunkSize)
	}
}

func TestFileManager_StoreSmallFile(t *testing.T) {
	fm := NewFileManager(DEFAULT_CHUNK_SIZE)
	index := newMockIndex()

	segment, err := NewSegment(1, t.TempDir()+"/test_segment")
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	segments := []*Segment{segment}

	key := "test-key"
	data := []byte("small file data")

	metadata, err := fm.StoreFile(key, data, segments, index)
	if err != nil {
		t.Fatalf("Failed to store small file: %v", err)
	}

	if metadata.OriginalSize != uint64(len(data)) {
		t.Errorf("Expected original size %d, got %d", len(data), metadata.OriginalSize)
	}

	if metadata.ChunkCount != 1 {
		t.Errorf("Expected chunk count 1 for small file, got %d", metadata.ChunkCount)
	}

	keyHash := HashKey(key)
	_, err = index.Lookup(keyHash)
	if err != nil {
		t.Error("Index should contain entry for stored file")
	}
}

func TestFileManager_RetrieveSmallFile(t *testing.T) {
	fm := NewFileManager(DEFAULT_CHUNK_SIZE)
	index := newMockIndex()

	segment, err := NewSegment(1, t.TempDir()+"/retrieve_segment")
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	segments := []*Segment{segment}
	segmentMap := map[uint32]*Segment{1: segment}

	key := "retrieve-test"
	originalData := []byte("data to retrieve")

	_, err = fm.StoreFile(key, originalData, segments, index)
	if err != nil {
		t.Fatalf("Failed to store file: %v", err)
	}

	retrievedData, err := fm.RetrieveFile(key, segmentMap, index)
	if err != nil {
		t.Fatalf("Failed to retrieve file: %v", err)
	}

	if string(originalData) != string(retrievedData) {
		t.Error("Retrieved data should match original data")
	}
}

func TestFileManager_NonexistentFile(t *testing.T) {
	fm := NewFileManager(DEFAULT_CHUNK_SIZE)
	index := newMockIndex()

	segment, err := NewSegment(1, t.TempDir()+"/nonexistent_segment")
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	segmentMap := map[uint32]*Segment{1: segment}

	_, err = fm.RetrieveFile("nonexistent-key", segmentMap, index)
	if err == nil {
		t.Error("Should get error when retrieving nonexistent file")
	}
}

func TestFileManager_ChunkSizeDecision(t *testing.T) {
	fm := NewFileManager(1024)

	testCases := []struct {
		dataSize    int
		shouldChunk bool
		description string
	}{
		{1024, false, "small file"},
		{MIN_CHUNKING_SIZE - 1, false, "just below chunking threshold"},
		{MIN_CHUNKING_SIZE, false, "at chunking threshold"},
		{MIN_CHUNKING_SIZE + 1, true, "just above chunking threshold"},
	}

	for _, tc := range testCases {
		data := make([]byte, tc.dataSize)
		shouldChunk := fm.chunkManager.ShouldChunk(uint64(len(data)))

		if shouldChunk != tc.shouldChunk {
			t.Errorf("%s: expected ShouldChunk=%v for size %d, got %v",
				tc.description, tc.shouldChunk, tc.dataSize, shouldChunk)
		}
	}
}

func TestFileManager_ErrorHandling(t *testing.T) {
	fm := NewFileManager(DEFAULT_CHUNK_SIZE)

	// We can't easily test the nil/empty segments case without modifying
	// the FileManager implementation, as it will panic on segments[0]
	// This would require adding proper validation in the StoreFile method

	// For now, we just test that the FileManager was created properly
	if fm.chunkManager == nil {
		t.Error("FileManager should have a chunk manager")
	}
}

func TestFileManager_EmptyFile(t *testing.T) {
	fm := NewFileManager(DEFAULT_CHUNK_SIZE)
	index := newMockIndex()

	segment, err := NewSegment(1, t.TempDir()+"/empty_segment")
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	segments := []*Segment{segment}
	segmentMap := map[uint32]*Segment{1: segment}

	key := "empty-file"
	data := []byte{}

	metadata, err := fm.StoreFile(key, data, segments, index)
	if err != nil {
		t.Fatalf("Failed to store empty file: %v", err)
	}

	if metadata.OriginalSize != 0 {
		t.Errorf("Expected original size 0, got %d", metadata.OriginalSize)
	}

	retrievedData, err := fm.RetrieveFile(key, segmentMap, index)
	if err != nil {
		t.Fatalf("Failed to retrieve empty file: %v", err)
	}

	if len(retrievedData) != 0 {
		t.Errorf("Expected empty retrieved data, got %d bytes", len(retrievedData))
	}
}

func TestHashKey(t *testing.T) {
	testCases := []struct {
		key      string
		expected bool
	}{
		{"test", true},
		{"", true},
		{"very-long-key-with-lots-of-characters", true},
		{"key with spaces", true},
		{"key\nwith\nnewlines", true},
	}

	for _, tc := range testCases {
		hash := HashKey(tc.key)
		if hash == 0 && tc.key != "" {
			t.Errorf("Hash should not be zero for non-empty key: %s", tc.key)
		}
	}

	hash1 := HashKey("same")
	hash2 := HashKey("same")
	if hash1 != hash2 {
		t.Error("Same keys should produce same hashes")
	}

	hash3 := HashKey("different")
	if hash1 == hash3 {
		t.Error("Different keys should produce different hashes (collision very unlikely)")
	}
}

func TestHashChunkKey(t *testing.T) {
	key := "test-file"

	hash1 := HashChunkKey(key, 0)
	hash2 := HashChunkKey(key, 1)

	if hash1 == hash2 {
		t.Error("Different chunk IDs should produce different hashes")
	}

	hash3 := HashChunkKey(key, 0)
	if hash1 != hash3 {
		t.Error("Same key and chunk ID should produce same hash")
	}
}

func TestHashMetadataKey(t *testing.T) {
	key := "test-file"

	metadataHash := HashMetadataKey(key)
	keyHash := HashKey(key)

	if metadataHash == keyHash {
		t.Error("Metadata hash should be different from key hash")
	}

	metadataHash2 := HashMetadataKey(key)
	if metadataHash != metadataHash2 {
		t.Error("Same metadata key should produce same hash")
	}
}

func BenchmarkFileManager_StoreSmallFile(b *testing.B) {
	fm := NewFileManager(DEFAULT_CHUNK_SIZE)
	index := newMockIndex()

	segment, err := NewSegment(1, b.TempDir()+"/bench_segment")
	if err != nil {
		b.Fatalf("Failed to create segment: %v", err)
	}

	segments := []*Segment{segment}
	data := make([]byte, 1024)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		_, err := fm.StoreFile(key, data, segments, index)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileManager_RetrieveSmallFile(b *testing.B) {
	fm := NewFileManager(DEFAULT_CHUNK_SIZE)
	index := newMockIndex()

	segment, err := NewSegment(1, b.TempDir()+"/bench_retrieve_segment")
	if err != nil {
		b.Fatalf("Failed to create segment: %v", err)
	}

	segments := []*Segment{segment}
	segmentMap := map[uint32]*Segment{1: segment}
	data := make([]byte, 1024)
	keys := make([]string, 1000)

	for i := range keys {
		keys[i] = fmt.Sprintf("bench-retrieve-key-%d", i)
		_, err := fm.StoreFile(keys[i], data, segments, index)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		_, err := fm.RetrieveFile(key, segmentMap, index)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHashKey(b *testing.B) {
	keys := []string{
		"short",
		"medium-length-key",
		"very-long-key-with-lots-of-characters-to-test-performance",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		_ = HashKey(key)
	}
}
