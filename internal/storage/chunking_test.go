package storage

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"testing"
)

func TestChunkManager_BasicOperations(t *testing.T) {
	cm := NewChunkManager(1024)

	if cm.chunkSize != 1024 {
		t.Errorf("Expected chunk size 1024, got %d", cm.chunkSize)
	}

	cmDefault := NewChunkManager(0)
	if cmDefault.chunkSize != DEFAULT_CHUNK_SIZE {
		t.Errorf("Expected default chunk size %d, got %d", DEFAULT_CHUNK_SIZE, cmDefault.chunkSize)
	}
}

func TestChunkManager_ShouldChunk(t *testing.T) {
	cm := NewChunkManager(DEFAULT_CHUNK_SIZE)

	testCases := []struct {
		size     uint64
		expected bool
	}{
		{1024, false},
		{MIN_CHUNKING_SIZE - 1, false},
		{MIN_CHUNKING_SIZE, false},
		{MIN_CHUNKING_SIZE + 1, true},
		{100 * 1024 * 1024, true},
	}

	for _, tc := range testCases {
		result := cm.ShouldChunk(tc.size)
		if result != tc.expected {
			t.Errorf("ShouldChunk(%d): expected %v, got %v", tc.size, tc.expected, result)
		}
	}
}

func TestChunkManager_SplitIntoChunks_EmptyData(t *testing.T) {
	cm := NewChunkManager(1024)

	_, _, err := cm.SplitIntoChunks([]byte{})
	if err == nil {
		t.Error("Expected error for empty data")
	}
}

func TestChunkManager_SplitIntoChunks_SmallData(t *testing.T) {
	cm := NewChunkManager(1024)

	data := make([]byte, 512)
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks, metadata, err := cm.SplitIntoChunks(data)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(chunks) != 1 {
		t.Errorf("Expected 1 chunk for small data, got %d", len(chunks))
	}

	if !bytes.Equal(chunks[0], data) {
		t.Error("Chunk data should match original data")
	}

	if metadata.OriginalSize != uint64(len(data)) {
		t.Errorf("Expected original size %d, got %d", len(data), metadata.OriginalSize)
	}

	if metadata.ChunkCount != 1 {
		t.Errorf("Expected chunk count 1, got %d", metadata.ChunkCount)
	}

	expectedHash := sha256.Sum256(data)
	if metadata.FileHash != expectedHash {
		t.Error("File hash should match")
	}
}

func TestChunkManager_SplitIntoChunks_LargeData(t *testing.T) {
	cm := NewChunkManager(1024)

	data := make([]byte, 3000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks, metadata, err := cm.SplitIntoChunks(data)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedChunks := (len(data) + int(cm.chunkSize) - 1) / int(cm.chunkSize)
	if len(chunks) != expectedChunks {
		t.Errorf("Expected %d chunks, got %d", expectedChunks, len(chunks))
	}

	if metadata.ChunkCount != uint32(expectedChunks) {
		t.Errorf("Expected chunk count %d, got %d", expectedChunks, metadata.ChunkCount)
	}

	if len(metadata.Chunks) != expectedChunks {
		t.Errorf("Expected %d chunk metadata entries, got %d", expectedChunks, len(metadata.Chunks))
	}

	totalSize := 0
	for i, chunk := range chunks {
		totalSize += len(chunk)

		if i < len(chunks)-1 {
			if len(chunk) != int(cm.chunkSize) {
				t.Errorf("Chunk %d should be full size %d, got %d", i, cm.chunkSize, len(chunk))
			}
		}

		if i == len(chunks)-1 {
			expectedLastSize := len(data) % int(cm.chunkSize)
			if expectedLastSize == 0 {
				expectedLastSize = int(cm.chunkSize)
			}
			if len(chunk) != expectedLastSize {
				t.Errorf("Last chunk should be size %d, got %d", expectedLastSize, len(chunk))
			}
		}
	}

	if totalSize != len(data) {
		t.Errorf("Total chunk size %d should equal original data size %d", totalSize, len(data))
	}
}

func TestChunkManager_ReassembleChunks(t *testing.T) {
	cm := NewChunkManager(1024)

	originalData := make([]byte, 3000)
	for i := range originalData {
		originalData[i] = byte(i % 256)
	}

	chunks, metadata, err := cm.SplitIntoChunks(originalData)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	reassembled, err := cm.ReassembleChunks(chunks, metadata)
	if err != nil {
		t.Fatalf("Reassemble failed: %v", err)
	}

	if !bytes.Equal(originalData, reassembled) {
		t.Error("Reassembled data should match original data")
	}
}

func TestChunkManager_ReassembleChunks_InvalidMetadata(t *testing.T) {
	cm := NewChunkManager(1024)

	chunks := [][]byte{
		make([]byte, 100),
		make([]byte, 100),
	}

	metadata := &FileMetadata{
		OriginalSize: 300,
		ChunkCount:   2,
		ChunkSize:    100,
		Chunks:       make([]ChunkMetadata, 3),
	}

	_, err := cm.ReassembleChunks(chunks, metadata)
	if err == nil {
		t.Error("Expected error for mismatched chunk count")
	}
}

func TestChunkManager_ReassembleChunks_InvalidHash(t *testing.T) {
	cm := NewChunkManager(1024)

	data := make([]byte, 200)
	for i := range data {
		data[i] = byte(i)
	}

	chunks, metadata, err := cm.SplitIntoChunks(data)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	metadata.FileHash[0] = metadata.FileHash[0] ^ 0xFF

	_, err = cm.ReassembleChunks(chunks, metadata)
	if err == nil {
		t.Error("Expected error for invalid hash")
	}
}

func TestChunkMetadata_Basic(t *testing.T) {
	chunk := ChunkMetadata{
		ChunkID:   123,
		Offset:    4567890,
		Size:      1024,
		SegmentID: 99,
		Checksum:  0xDEADBEEF,
	}

	if chunk.ChunkID != 123 {
		t.Errorf("Expected ChunkID 123, got %d", chunk.ChunkID)
	}

	if chunk.Offset != 4567890 {
		t.Errorf("Expected Offset 4567890, got %d", chunk.Offset)
	}

	if chunk.Size != 1024 {
		t.Errorf("Expected Size 1024, got %d", chunk.Size)
	}
}

func TestFileMetadata_Serialization(t *testing.T) {
	original := &FileMetadata{
		OriginalSize: 1000000,
		ChunkSize:    4096,
		ChunkCount:   245,
		FileHash:     sha256.Sum256([]byte("test data")),
		Chunks: []ChunkMetadata{
			{ChunkID: 1, Offset: 0, Size: 4096, SegmentID: 1, Checksum: 0x12345678},
			{ChunkID: 2, Offset: 4096, Size: 4096, SegmentID: 1, Checksum: 0x87654321},
		},
	}

	serialized := original.Serialize()
	if len(serialized) == 0 {
		t.Error("Serialized data should not be empty")
	}

	deserialized, err := DeserializeMetadata(serialized)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	if original.OriginalSize != deserialized.OriginalSize {
		t.Error("Original size should match")
	}

	if original.ChunkSize != deserialized.ChunkSize {
		t.Error("Chunk size should match")
	}

	if original.ChunkCount != deserialized.ChunkCount {
		t.Error("Chunk count should match")
	}

	if original.FileHash != deserialized.FileHash {
		t.Error("File hash should match")
	}

	if len(original.Chunks) != len(deserialized.Chunks) {
		t.Error("Chunk metadata count should match")
	}

	for i, chunk := range original.Chunks {
		deserializedChunk := deserialized.Chunks[i]
		if chunk.ChunkID != deserializedChunk.ChunkID {
			t.Errorf("Chunk %d ChunkID should match: expected %d, got %d", i, chunk.ChunkID, deserializedChunk.ChunkID)
		}
		if chunk.Offset != deserializedChunk.Offset {
			t.Errorf("Chunk %d Offset should match: expected %d, got %d", i, chunk.Offset, deserializedChunk.Offset)
		}
		if chunk.Size != deserializedChunk.Size {
			t.Errorf("Chunk %d Size should match: expected %d, got %d", i, chunk.Size, deserializedChunk.Size)
		}
		if chunk.SegmentID != deserializedChunk.SegmentID {
			t.Errorf("Chunk %d SegmentID should match: expected %d, got %d", i, chunk.SegmentID, deserializedChunk.SegmentID)
		}
	}
}

func TestChunkManager_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	cm := NewChunkManager(1024 * 1024)

	size := 50 * 1024 * 1024
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}

	chunks, metadata, err := cm.SplitIntoChunks(data)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	expectedChunks := (size + int(cm.chunkSize) - 1) / int(cm.chunkSize)
	if len(chunks) != expectedChunks {
		t.Errorf("Expected %d chunks, got %d", expectedChunks, len(chunks))
	}

	reassembled, err := cm.ReassembleChunks(chunks, metadata)
	if err != nil {
		t.Fatalf("Reassemble failed: %v", err)
	}

	if !bytes.Equal(data, reassembled) {
		t.Error("Large file reassembly failed")
	}

	t.Logf("Successfully processed %d MB file into %d chunks", size/(1024*1024), len(chunks))
}

func TestChunkManager_MaxChunks(t *testing.T) {
	cm := NewChunkManager(1)

	data := make([]byte, MAX_CHUNKS+1)

	_, _, err := cm.SplitIntoChunks(data)
	if err == nil {
		t.Error("Expected error when exceeding max chunks")
	}
}

func TestChunkManager_EdgeCases(t *testing.T) {
	cm := NewChunkManager(1024)

	exactSize := make([]byte, 1024)
	chunks, metadata, err := cm.SplitIntoChunks(exactSize)
	if err != nil {
		t.Fatalf("Failed with exact chunk size: %v", err)
	}

	if len(chunks) != 1 {
		t.Errorf("Expected 1 chunk for exact size, got %d", len(chunks))
	}

	if len(chunks[0]) != 1024 {
		t.Errorf("Expected chunk size 1024, got %d", len(chunks[0]))
	}

	reassembled, err := cm.ReassembleChunks(chunks, metadata)
	if err != nil {
		t.Fatalf("Reassemble failed: %v", err)
	}

	if !bytes.Equal(exactSize, reassembled) {
		t.Error("Exact size reassembly failed")
	}
}

func BenchmarkChunkManager_Split(b *testing.B) {
	cm := NewChunkManager(DEFAULT_CHUNK_SIZE)

	data := make([]byte, 10*1024*1024)
	rand.Read(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := cm.SplitIntoChunks(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChunkManager_Reassemble(b *testing.B) {
	cm := NewChunkManager(DEFAULT_CHUNK_SIZE)

	data := make([]byte, 10*1024*1024)
	rand.Read(data)

	chunks, metadata, err := cm.SplitIntoChunks(data)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := cm.ReassembleChunks(chunks, metadata)
		if err != nil {
			b.Fatal(err)
		}
	}
}
