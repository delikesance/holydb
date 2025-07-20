package index

import (
	"testing"
)

func TestBloomFilter_BasicOperations(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	testKey := []byte("test-key")

	if bf.Contains(testKey) {
		t.Error("Bloom filter should not contain key before adding")
	}

	bf.Add(testKey)

	if !bf.Contains(testKey) {
		t.Error("Bloom filter should contain key after adding")
	}
}

func TestBloomFilter_HashBasedOperations(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	testHash := uint64(12345)

	if bf.ContainsHash(testHash) {
		t.Error("Bloom filter should not contain hash before adding")
	}

	bf.AddHash(testHash)

	if !bf.ContainsHash(testHash) {
		t.Error("Bloom filter should contain hash after adding")
	}
}

func TestBloomFilter_FalsePositives(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	truePositives := 0
	falsePositives := 0
	tested := 0

	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		bf.Add(key)
	}

	for i := 0; i < 200; i++ {
		key := []byte{byte(i)}
		contains := bf.Contains(key)

		if i < 50 {
			if contains {
				truePositives++
			} else {
				t.Errorf("Should contain key %d (false negative)", i)
			}
		} else {
			tested++
			if contains {
				falsePositives++
			}
		}
	}

	falsePositiveRate := float64(falsePositives) / float64(tested)

	t.Logf("False positive rate: %.4f (target: 0.01)", falsePositiveRate)

	if falsePositiveRate > 0.05 {
		t.Errorf("False positive rate too high: %.4f", falsePositiveRate)
	}
}

func TestBloomFilter_Stats(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	stats := bf.Stats()
	if stats.Size == 0 {
		t.Error("Bloom filter size should not be zero")
	}
	if stats.BitsSet != 0 {
		t.Error("Initially no bits should be set")
	}
	if stats.EstimatedElements != 0 {
		t.Error("Initially no elements should be estimated")
	}

	for i := 0; i < 10; i++ {
		bf.Add([]byte{byte(i)})
	}

	stats = bf.Stats()
	if stats.BitsSet == 0 {
		t.Error("Some bits should be set after adding elements")
	}
	if stats.EstimatedElements != 10 {
		t.Errorf("Expected 10 estimated elements, got %d", stats.EstimatedElements)
	}
	if stats.FillRatio <= 0 {
		t.Error("Fill ratio should be positive")
	}
}

func TestBloomFilter_Clear(t *testing.T) {
	config := DefaultBloomConfig()
	bf := NewBloomFilter(config)

	for i := 0; i < 10; i++ {
		bf.Add([]byte{byte(i)})
	}

	stats := bf.Stats()
	if stats.BitsSet == 0 {
		t.Error("Should have bits set before clear")
	}

	bf.Clear()

	stats = bf.Stats()
	if stats.BitsSet != 0 {
		t.Error("No bits should be set after clear")
	}
	if stats.EstimatedElements != 0 {
		t.Error("No elements should be estimated after clear")
	}

	for i := 0; i < 10; i++ {
		if bf.Contains([]byte{byte(i)}) {
			t.Error("Should not contain any elements after clear")
		}
	}
}

func TestBloomFilter_Serialization(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}
	original := NewBloomFilter(config)

	testKeys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	for _, key := range testKeys {
		original.Add(key)
	}

	serialized := original.Serialize()

	deserialized, err := DeserializeBloomFilter(serialized)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	for _, key := range testKeys {
		if !deserialized.Contains(key) {
			t.Errorf("Deserialized bloom filter should contain key %s", string(key))
		}
	}

	originalStats := original.Stats()
	deserializedStats := deserialized.Stats()

	if originalStats.Size != deserializedStats.Size {
		t.Error("Size should match after deserialization")
	}
	if originalStats.EstimatedElements != deserializedStats.EstimatedElements {
		t.Error("Estimated elements should match after deserialization")
	}
}

func TestBloomFilter_DefaultConfig(t *testing.T) {
	config := DefaultBloomConfig()

	if config.ExpectedElements == 0 {
		t.Error("Default config should have non-zero expected elements")
	}
	if config.FalsePositiveRate <= 0 || config.FalsePositiveRate >= 1 {
		t.Error("Default config should have valid false positive rate")
	}

	bf := NewBloomFilter(config)
	if bf == nil {
		t.Error("Should be able to create bloom filter with default config")
	}
}

func TestBloomFilter_ErrorCases(t *testing.T) {
	invalidData := []byte{1, 2, 3}

	_, err := DeserializeBloomFilter(invalidData)
	if err == nil {
		t.Error("Should return error for invalid serialized data")
	}

	emptyData := []byte{}
	_, err = DeserializeBloomFilter(emptyData)
	if err == nil {
		t.Error("Should return error for empty data")
	}
}

func BenchmarkBloomFilter_Add(b *testing.B) {
	config := BloomFilterConfig{
		ExpectedElements:  uint64(b.N),
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		bf.Add(key)
	}
}

func BenchmarkBloomFilter_Contains(b *testing.B) {
	config := BloomFilterConfig{
		ExpectedElements:  10000,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	for i := 0; i < 1000; i++ {
		key := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		bf.Add(key)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		bf.Contains(key)
	}
}

func BenchmarkBloomFilter_AddHash(b *testing.B) {
	config := BloomFilterConfig{
		ExpectedElements:  uint64(b.N),
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		bf.AddHash(uint64(i))
	}
}

func BenchmarkBloomFilter_ContainsHash(b *testing.B) {
	config := BloomFilterConfig{
		ExpectedElements:  10000,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	for i := 0; i < 1000; i++ {
		bf.AddHash(uint64(i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		bf.ContainsHash(uint64(i))
	}
}
