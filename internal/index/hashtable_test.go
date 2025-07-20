package index

import (
	"sync"
	"testing"
	"time"
)

func TestHashTable_BasicOperations(t *testing.T) {
	ht := NewHashTable(16)

	testEntry := NewIndexEntry(123456, 1, 100, 256)

	err := ht.Insert(testEntry)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	found, err := ht.Lookup(123456)
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	if found.KeyHash != 123456 || found.SegmentID != 1 || found.Offset != 100 || found.Size != 256 {
		t.Errorf("Retrieved entry doesn't match inserted entry")
	}

	err = ht.Delete(123456)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = ht.Lookup(123456)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got: %v", err)
	}
}

func TestHashTable_BloomFilterOptimization(t *testing.T) {
	ht := NewHashTable(16)

	nonExistentKey := uint64(999999)

	_, err := ht.Lookup(nonExistentKey)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for non-existent key, got: %v", err)
	}

	stats := ht.GetBloomStats()
	if stats.Size == 0 {
		t.Error("Bloom filter should be initialized with non-zero size")
	}
}

func TestHashTable_Resize(t *testing.T) {
	ht := NewHashTable(4)

	for i := uint64(0); i < 10; i++ {
		entry := NewIndexEntry(i, uint32(i), i*100, 256)
		err := ht.Insert(entry)
		if err != nil {
			t.Fatalf("Insert failed for entry %d: %v", i, err)
		}
	}

	for i := uint64(0); i < 10; i++ {
		found, err := ht.Lookup(i)
		if err != nil {
			t.Fatalf("Lookup failed for key %d after resize: %v", i, err)
		}
		if found.KeyHash != i {
			t.Errorf("Wrong entry found for key %d", i)
		}
	}
}

func TestHashTable_ConcurrentAccess(t *testing.T) {
	ht := NewHashTable(64)
	const numGoroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < opsPerGoroutine; i++ {
				keyHash := uint64(goroutineID*opsPerGoroutine + i)
				entry := NewIndexEntry(keyHash, uint32(goroutineID), uint64(i), 256)

				err := ht.Insert(entry)
				if err != nil {
					t.Errorf("Concurrent insert failed: %v", err)
					return
				}

				found, err := ht.Lookup(keyHash)
				if err != nil {
					t.Errorf("Concurrent lookup failed: %v", err)
					return
				}

				if found.KeyHash != keyHash {
					t.Errorf("Concurrent operation data corruption detected")
					return
				}
			}
		}(g)
	}

	wg.Wait()
}

func TestHashTable_UpdateEntry(t *testing.T) {
	ht := NewHashTable(16)

	keyHash := uint64(12345)
	originalEntry := NewIndexEntry(keyHash, 1, 100, 256)
	updatedEntry := NewIndexEntry(keyHash, 2, 200, 512)

	err := ht.Insert(originalEntry)
	if err != nil {
		t.Fatalf("Insert original entry failed: %v", err)
	}

	err = ht.Insert(updatedEntry)
	if err != nil {
		t.Fatalf("Update entry failed: %v", err)
	}

	found, err := ht.Lookup(keyHash)
	if err != nil {
		t.Fatalf("Lookup after update failed: %v", err)
	}

	if found.SegmentID != 2 || found.Offset != 200 || found.Size != 512 {
		t.Errorf("Entry was not properly updated")
	}
}

func TestHashKey_Consistency(t *testing.T) {
	testKeys := []string{
		"test-key-1",
		"test-key-2",
		"",
		"very-long-key-with-many-characters-to-test-hashing",
		"special-chars-!@#$%^&*()",
		"unicode-éñïçødé",
	}

	for _, key := range testKeys {
		hash1 := HashKey(key)
		hash2 := HashKey(key)

		if hash1 != hash2 {
			t.Errorf("HashKey not consistent for key '%s': %d != %d", key, hash1, hash2)
		}
	}

	hash1 := HashKey("key1")
	hash2 := HashKey("key2")
	if hash1 == hash2 {
		t.Error("Different keys should produce different hashes (collision detected)")
	}
}

func TestHashTable_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ht := NewHashTable(1024)
	const numEntries = 100000

	start := time.Now()

	for i := 0; i < numEntries; i++ {
		keyHash := uint64(i)
		entry := NewIndexEntry(keyHash, uint32(i%10), uint64(i*100), 256)
		err := ht.Insert(entry)
		if err != nil {
			t.Fatalf("Performance test insert failed at %d: %v", i, err)
		}
	}

	insertTime := time.Since(start)

	start = time.Now()

	for i := 0; i < numEntries; i++ {
		keyHash := uint64(i)
		_, err := ht.Lookup(keyHash)
		if err != nil {
			t.Fatalf("Performance test lookup failed at %d: %v", i, err)
		}
	}

	lookupTime := time.Since(start)

	t.Logf("Performance test: %d entries", numEntries)
	t.Logf("Insert time: %v (%.2f ns/op)", insertTime, float64(insertTime.Nanoseconds())/float64(numEntries))
	t.Logf("Lookup time: %v (%.2f ns/op)", lookupTime, float64(lookupTime.Nanoseconds())/float64(numEntries))

	if insertTime > time.Second*5 {
		t.Error("Insert performance is too slow")
	}
	if lookupTime > time.Second*2 {
		t.Error("Lookup performance is too slow")
	}
}

func TestNewIndexEntry(t *testing.T) {
	before := time.Now()
	entry := NewIndexEntry(12345, 1, 100, 256)
	after := time.Now()

	if entry.KeyHash != 12345 {
		t.Errorf("Expected KeyHash 12345, got %d", entry.KeyHash)
	}
	if entry.SegmentID != 1 {
		t.Errorf("Expected SegmentID 1, got %d", entry.SegmentID)
	}
	if entry.Offset != 100 {
		t.Errorf("Expected Offset 100, got %d", entry.Offset)
	}
	if entry.Size != 256 {
		t.Errorf("Expected Size 256, got %d", entry.Size)
	}

	timestamp := time.Unix(0, int64(entry.Timestamp))
	if timestamp.Before(before) || timestamp.After(after) {
		t.Error("Timestamp should be set to current time")
	}
}

func benchmarkHashTableLookup(b *testing.B, size int) {
	ht := NewHashTable(uint64(size))

	for i := 0; i < size; i++ {
		entry := NewIndexEntry(uint64(i), uint32(i%10), uint64(i*100), 256)
		ht.Insert(entry)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		keyHash := uint64(i % size)
		_, err := ht.Lookup(keyHash)
		if err != nil {
			b.Fatalf("Lookup failed: %v", err)
		}
	}
}

func BenchmarkHashTable_Lookup_1K(b *testing.B)   { benchmarkHashTableLookup(b, 1000) }
func BenchmarkHashTable_Lookup_10K(b *testing.B)  { benchmarkHashTableLookup(b, 10000) }
func BenchmarkHashTable_Lookup_100K(b *testing.B) { benchmarkHashTableLookup(b, 100000) }

func BenchmarkHashTable_Insert(b *testing.B) {
	ht := NewHashTable(1024)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := NewIndexEntry(uint64(i), uint32(i%10), uint64(i*100), 256)
		err := ht.Insert(entry)
		if err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}
}
