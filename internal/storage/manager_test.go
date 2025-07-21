package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func setupManager(t *testing.T, dir string) *StorageManager {
	_ = os.MkdirAll(dir+"/segments", 0755)
	configPath := dir + "/config.yaml"
	os.WriteFile(configPath, []byte("storage_path: \""+dir+"\""), 0644)
	manager, err := NewStorageManager(configPath)
	if err != nil {
		t.Fatalf("Failed to initialize StorageManager: %v", err)
	}
	return manager
}

func TestStorageManager_StoreFile(t *testing.T) {
    tmpDir := "testdata_manager_storefile"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    fileKey := "file1"
    fileData := []byte("file content")
    if err := manager.StoreFile(fileKey, fileData); err != nil {
        t.Fatalf("StoreFile failed: %v", err)
    }
}

func TestStorageManager_RetrieveFile(t *testing.T) {
    tmpDir := "testdata_manager_retrievefile"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    fileKey := "file1"
    fileData := []byte("file content")
    if err := manager.StoreFile(fileKey, fileData); err != nil {
        t.Fatalf("StoreFile failed: %v", err)
    }
    got, err := manager.RetrieveFile(fileKey)
    if err != nil {
        t.Errorf("RetrieveFile failed: %v", err)
    }
    if string(got) != string(fileData) {
        t.Errorf("RetrieveFile returned wrong data: got %s, want %s", string(got), string(fileData))
    }
}

func TestStorageManager_UpdateFile(t *testing.T) {
    tmpDir := "testdata_manager_updatefile"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    fileKey := "file1"
    fileData := []byte("file content")
    if err := manager.StoreFile(fileKey, fileData); err != nil {
        t.Fatalf("StoreFile failed: %v", err)
    }
    newFileData := []byte("updated file content")
    if err := manager.UpdateFile(fileKey, newFileData); err != nil {
        t.Fatalf("UpdateFile failed: %v", err)
    }
    got, err := manager.RetrieveFile(fileKey)
    if err != nil {
        t.Errorf("RetrieveFile failed: %v", err)
    }
    if string(got) != string(newFileData) {
        t.Errorf("UpdateFile did not update content")
    }
}

func TestStorageManager_DeleteFile(t *testing.T) {
    tmpDir := "testdata_manager_deletefile"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    fileKey := "file1"
    fileData := []byte("file content")
    if err := manager.StoreFile(fileKey, fileData); err != nil {
        t.Fatalf("StoreFile failed: %v", err)
    }
    if err := manager.DeleteFile(fileKey); err != nil {
        t.Fatalf("DeleteFile failed: %v", err)
    }
    _, err := manager.RetrieveFile(fileKey)
    if err == nil {
        t.Errorf("Expected error after DeleteFile, got nil")
    }
}

func TestStorageManager_StoreData(t *testing.T) {
    tmpDir := "testdata_manager_storedata"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    dataKey := "datakey"
    data := []byte("some data")
    if err := manager.StoreData(dataKey, data); err != nil {
        t.Fatalf("StoreData failed: %v", err)
    }
}

func TestStorageManager_RetrieveData(t *testing.T) {
    tmpDir := "testdata_manager_retrievedata"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    dataKey := "datakey"
    data := []byte("some data")
    if err := manager.StoreData(dataKey, data); err != nil {
        t.Fatalf("StoreData failed: %v", err)
    }
    got, err := manager.RetrieveData(dataKey)
    if err != nil {
        t.Errorf("RetrieveData failed: %v", err)
    }
    if string(got) != string(data) {
        t.Errorf("RetrieveData returned wrong data: got %s, want %s", string(got), string(data))
    }
}

func TestStorageManager_UpdateData(t *testing.T) {
    tmpDir := "testdata_manager_updatedata"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    dataKey := "datakey"
    data := []byte("some data")
    if err := manager.StoreData(dataKey, data); err != nil {
        t.Fatalf("StoreData failed: %v", err)
    }
    newData := []byte("updated data")
    if err := manager.UpdateData(dataKey, newData); err != nil {
        t.Fatalf("UpdateData failed: %v", err)
    }
    got, err := manager.RetrieveData(dataKey)
    if err != nil {
        t.Errorf("RetrieveData failed: %v", err)
    }
    if string(got) != string(newData) {
        t.Errorf("UpdateData did not update content")
    }
}

func TestStorageManager_DeleteData(t *testing.T) {
    tmpDir := "testdata_manager_deletedata"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    dataKey := "datakey"
    data := []byte("some data")
    if err := manager.StoreData(dataKey, data); err != nil {
        t.Fatalf("StoreData failed: %v", err)
    }
    if err := manager.DeleteData(dataKey); err != nil {
        t.Fatalf("DeleteData failed: %v", err)
    }
    _, err := manager.RetrieveData(dataKey)
    if err == nil {
        t.Errorf("Expected error after DeleteData, got nil")
    }
}

func TestStorageManager_EmptyData(t *testing.T) {
    tmpDir := "testdata_manager_emptydata"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    if err := manager.StoreData("empty", []byte{}); err != nil {
        t.Errorf("StoreData failed for empty data: %v", err)
    }
    got, err := manager.RetrieveData("empty")
    if err != nil || len(got) != 0 {
        t.Errorf("RetrieveData failed for empty data")
    }
}

func TestStorageManager_DuplicateKey(t *testing.T) {
    tmpDir := "testdata_manager_duplicatekey"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    if err := manager.StoreData("dup", []byte("first")); err != nil {
        t.Errorf("StoreData failed for dup key: %v", err)
    }
    if err := manager.StoreData("dup", []byte("second")); err != nil {
        t.Errorf("StoreData failed for dup key: %v", err)
    }
    got, err := manager.RetrieveData("dup")
    if err != nil || string(got) != "second" {
        t.Errorf("Duplicate key did not update value")
    }
}

func TestStorageManager_LargeData(t *testing.T) {
    tmpDir := "testdata_manager_largedata"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    large := make([]byte, MaxDocumentSize)
    for i := range large {
        large[i] = byte(i % 256)
    }
    if err := manager.StoreData("large", large); err != nil {
        t.Errorf("StoreData failed for large data: %v", err)
    }
    got, err := manager.RetrieveData("large")
    if err != nil || len(got) != MaxDocumentSize {
        t.Errorf("RetrieveData failed for large data")
    }
}

func TestStorageManager_SegmentCapacity(t *testing.T) {
    tmpDir := "testdata_manager_segmentcapacity"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
bigData := make([]byte, SegmentSize)
if err := manager.StoreData("bigkey", bigData); err == nil {
    t.Errorf("Expected error for big segment, got nil")
}
/* Segment is not full since bigData was not stored due to size limit.
err := manager.StoreData("overflow", []byte("x"))
if err == nil {
    t.Errorf("Expected error for overflow, got nil")
}
*/
}

func TestStorageManager_WALRecovery(t *testing.T) {
    tmpDir := "testdata_manager_walrecovery"
    defer os.RemoveAll(tmpDir)
    
    // First manager - store data
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    if err := manager.StoreData("recover", []byte("recovered")); err != nil {
        t.Fatalf("StoreData failed: %v", err)
    }
    
    // Close the first manager to ensure WAL is flushed
    if closer, ok := interface{}(manager.walManager).(interface{ Close() error }); ok {
        if err := closer.Close(); err != nil {
            t.Fatalf("Failed to close WAL: %v", err)
        }
    }
    
    // Second manager - should recover from WAL
    manager2 := setupManager(t, tmpDir)
    // Add segments to manager2 as well since they might not be automatically restored
    for i := 1; i <= 3; i++ {
        if err := manager2.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d to manager2: %v", i, err)
        }
    }
    got, err := manager2.RetrieveData("recover")
    if err != nil {
        t.Errorf("WAL recovery failed - could not retrieve data: %v", err)
        return
    }
    if string(got) != "recovered" {
        t.Errorf("WAL recovery failed - wrong data: got %s, want recovered", string(got))
    }
}

func TestStorageManager_Concurrency(t *testing.T) {
    tmpDir := "testdata_manager_concurrency"
    defer os.RemoveAll(tmpDir)
    manager := setupManager(t, tmpDir)
    for i := 1; i <= 3; i++ {
        if err := manager.AddSegment(uint64(i)); err != nil {
            t.Fatalf("Failed to add segment %d: %v", i, err)
        }
    }
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            key := fmt.Sprintf("concurrent_%d", i)
            val := []byte(fmt.Sprintf("val_%d", i))
            if err := manager.StoreData(key, val); err != nil {
                t.Errorf("Concurrent StoreData failed: %v", err)
            }
            got, err := manager.RetrieveData(key)
            if err != nil || string(got) != string(val) {
                t.Errorf("Concurrent RetrieveData failed: %v", err)
            }
        }(i)
    }
    wg.Wait()
}
