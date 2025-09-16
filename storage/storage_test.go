package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Attempt to load .env file for local development
	// This will silently fail if .env doesn't exist, which is fine
	_ = godotenv.Load("../.env")
}

const (
	testContainerName = "test-container"
)

func setupTestStorage(t *testing.T) *Storage {
	ctx := context.Background()

	// Check if we should skip Azurite tests, this currently all tests but could be useful in the future
	if os.Getenv("SKIP_AZURITE_TESTS") == "true" {
		t.Skip("Azurite tests disabled via SKIP_AZURITE_TESTS")
	}

	// Get connection string from environment (optional)
	connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")

	// Get account name from environment (required)
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	require.NotEmpty(t, accountName, "AZURE_STORAGE_ACCOUNT is required")

	config := Config{
		AccountName:      accountName,
		ContainerName:    testContainerName,
		ConnectionString: connectionString,
		// Credential will use default Azure credential chain if ConnectionString is empty
	}

	s, err := NewStorage(ctx, config)
	require.NoError(t, err, "Azure storage or Azurite must be available")

	return s
}

func TestAzureBlobStorageOperations(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()

	key := "some/object/file.txt"
	content := []byte("test data content")

	// Test Store operation
	err := s.Store(ctx, key, content)
	require.NoError(t, err)

	// Test Load operation
	loaded, err := s.Load(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, content, loaded)

	// Test Exists operation
	exists := s.Exists(ctx, key)
	assert.True(t, exists)

	// Test Stat operation
	info, err := s.Stat(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, key, info.Key)
	assert.Equal(t, int64(len(content)), info.Size)
	assert.True(t, info.IsTerminal) // files are terminal
	assert.False(t, info.Modified.IsZero())

	// Test Delete operation
	err = s.Delete(ctx, key)
	require.NoError(t, err)

	// Verify deletion
	exists = s.Exists(ctx, key)
	assert.False(t, exists)
}

func TestDeleteBehaviorVerification(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()

	testKey := "delete-behavior-test/test-file.txt"
	testContent := []byte("test content for delete behavior")

	// First, test deleting an existing key
	err := s.Store(ctx, testKey, testContent)
	require.NoError(t, err, "Should be able to store test file")

	exists := s.Exists(ctx, testKey)
	assert.True(t, exists, "File should exist after storing")

	// Delete existing key should succeed
	err = s.Delete(ctx, testKey)
	assert.NoError(t, err, "Delete of existing key should succeed")

	exists = s.Exists(ctx, testKey)
	assert.False(t, exists, "File should not exist after deletion")

	// Second, test deleting the same (now non-existent) key again
	err = s.Delete(ctx, testKey)
	assert.NoError(t, err, "Delete of non-existent key should succeed (idempotent)")

	// Third, test deleting a completely different non-existent key
	nonExistentKey := fmt.Sprintf("never-existed-%d.txt", time.Now().UnixNano())
	err = s.Delete(ctx, nonExistentKey)
	assert.NoError(t, err, "Delete of never-existing key should succeed (idempotent)")

	t.Log("Verified: Azure Blob Storage Delete operations are idempotent as required by CertMagic interface")
}

func TestListOperations(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()

	prefix := "list-test/"
	testKeys := []string{
		prefix + "file1.txt",
		prefix + "dir/file2.txt",
		prefix + "dir/subdir/file3.txt",
	}

	// Store test files
	for i, key := range testKeys {
		content := []byte(fmt.Sprintf("test content %d", i))
		t.Logf("Storing key: %s", key)
		err := s.Store(ctx, key, content)
		require.NoError(t, err, "Failed to store key: %s", key)

		// Verify each key was stored
		exists := s.Exists(ctx, key)
		assert.True(t, exists, "Key should exist after storing: %s", key)
		t.Logf("Key %s stored and verified", key)
	}

	// Test listing with prefix
	keys, err := s.List(ctx, prefix, true) // Use recursive=true to find all files
	require.NoError(t, err)

	// Debug: print what we found
	t.Logf("Found %d keys with prefix '%s': %v", len(keys), prefix, keys)

	// Should find all our test keys
	assert.GreaterOrEqual(t, len(keys), len(testKeys))

	// Verify our keys are in the results
	keyMap := make(map[string]bool)
	for _, key := range keys {
		keyMap[key] = true
	}

	for _, testKey := range testKeys {
		assert.True(t, keyMap[testKey], "Expected key %s to be in list results", testKey)
	}

	// Clean up
	for _, key := range testKeys {
		_ = s.Delete(ctx, key)
	}
}

func TestLockingOperations(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()

	key := "lock-test-key"

	// Test Lock operation
	err := s.Lock(ctx, key)
	require.NoError(t, err)

	// Verify lock blob exists (it should exist but be leased)
	lockKey := key + ".lock"
	exists := s.Exists(ctx, lockKey)
	assert.True(t, exists, "Lock blob should exist after acquiring lease")

	// Test Unlock operation (releases the lease)
	err = s.Unlock(ctx, key)
	require.NoError(t, err)

	// With lease-based locking, the lock blob still exists but is no longer leased
	// The blob should remain (unlike the old file-based approach)
	exists = s.Exists(ctx, lockKey)
	assert.True(t, exists, "Lock blob should still exist after releasing lease")

	// Clean up the lock blob for the test
	_ = s.Delete(ctx, lockKey)
}

func TestConcurrentLocking(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()

	key := "concurrent-lock-test"

	// First lock should succeed
	err := s.Lock(ctx, key)
	require.NoError(t, err)

	// Unlock first lock
	err = s.Unlock(ctx, key)
	require.NoError(t, err)

	// Second lock attempt should now succeed
	err2 := s.Lock(ctx, key)
	require.NoError(t, err2)

	// Unlock second lock
	err = s.Unlock(ctx, key)
	require.NoError(t, err)

	// Clean up any remaining lock files
	lockKey := key + ".lock"
	_ = s.Delete(ctx, lockKey)
}

func TestLeaseBasedConcurrentLocking(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()

	key := "lease-concurrent-test"

	// First lock should succeed immediately
	err := s.Lock(ctx, key)
	require.NoError(t, err)

	// Try to acquire another lock on the same key with a short timeout
	// This should fail due to the lease conflict
	shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err2 := s.Lock(shortCtx, key)
	assert.Error(t, err2, "Second lock attempt should fail due to lease conflict")
	assert.Equal(t, context.DeadlineExceeded, err2, "Should timeout waiting for lease")

	// Release the first lock
	err = s.Unlock(ctx, key)
	require.NoError(t, err)

	// Now a new lock should succeed
	err3 := s.Lock(ctx, key)
	require.NoError(t, err3)

	// Clean up
	err = s.Unlock(ctx, key)
	require.NoError(t, err)

	lockKey := key + ".lock"
	_ = s.Delete(ctx, lockKey)
}

func TestLoadNonExistentKey(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()

	_, err := s.Load(ctx, "non-existent-key")
	assert.Error(t, err)
	// Azure SDK returns specific error types for not found
}

func TestCreatePersistentFiles(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()

	// Create some test files that we won't clean up
	// so you can see them in the Azurite storage browser
	testFiles := map[string]string{
		"demo/certificate.pem":      "-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----",
		"demo/private.key":          "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
		"demo/config/settings.json": `{"domain": "example.com", "email": "admin@example.com"}`,
		"demo/logs/access.log":      "2025-08-01 08:00:00 INFO Certificate renewed successfully",
	}

	for key, content := range testFiles {
		t.Logf("Creating test file: %s", key)
		err := s.Store(ctx, key, []byte(content))
		require.NoError(t, err, "Failed to store %s", key)

		// Verify it exists
		exists := s.Exists(ctx, key)
		assert.True(t, exists, "File should exist: %s", key)
	}

	// List all demo files
	keys, err := s.List(ctx, "demo/", true) // recursive listing
	require.NoError(t, err)
	t.Logf("Created %d demo files: %v", len(keys), keys)

	// Clean up test files
	for key := range testFiles {
		err := s.Delete(ctx, key)
		require.NoError(t, err, "Failed to delete %s", key)
	}

	t.Log("Test files created, verified, and cleaned up successfully")
}

func TestListRecursiveBehavior(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()

	prefix := "recursive-test/"
	testKeys := []string{
		prefix + "file1.txt",                // Should appear in both recursive and non-recursive
		prefix + "file2.txt",                // Should appear in both recursive and non-recursive
		prefix + "dir/file3.txt",            // Should only appear in recursive
		prefix + "dir/subdir/file4.txt",     // Should only appear in recursive
		prefix + "another/nested/file5.txt", // Should only appear in recursive
	}

	// Store test files
	for i, key := range testKeys {
		content := []byte(fmt.Sprintf("test content for recursive test %d", i))
		t.Logf("Storing key: %s", key)
		err := s.Store(ctx, key, content)
		require.NoError(t, err, "Failed to store key: %s", key)

		// Verify each key was stored
		exists := s.Exists(ctx, key)
		assert.True(t, exists, "Key should exist after storing: %s", key)
	}

	// Test recursive=true (should find all files)
	recursiveKeys, err := s.List(ctx, prefix, true)
	require.NoError(t, err, "Recursive listing should not fail")
	t.Logf("Recursive listing found %d keys: %v", len(recursiveKeys), recursiveKeys)

	// Verify all test keys are found in recursive listing
	recursiveKeyMap := make(map[string]bool)
	for _, key := range recursiveKeys {
		recursiveKeyMap[key] = true
	}

	for _, testKey := range testKeys {
		assert.True(t, recursiveKeyMap[testKey], "Expected key %s to be in recursive listing", testKey)
	}

	// Test recursive=false (should only find files directly under prefix, not in subdirectories)
	nonRecursiveKeys, err := s.List(ctx, prefix, false)
	require.NoError(t, err, "Non-recursive listing should not fail")
	t.Logf("Non-recursive listing found %d keys: %v", len(nonRecursiveKeys), nonRecursiveKeys)

	// Expected keys for non-recursive listing (only files directly under prefix)
	expectedNonRecursiveKeys := []string{
		prefix + "file1.txt",
		prefix + "file2.txt",
	}

	// Keys that should NOT be in non-recursive listing (files in subdirectories)
	unexpectedNonRecursiveKeys := []string{
		prefix + "dir/file3.txt",
		prefix + "dir/subdir/file4.txt",
		prefix + "another/nested/file5.txt",
	}

	nonRecursiveKeyMap := make(map[string]bool)
	for _, key := range nonRecursiveKeys {
		nonRecursiveKeyMap[key] = true
	}

	// Verify expected keys are present in non-recursive listing
	for _, expectedKey := range expectedNonRecursiveKeys {
		assert.True(t, nonRecursiveKeyMap[expectedKey], "Expected key %s to be in non-recursive listing", expectedKey)
	}

	// Verify unexpected keys are NOT present in non-recursive listing
	for _, unexpectedKey := range unexpectedNonRecursiveKeys {
		assert.False(t, nonRecursiveKeyMap[unexpectedKey], "Key %s should NOT be in non-recursive listing", unexpectedKey)
	}

	// Verify that non-recursive returns fewer or equal items than recursive
	assert.LessOrEqual(t, len(nonRecursiveKeys), len(recursiveKeys), "Non-recursive listing should return fewer or equal items than recursive")

	// Test edge case: empty prefix
	allKeys, err := s.List(ctx, "", true)
	require.NoError(t, err, "Listing with empty prefix should not fail")
	t.Logf("Found %d total keys with empty prefix", len(allKeys))

	// All our test keys should be in the complete listing
	allKeyMap := make(map[string]bool)
	for _, key := range allKeys {
		allKeyMap[key] = true
	}

	for _, testKey := range testKeys {
		assert.True(t, allKeyMap[testKey], "Expected key %s to be in complete listing", testKey)
	}

	// Clean up all test files
	for _, key := range testKeys {
		err := s.Delete(ctx, key)
		require.NoError(t, err, "Failed to delete key: %s", key)
	}

	t.Log("Recursive behavior test completed successfully")
}

// Test Stat on a non-existent key (should return fs.ErrNotExist)
func TestStatNonExistentKey(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()
	_, err := s.Stat(ctx, "definitely-not-a-real-key.txt")
	assert.ErrorIs(t, err, os.ErrNotExist, "Stat on non-existent key should return os.ErrNotExist")
}

// Test storing and loading a very large blob (>10MB)
func TestLargeBlobStoreLoad(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()
	largeKey := "large-blob-test/largefile.bin"
	largeContent := make([]byte, 12*1024*1024) // 12MB
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	err := s.Store(ctx, largeKey, largeContent)
	require.NoError(t, err, "Should be able to store large blob")
	loadedLarge, err := s.Load(ctx, largeKey)
	require.NoError(t, err, "Should be able to load large blob")
	assert.Equal(t, largeContent, loadedLarge, "Loaded large blob should match stored content")
	_ = s.Delete(ctx, largeKey)
}

// Test storing and loading keys with special characters (Unicode, spaces, etc.)
func TestSpecialCharacterKeys(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()
	specialKeys := []string{
		"special chars/üñîçødë.txt",
		"special chars/with space.txt",
		"special chars/!@#$%^&().txt",
		"special chars/中文文件.txt",
	}
	for i, key := range specialKeys {
		content := []byte(fmt.Sprintf("special content %d", i))
		err := s.Store(ctx, key, content)
		require.NoError(t, err, "Failed to store special key: %s", key)
		loaded, err := s.Load(ctx, key)
		require.NoError(t, err, "Failed to load special key: %s", key)
		assert.Equal(t, content, loaded, "Loaded content mismatch for key: %s", key)
		_ = s.Delete(ctx, key)
	}
}

// Test Store overwriting an existing blob (ensure new content is present)
func TestBlobOverwrite(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()
	key := "overwrite-test/file.txt"
	content1 := []byte("first content")
	content2 := []byte("second content")
	err := s.Store(ctx, key, content1)
	require.NoError(t, err)
	err = s.Store(ctx, key, content2)
	require.NoError(t, err)
	loaded, err := s.Load(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, content2, loaded, "Loaded content should match overwritten content")
	_ = s.Delete(ctx, key)
}

// Test concurrent Store/Load/Delete/List operations from multiple goroutines
func TestConcurrentOperations(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()
	keyPrefix := "concurrent-test/"
	numGoroutines := 10
	numOps := 20
	done := make(chan struct{})

	// Store concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("%sfile-%d-%d.txt", keyPrefix, id, j)
				content := []byte(fmt.Sprintf("content-%d-%d", id, j))
				err := s.Store(ctx, key, content)
				if err != nil {
					t.Errorf("Store failed: %v", err)
				}
				loaded, err := s.Load(ctx, key)
				if err != nil {
					t.Errorf("Load failed: %v", err)
				}
				if string(loaded) != string(content) {
					t.Errorf("Loaded content mismatch for %s", key)
				}
				_ = s.Delete(ctx, key)
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// List should be empty after deletes
	keys, err := s.List(ctx, keyPrefix, true)
	require.NoError(t, err)
	assert.Empty(t, keys, "All keys should be deleted after concurrent ops")
}

// Test lock lease expiration (acquire lock, wait for lease to expire, then try to acquire again)
func TestLockLeaseExpiration(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()
	key := "lease-expiration-test"

	// Acquire lock
	err := s.Lock(ctx, key)
	require.NoError(t, err)

	// Wait for lease to expire (LockExpiration + 2 seconds)
	wait := time.Duration(LockExpiration+2) * time.Second
	t.Logf("Waiting %v for lease to expire...", wait)
	time.Sleep(wait)

	// Try to acquire lock again (should succeed after lease expiration)
	err = s.Lock(ctx, key)
	require.NoError(t, err, "Should be able to acquire lock after lease expires")

	_ = s.Unlock(ctx, key)
	_ = s.Delete(ctx, key+".lock")
}

// Test List with a prefix that matches no blobs (should return empty slice)
func TestListNoMatches(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()
	keys, err := s.List(ctx, "no-such-prefix/", true)
	require.NoError(t, err)
	assert.Empty(t, keys, "List with no matching prefix should return empty slice")
}

// Test Delete on a key with special characters
func TestDeleteSpecialCharacterKey(t *testing.T) {
	s := setupTestStorage(t)
	ctx := context.Background()
	key := "delete-special/üñîçødë file.txt"
	content := []byte("to be deleted")
	err := s.Store(ctx, key, content)
	require.NoError(t, err)
	err = s.Delete(ctx, key)
	require.NoError(t, err)
	exists := s.Exists(ctx, key)
	assert.False(t, exists, "Key with special characters should be deleted")
}
