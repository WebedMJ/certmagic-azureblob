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

	// Check if we should skip tests (useful for CI environments without Azurite)
	if os.Getenv("SKIP_AZURITE_TESTS") == "true" {
		t.Skip("Azurite tests disabled via SKIP_AZURITE_TESTS environment variable")
	}

	// Get connection string from environment (optional)
	connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")

	// Get account name from environment (required)
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	if accountName == "" {
		t.Skip("AZURE_STORAGE_ACCOUNT environment variable is required for tests. Set it in your shell or create a .env file (see .env.example).")
	}

	config := Config{
		AccountName:      accountName,
		ContainerName:    testContainerName,
		ConnectionString: connectionString,
		// Credential will use default Azure credential chain if ConnectionString is empty
	}

	s, err := NewStorage(ctx, config)
	if err != nil {
		t.Skipf("Azure storage not available: %v. Make sure your credentials are valid and storage is accessible", err)
	}

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
		"demo/certificate.pem":        "-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----",
		"demo/private.key":           "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
		"demo/config/settings.json":  `{"domain": "example.com", "email": "admin@example.com"}`,
		"demo/logs/access.log":       "2025-08-01 08:00:00 INFO Certificate renewed successfully",
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
