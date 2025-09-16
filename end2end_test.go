package certmagicazureblob_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/caddyserver/certmagic"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/webedmj/certmagic-azureblob/storage"
)

func init() {
	// Try to load .env file, but don't fail if it doesn't exist
	// This allows tests to work with either .env files or environment variables
	_ = godotenv.Load()
}

const (
	testContainer = "test-container"
)

// getTestConnectionString returns the connection string for tests

func getTestConnectionString() string {
	connStr := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	// Connection string is optional - return empty string if not set
	return connStr
}

// getTestAccountName returns the account name for tests
// Requires AZURE_STORAGE_ACCOUNT environment variable or .env file
func getTestAccountName() (string, error) {
	accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	if accountName == "" {
		return "", fmt.Errorf("AZURE_STORAGE_ACCOUNT environment variable is required for tests. Set it in your shell or create a .env file (see .env.example)")
	}
	return accountName, nil
}

func TestAzureBlobStorage(t *testing.T) {
	// Check if we should skip Azurite tests, this currently all tests but could be useful in the future
	if os.Getenv("SKIP_AZURITE_TESTS") == "true" {
		t.Skip("Azurite tests disabled via SKIP_AZURITE_TESTS environment variable")
	}

	ctx := context.Background()

	// Get connection details from environment variables
	connectionString := getTestConnectionString()

	accountName, err := getTestAccountName()
	if err != nil {
		t.Skipf("Skipping test: %v", err)
	}

	// Set up Azure Blob Storage with environment-configured credentials
	// If connectionString is empty, will use Azure CLI/managed identity
	storageBackend, err := storage.NewStorage(ctx, storage.Config{
		AccountName:      accountName,
		ContainerName:    testContainer,
		ConnectionString: connectionString,
	})
	require.NoError(t, err, "Azure storage must be available for end-to-end integration test. Check your credentials and ensure storage is accessible")

	// Test basic storage operations to verify integration
	testKey := "end2end-test/certificate.pem"
	testData := []byte("-----BEGIN CERTIFICATE-----\nMIIC...")

	// Test Store operation
	err = storageBackend.Store(ctx, testKey, testData)
	require.NoError(t, err)

	// Test Load operation
	loaded, err := storageBackend.Load(ctx, testKey)
	require.NoError(t, err)
	assert.Equal(t, testData, loaded)

	// Test CertMagic integration
	// Verify that CertMagic can use our storage backend
	certmagic.Default.Storage = storageBackend

	// Verify CertMagic can use the storage
	assert.NotNil(t, certmagic.Default.Storage)

	// Test key listing through CertMagic interface
	keys, err := certmagic.Default.Storage.List(ctx, "end2end-test/", false)
	require.NoError(t, err)
	assert.Contains(t, keys, testKey)

	// Clean up
	err = storageBackend.Delete(ctx, testKey)
	require.NoError(t, err)

	t.Log("Azure Blob Storage integration with CertMagic successful")
}

// Test CertMagic integration for lock/unlock and distributed workflows
func TestCertMagicDistributedLocking(t *testing.T) {
	if os.Getenv("SKIP_AZURITE_TESTS") == "true" {
		t.Skip("Azurite tests disabled via SKIP_AZURITE_TESTS environment variable")
	}

	ctx := context.Background()
	connectionString := getTestConnectionString()
	accountName, err := getTestAccountName()
	if err != nil {
		t.Skipf("Skipping test: %v", err)
	}

	storageBackend, err := storage.NewStorage(ctx, storage.Config{
		AccountName:      accountName,
		ContainerName:    testContainer,
		ConnectionString: connectionString,
	})
	require.NoError(t, err, "Azure storage must be available for distributed lock test")

	certmagic.Default.Storage = storageBackend

	lockKey := "distributed-lock-test"

	// Acquire lock in one goroutine
	locked := make(chan struct{})
	released := make(chan struct{})

	go func() {
		err := certmagic.Default.Storage.(certmagic.Locker).Lock(ctx, lockKey)
		require.NoError(t, err, "First lock should succeed")
		locked <- struct{}{}
		<-released
		err = certmagic.Default.Storage.(certmagic.Locker).Unlock(ctx, lockKey)
		require.NoError(t, err, "Unlock should succeed")
	}()

	<-locked

	// Try to acquire the same lock in another goroutine (should block until released)
	acquired := make(chan struct{})
	go func() {
		err := certmagic.Default.Storage.(certmagic.Locker).Lock(ctx, lockKey)
		require.NoError(t, err, "Second lock should succeed after first is released")
		acquired <- struct{}{}
		_ = certmagic.Default.Storage.(certmagic.Locker).Unlock(ctx, lockKey)
	}()

	// Wait a moment to ensure the second goroutine is blocked
	time.Sleep(2 * time.Second)

	// Release the first lock
	released <- struct{}{}

	// Wait for the second lock to be acquired
	select {
	case <-acquired:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Second lock was not acquired in time")
	}

	// Clean up lock blob
	_ = storageBackend.Delete(ctx, lockKey+".lock")
	t.Log("CertMagic distributed lock/unlock integration successful")
}
