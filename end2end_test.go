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
	firstLockAcquired := make(chan error, 1)
	released := make(chan struct{})
	firstUnlockErr := make(chan error, 1)

	go func() {
		err := certmagic.Default.Storage.(certmagic.Locker).Lock(ctx, lockKey)
		firstLockAcquired <- err
		if err != nil {
			return
		}
		<-released
		err = certmagic.Default.Storage.(certmagic.Locker).Unlock(ctx, lockKey)
		firstUnlockErr <- err
	}()

	err = <-firstLockAcquired
	require.NoError(t, err, "First lock should succeed")

	// While first lock is held, a second lock attempt with short timeout must fail.
	blockedAttemptErr := make(chan error, 1)
	go func() {
		shortCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		err := certmagic.Default.Storage.(certmagic.Locker).Lock(shortCtx, lockKey)
		blockedAttemptErr <- err
		if err == nil {
			_ = certmagic.Default.Storage.(certmagic.Locker).Unlock(ctx, lockKey)
		}
	}()

	err = <-blockedAttemptErr
	require.Error(t, err, "Second lock attempt should fail while first lock is held")
	require.Equal(t, context.DeadlineExceeded, err, "Second lock should block until timeout while first lock is held")

	// Try to acquire the same lock in another goroutine after release (should succeed)
	secondLockAcquired := make(chan error, 1)
	go func() {
		err := certmagic.Default.Storage.(certmagic.Locker).Lock(ctx, lockKey)
		secondLockAcquired <- err
		if err == nil {
			_ = certmagic.Default.Storage.(certmagic.Locker).Unlock(ctx, lockKey)
		}
	}()

	// Release the first lock
	released <- struct{}{}

	// Wait for the second lock to be acquired
	select {
	case err := <-secondLockAcquired:
		require.NoError(t, err, "Second lock should succeed after first is released")
	case <-time.After(10 * time.Second):
		t.Fatal("Second lock was not acquired in time")
	}

	select {
	case err := <-firstUnlockErr:
		require.NoError(t, err, "Unlock should succeed")
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for first unlock result")
	}

	// Clean up lock blob
	_ = storageBackend.Delete(ctx, lockKey+".lock")
	t.Log("CertMagic distributed lock/unlock integration successful")
}

func TestCertMagicLockLeaseRenewal(t *testing.T) {
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
	require.NoError(t, err, "Azure storage must be available for lock lease renewal test")

	certmagic.Default.Storage = storageBackend
	lockKey := "distributed-lock-renewal-test"

	err = certmagic.Default.Storage.(certmagic.Locker).Lock(ctx, lockKey)
	require.NoError(t, err, "First lock should succeed")

	renewer, ok := certmagic.Default.Storage.(certmagic.LockLeaseRenewer)
	require.True(t, ok, "Storage should implement LockLeaseRenewer")

	err = renewer.RenewLockLease(ctx, lockKey, 30*time.Second)
	require.NoError(t, err, "Lock lease renewal should succeed for active lock")

	shortCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	err = certmagic.Default.Storage.(certmagic.Locker).Lock(shortCtx, lockKey)
	require.ErrorIs(t, err, context.DeadlineExceeded, "Contender should remain blocked while renewed lock is held")

	err = certmagic.Default.Storage.(certmagic.Locker).Unlock(ctx, lockKey)
	require.NoError(t, err, "Unlock should succeed")

	reacquireCtx, reacquireCancel := context.WithTimeout(ctx, 3*time.Second)
	defer reacquireCancel()
	err = certmagic.Default.Storage.(certmagic.Locker).Lock(reacquireCtx, lockKey)
	require.NoError(t, err, "Contender should acquire lock after release")

	_ = certmagic.Default.Storage.(certmagic.Locker).Unlock(ctx, lockKey)
	_ = storageBackend.Delete(ctx, lockKey+".lock")
}
