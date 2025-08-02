package certmagicazureblob_test

import (
	"context"
	"fmt"
	"os"
	"testing"

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
