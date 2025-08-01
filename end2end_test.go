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

// Test Configuration:
//
// REQUIRED environment variables:
//   AZURE_STORAGE_CONNECTION_STRING - Azure Storage connection string
//   AZURE_STORAGE_ACCOUNT - Azure Storage account name
//
// Environment variables can be provided via:
//   1. .env file (automatically loaded) - recommended for local development
//   2. Shell environment variables - required for CI/CD
//   3. VS Code launch configurations - .vscode/launch.json
//
// For local development with Azurite (connection string required):
//   Create .env file with:
//   AZURE_STORAGE_CONNECTION_STRING="YOUR_AZURITE_CONNECTION_STRING"
//   AZURE_STORAGE_ACCOUNT=YOUR_AZURITE_ACCOUNT
//
// For testing with real Azure Storage (connection string optional):
//   Option 1 - Connection String:
//     AZURE_STORAGE_CONNECTION_STRING=YOUR_STORAGE_CONNECTION_STRING
//     AZURE_STORAGE_ACCOUNT=YOUR_STORAGE_ACCOUNT
//   Option 2 - Azure CLI (after `az login`):
//     AZURE_STORAGE_ACCOUNT=YOUR_STORAGE_ACCOUNT
//     # No connection string needed!
//
// To skip tests (e.g., in CI):
//   SKIP_AZURITE_TESTS=true

func init() {
	// Try to load .env file, but don't fail if it doesn't exist
	// This allows tests to work with either .env files or environment variables
	_ = godotenv.Load()
}

const (
	testContainer = "test-container"
	)

// getTestConnectionString returns the connection string for tests
// Connection string is optional - when omitted, will use Azure CLI/managed identity
func getTestConnectionString() (string, error) {
	connStr := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	// Connection string is optional - return empty string if not set
	return connStr, nil
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
	// Check if we should skip tests (useful for CI environments without Azurite)
	if os.Getenv("SKIP_AZURITE_TESTS") == "true" {
		t.Skip("Azurite tests disabled via SKIP_AZURITE_TESTS environment variable")
	}

	ctx := context.Background()

	// Get connection details from environment variables
	connectionString, err := getTestConnectionString()
	if err != nil {
		t.Skipf("Skipping test: %v", err)
	}

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
	if err != nil {
		t.Skipf("Azure storage not available: %v. Check your credentials and ensure storage is accessible", err)
	}

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
	// Note: Full ACME integration would require a test ACME server like Pebble
	// For now, we just verify that CertMagic can use our storage backend
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
