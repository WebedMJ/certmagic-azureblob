package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/caddyserver/certmagic"
)

var (
	// LockExpiration in seconds is the duration before which a Lock is considered expired (Azure lease duration)
	LockExpiration int32 = 60
	// LockPollInterval is the interval between lease acquisition retries
	LockPollInterval = 1 * time.Second
)

// Storage is a certmagic.Storage backed by an Azure Blob Storage container
type Storage struct {
	containerClient *container.Client
	// activeLocks tracks active lease clients for cleanup
	activeLocks map[string]*lease.BlobClient
}

// Interface guards
var (
	_ certmagic.Storage = (*Storage)(nil)
	_ certmagic.Locker  = (*Storage)(nil)
)

//nolint:govet // fieldalignment: struct field order optimized for readability over memory
type Config struct {
	// AccountName is the Azure Storage account name
	AccountName string
	// ContainerName is the name of the Azure Blob Storage container
	ContainerName string
	// ConnectionString is the Azure Storage connection string (optional)
	ConnectionString string
	// Credential can be used for authentication (managed identity, etc.)
	Credential azcore.TokenCredential
}

//nolint:nestif // Functionally correct and readable
func NewStorage(ctx context.Context, config Config) (*Storage, error) {
	var containerClient *container.Client
	var err error

	if config.ConnectionString != "" {
		// Use connection string
		containerClient, err = container.NewClientFromConnectionString(config.ConnectionString, config.ContainerName, nil)
		if err != nil {
			return nil, fmt.Errorf("could not initialize container client with connection string: %w", err)
		}
	} else {
		// Use credential (explicit or default chain)
		var credential azcore.TokenCredential
		if config.Credential != nil {
			credential = config.Credential
		} else {
			// Use default Azure credential chain (Azure CLI, managed identity, etc.)
			credential, err = azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				return nil, fmt.Errorf("could not create default Azure credential: %w", err)
			}
		}

		// Use managed identity or other credential
		accountURL := fmt.Sprintf("https://%s.blob.core.windows.net/", config.AccountName)
		serviceClient, clientErr := azblob.NewClient(accountURL, credential, nil)
		if clientErr != nil {
			return nil, fmt.Errorf("could not initialize service client: %w", clientErr)
		}
		containerClient = serviceClient.ServiceClient().NewContainerClient(config.ContainerName)
	}

	// Ensure the container exists (create if it doesn't)
	_, err = containerClient.Create(ctx, nil)
	if err != nil {
		// Check if error is because container already exists (which is fine)
		var respErr *azcore.ResponseError
		if !errors.As(err, &respErr) || respErr.ErrorCode != "ContainerAlreadyExists" {
			return nil, fmt.Errorf("could not create container: %w", err)
		}
		// Container already exists, which is fine - continue
	}

	return &Storage{
		containerClient: containerClient,
		activeLocks:     make(map[string]*lease.BlobClient),
	}, nil
}

// Store puts value at key.
func (s *Storage) Store(ctx context.Context, key string, value []byte) error {
	blockBlobClient := s.containerClient.NewBlockBlobClient(key)

	// Upload the blob data directly from bytes
	_, err := blockBlobClient.UploadBuffer(ctx, value, nil)
	if err != nil {
		return fmt.Errorf("uploading blob %s: %w", key, err)
	}
	return nil
}

// Load retrieves the value at key.
func (s *Storage) Load(ctx context.Context, key string) ([]byte, error) {
	blobClient := s.containerClient.NewBlobClient(key)

	response, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		// Check if blob doesn't exist
		var responseError *azcore.ResponseError
		if errors.As(err, &responseError) && responseError.StatusCode == 404 {
			return nil, fs.ErrNotExist
		}
		return nil, fmt.Errorf("downloading blob %s: %w", key, err)
	}
	defer response.Body.Close()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("reading blob %s: %w", key, err)
	}

	return data, nil
}

// Delete deletes key. An error should be returned only if the key still exists when the method returns.
func (s *Storage) Delete(ctx context.Context, key string) error {
	blobClient := s.containerClient.NewBlobClient(key)

	_, err := blobClient.Delete(ctx, nil)
	if err != nil {
		// Check if blob doesn't exist (404 error)
		var responseError *azcore.ResponseError
		if errors.As(err, &responseError) && responseError.StatusCode == 404 {
			// Blob doesn't exist - this is OK for Delete (idempotent behavior)
			// CertMagic interface: "error should be returned only if key still exists"
			// Since key doesn't exist, we return success
			return nil
		}
		return fmt.Errorf("deleting blob %s: %w", key, err)
	}
	return nil
}

// Exists returns true if the key exists
func (s *Storage) Exists(ctx context.Context, key string) bool {
	blobClient := s.containerClient.NewBlobClient(key)

	_, err := blobClient.GetProperties(ctx, nil)
	return err == nil
}

// List returns all keys that match prefix. If recursive is true, non-terminal keys will be enumerated
// otherwise, only keys prefixed exactly by prefix will be listed.
func (s *Storage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	var names []string

	// For now, use a simple approach with NewListBlobsFlatPager
	pager := s.containerClient.NewListBlobsFlatPager(nil)

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing blobs: %w", err)
		}

		for _, blob := range resp.Segment.BlobItems {
			if blob.Name != nil {
				blobName := *blob.Name

				// Filter by prefix
				if !strings.HasPrefix(blobName, prefix) {
					continue
				}

				// For non-recursive listing, filter out deeper nested paths
				if !recursive && strings.Contains(blobName[len(prefix):], "/") {
					continue
				}

				names = append(names, blobName)
			}
		}
	}

	return names, nil
}

// Stat returns information about key.
func (s *Storage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	var keyInfo certmagic.KeyInfo
	blobClient := s.containerClient.NewBlobClient(key)

	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		// Check if blob doesn't exist
		var responseError *azcore.ResponseError
		if errors.As(err, &responseError) && responseError.StatusCode == 404 {
			return keyInfo, fs.ErrNotExist
		}
		return keyInfo, fmt.Errorf("getting properties for %s: %w", key, err)
	}

	keyInfo.Key = key
	keyInfo.Modified = *props.LastModified
	keyInfo.Size = *props.ContentLength
	keyInfo.IsTerminal = true
	return keyInfo, nil
}

// Lock acquires the lock for key, blocking until the lock can be obtained or an error is returned.
func (s *Storage) Lock(ctx context.Context, key string) error {
	lockKey := s.objLockName(key)

	// Create blob client for the lock blob
	blobClient := s.containerClient.NewBlobClient(lockKey)

	// First, ensure the lock blob exists, try to create an empty lock blob if it doesn't exist
	exists := s.Exists(ctx, lockKey)
	if !exists {
		// Create an empty blob to lease
		blockBlobClient := s.containerClient.NewBlockBlobClient(lockKey)
		_, err := blockBlobClient.UploadBuffer(ctx, []byte(""), nil)
		// Ignore error if blob already exists, Azure will return ConflictError if blob already exists
		_ = err
	}

	// Create lease client
	leaseClient, err := lease.NewBlobClient(blobClient, nil)
	if err != nil {
		return fmt.Errorf("creating lease client for %s: %w", lockKey, err)
	}

	// Try to acquire the lease with retries
	for {
		// Attempt to acquire a lease
		_, err := leaseClient.AcquireLease(ctx, LockExpiration, nil)
		if err == nil {
			// Successfully acquired the lease
			s.activeLocks[key] = leaseClient
			return nil
		}

		// Check if this is a lease conflict (blob already leased)
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.ErrorCode == "LeaseAlreadyPresent" {
			// Wait and retry
			select {
			case <-time.After(LockPollInterval):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			// Some other error occurred
			return fmt.Errorf("acquiring lease on %s: %w", lockKey, err)
		}
	}
}

// Unlock releases the lock for key by releasing the Azure Blob lease.
func (s *Storage) Unlock(ctx context.Context, key string) error {
	// Get the lease client from active locks
	leaseClient, exists := s.activeLocks[key]
	if !exists {
		// Lock was not acquired or already released
		return nil
	}

	// Remove from active locks immediately to prevent double-release
	delete(s.activeLocks, key)

	// Release the lease
	_, err := leaseClient.ReleaseLease(ctx, nil)
	if err != nil {
		// Log error but don't fail - the lease will expire anyway
		return fmt.Errorf("releasing lease for %s: %w", key, err)
	}

	return nil
}

func (s *Storage) objLockName(key string) string {
	return key + ".lock"
}
