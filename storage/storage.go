package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/caddyserver/certmagic"
)

var (
	// LockExpiration in seconds is the Azure lease duration for lock blobs.
	// Azure Blob fixed lease durations must be in [15, 60] seconds (or -1 for infinite lease).
	// Use the max fixed duration and rely on retries when contention exists.
	LockExpiration int32 = 60
	// LockPollInterval is the interval between lease acquisition retries
	LockPollInterval = 1 * time.Second

	errNoActiveLease = errors.New("no active lock lease")
)

type activeLease struct {
	leaseClient      *lease.BlobClient
	renewCancel      context.CancelFunc
	acquiredAt       time.Time
	lastRenewedAt    time.Time
	lastRenewRequest time.Duration
}

// Storage is a certmagic.Storage backed by an Azure Blob Storage container
type Storage struct {
	containerClient *container.Client
	// activeLocks tracks active lease state per logical lock key.
	activeLocks map[string]activeLease
	locksMu     sync.Mutex
}

// Interface guards
var (
	_ certmagic.Storage          = (*Storage)(nil)
	_ certmagic.Locker           = (*Storage)(nil)
	_ certmagic.LockLeaseRenewer = (*Storage)(nil)
)

type Config struct {
	// Credential can be used for authentication (managed identity, etc.)
	Credential azcore.TokenCredential
	// AccountName is the Azure Storage account name
	AccountName string
	// ContainerName is the name of the Azure Blob Storage container
	ContainerName string
	// ConnectionString is the Azure Storage connection string (optional)
	ConnectionString string
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
		activeLocks:     make(map[string]activeLease),
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
	// Check if key is a file or directory
	info, err := s.Stat(ctx, key)
	var keysToDelete []string
	switch {
	case err == nil:
		// Assume that this is a file and it exists, so delete it directly
		keysToDelete = []string{key}
	case errors.Is(err, fs.ErrNotExist):
		// If Stat returns not exist, treat as already deleted for files (terminal=true)
		if info.IsTerminal {
			// File does not exist, idempotent
			return nil
		} else {
			childKeys, listErr := s.List(ctx, key, true)
			if listErr != nil {
				// If listing fails, treat as already deleted
				return nil
			}
			if len(childKeys) == 0 {
				return nil
			}
			keysToDelete = childKeys
		}
	default:
		return fmt.Errorf("stat for delete %s: %w", key, err)
	}

	var deleteErrs []string
	for _, delKey := range keysToDelete {
		blobClient := s.containerClient.NewBlobClient(delKey)
		_, err := blobClient.Delete(ctx, nil)
		if err != nil {
			var responseError *azcore.ResponseError
			if errors.As(err, &responseError) && responseError.StatusCode == 404 {
				continue // Already deleted
			}
			deleteErrs = append(deleteErrs, fmt.Sprintf("%s: %v", delKey, err))
		}
	}
	if len(deleteErrs) > 0 {
		return fmt.Errorf("errors deleting blobs: %s", strings.Join(deleteErrs, "; "))
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

	pager := s.containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing blobs: %w", err)
		}

		for _, blob := range resp.Segment.BlobItems {
			if blob.Name != nil {
				blobName := *blob.Name

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
			// This will return a KeyInfo with IsTerminal=false
			// which is appropriate for non-existent keys on Azure blob (could be a directory or a missing file)
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

	// Ensure the lock blob exists. Always attempt creation unconditionally to avoid a
	// TOCTOU race. Two response codes are expected and safe to ignore:
	// 	 409 Conflict      — blob already exists (created by another caller first)
	// 	 412 Precondition  — blob exists and is currently leased; we cannot overwrite
	// 			it without a lease ID, but it already exists so we proceed.
	// Any other error is a genuine failure and is returned to the caller.
	blockBlobClient := s.containerClient.NewBlockBlobClient(lockKey)
	_, uploadErr := blockBlobClient.UploadBuffer(ctx, []byte(""), nil)
	if uploadErr != nil {
		var respErr *azcore.ResponseError
		if !errors.As(uploadErr, &respErr) || (respErr.StatusCode != 409 && respErr.StatusCode != 412) {
			return fmt.Errorf("ensuring lock blob exists %s: %w", lockKey, uploadErr)
		}
		// 409 Conflict or 412 Precondition: blob already exists or is leased — either way it exists, which is all we need.
	}

	// Create lease client
	leaseClient, err := lease.NewBlobClient(blobClient, nil)
	if err != nil {
		return fmt.Errorf("creating lease client for %s: %w", lockKey, err)
	}

	// Capture the current LockExpiration value for use by the background goroutine,
	// avoiding a data race if the global is modified concurrently (e.g. in tests).
	lockExp := LockExpiration

	// Try to acquire the lease with retries
	for {
		// Attempt to acquire a lease
		_, err := leaseClient.AcquireLease(ctx, lockExp, nil)
		if err == nil {
			// Successfully acquired the lease. Start a background goroutine to keep it alive.
			renewCancel := s.startBackgroundRenewal(leaseClient, lockExp)
			s.locksMu.Lock()
			s.activeLocks[key] = activeLease{
				leaseClient: leaseClient,
				acquiredAt:  time.Now(),
				renewCancel: renewCancel,
			}
			s.locksMu.Unlock()
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

// RenewLockLease renews an active lease for the given logical lock key.
// This only succeeds for a currently-held lock in this process.
func (s *Storage) RenewLockLease(ctx context.Context, lockKey string, leaseDuration time.Duration) error {
	s.locksMu.Lock()
	state, exists := s.activeLocks[lockKey]
	s.locksMu.Unlock()
	if !exists {
		return fmt.Errorf("renewing lease for %s: %w", lockKey, errNoActiveLease)
	}

	_, err := state.leaseClient.RenewLease(ctx, nil)
	if err != nil {
		return fmt.Errorf("renewing lease for %s: %w", lockKey, err)
	}

	// Cancel the old background goroutine and start a fresh one to reset the renewal timer.
	// Capture LockExpiration for the new goroutine to avoid a data race on the global.
	state.renewCancel()
	renewCancel := s.startBackgroundRenewal(state.leaseClient, LockExpiration)

	s.locksMu.Lock()
	current, ok := s.activeLocks[lockKey]
	if ok {
		current.lastRenewedAt = time.Now()
		current.lastRenewRequest = leaseDuration
		current.renewCancel = renewCancel
		s.activeLocks[lockKey] = current
	} else {
		// Lock was released while we were renewing; stop the new goroutine immediately.
		renewCancel()
	}
	s.locksMu.Unlock()

	return nil
}

// Unlock releases the lock for key by releasing the Azure Blob lease.
func (s *Storage) Unlock(ctx context.Context, key string) error {
	s.locksMu.Lock()
	state, exists := s.activeLocks[key]
	s.locksMu.Unlock()
	if !exists {
		// Lock was not acquired or already released
		return nil
	}

	// Stop the background renewal goroutine before releasing the lease.
	state.renewCancel()

	// Release the lease
	_, err := state.leaseClient.ReleaseLease(ctx, nil)
	if err != nil {
		return fmt.Errorf("releasing lease for %s: %w", key, err)
	}

	s.locksMu.Lock()
	delete(s.activeLocks, key)
	s.locksMu.Unlock()

	return nil
}

// startBackgroundRenewal creates a cancellable context, launches a goroutine that
// periodically renews the Azure blob lease, and returns the cancel function.
// Isolating context.Background() here avoids gosec G118 warnings in callers that
// have a request-scoped context in scope.
func (s *Storage) startBackgroundRenewal(leaseClient *lease.BlobClient, lockExpiration int32) context.CancelFunc {
	renewCtx, renewCancel := context.WithCancel(context.Background())
	go s.runLeaseRenewer(renewCtx, leaseClient, lockExpiration)
	return renewCancel
}

// runLeaseRenewer runs in a goroutine and periodically renews the Azure blob lease
// at a safe interval (roughly 2/3 of the lease duration) to prevent it from expiring.
// It stops when ctx is cancelled (e.g., on Unlock or RenewLockLease restart) or on
// renewal error.
func (s *Storage) runLeaseRenewer(ctx context.Context, leaseClient *lease.BlobClient, lockExpiration int32) {
	renewInterval := time.Duration(lockExpiration) * time.Second * 2 / 3
	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, err := leaseClient.RenewLease(context.Background(), nil)
			if err != nil {
				// Stop renewing on error to avoid spinning on a broken lease.
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Storage) objLockName(key string) string {
	return key + ".lock"
}
