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
	LockExpiration      int32 = 60
	LockPollInterval    = 1 * time.Second
	errNoActiveLease    = errors.New("no active lock lease")
)

type activeLease struct {
	leaseClient      *lease.BlobClient
	reneCancel       context.CancelFunc
	acquiredAt       time.Time
	lastRenewedAt    time.Time
	lastRenewRequest time.Duration
}

// Storage is a certmagic.Storage backed by an Azure Blob Storage container

type Storage struct {
	containerClient *container.Client
	activeLocks     map[string]activeLease
	locksMu         sync.Mutex
}

var (
	_ certmagic.Storage          = (*Storage)(nil)
	_ certmagic.Locker           = (*Storage)(nil)
	_ certmagic.LockLeaseRenewer = (*Storage)(nil)
)

//nolint:govet // fieldalignment: struct field order optimized for readability over memory
type Config struct {
	AccountName      string
	ContainerName    string
	ConnectionString string
	Credential       azcore.TokenCredential
}

//nolint:nestif // Functionally correct and readable
func NewStorage(ctx context.Context, config Config) (*Storage, error) {
	var containerClient *container.Client
	var err error

	if config.ConnectionString != "" {
		containerClient, err = container.NewClientFromConnectionString(config.ConnectionString, config.ContainerName, nil)
		if err != nil {
			return nil, fmt.Errorf("could not initialize container client with connection string: %w", err)
		}
	} else {
		var credential azcore.TokenCredential
		if config.Credential != nil {
			credential = config.Credential
		} else {
			credential, err = azidentity.NewDefaultAzureCredential(nil)
			if err != nil {
				return nil, fmt.Errorf("could not create default Azure credential: %w", err)
			}
		}
		accountURL := fmt.Sprintf("https://%s.blob.core.windows.net/", config.AccountName)
		serviceClient, clientErr := azblob.NewClient(accountURL, credential, nil)
		if clientErr != nil {
			return nil, fmt.Errorf("could not initialize service client: %w", clientErr)
		}
		containerClient = serviceClient.ServiceClient().NewContainerClient(config.ContainerName)
	}
	_, err = containerClient.Create(ctx, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if !errors.As(err, &respErr) || respErr.ErrorCode != "ContainerAlreadyExists" {
			return nil, fmt.Errorf("could not create container: %w", err)
		}
	}
	return &Storage{
		containerClient: containerClient,
		activeLocks:     make(map[string]activeLease),
	}, nil
}

// Store puts value at key.
func (s *Storage) Store(ctx context.Context, key string, value []byte) error {
	blockBlobClient := s.containerClient.NewBlockBlobClient(key)
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
		var responseError *azcore.ResponseError
		if errors.As(err, &responseError) && responseError.StatusCode == 404 {
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
	blobClient := s.containerClient.NewBlobClient(lockKey)
	blockBlobClient := s.containerClient.NewBlockBlobClient(lockKey)
	_, uploadErr := blockBlobClient.UploadBuffer(ctx, []byte(""), nil)
	if uploadErr != nil {
		var respErr *azcore.ResponseError
		if !errors.As(uploadErr, &respErr) || (respErr.StatusCode != 409 && respErr.StatusCode != 412) {
			return fmt.Errorf("ensuring lock blob exists %s: %w", lockKey, uploadErr)
		}
	}
	leaseClient, err := lease.NewBlobClient(blobClient, nil)
	if err != nil {
		return fmt.Errorf("creating lease client for %s: %w", lockKey, err)
	}
	lockExp := LockExpiration
	for {
		_, err := leaseClient.AcquireLease(ctx, lockExp, nil)
		if err == nil {
			renewCancel := s.startBackgroundRenewal(leaseClient, lockExp)
			s.locksMu.Lock()
			s.activeLocks[key] = activeLease{
				leaseClient: leaseClient,
				acquiredAt:  time.Now(),
				reneCancel: renewCancel,
			}
			s.locksMu.Unlock()
			return nil
		}
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.ErrorCode == "LeaseAlreadyPresent" {
			select {
			case <-time.After(LockPollInterval):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			return fmt.Errorf("acquiring lease on %s: %w", lockKey, err)
		}
	}
}

// RenewLockLease renews an active lease for the given logical lock key.
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
		return nil
	}
	state.renewCancel()
	_, err := state.leaseClient.ReleaseLease(ctx, nil)
	if err != nil {
		return fmt.Errorf("releasing lease for %s: %w", key, err)
	}
	s.locksMu.Lock()
	delete(s.activeLocks, key)
	s.locksMu.Unlock()
	return nil
}

func (s *Storage) startBackgroundRenewal(leaseClient *lease.BlobClient, lockExpiration int32) context.CancelFunc {
	renewCtx, renewCancel := context.WithCancel(context.Background())
	go s.runLeaseRenewer(renewCtx, leaseClient, lockExpiration)
	return renewCancel
}

func (s *Storage) runLeaseRenewer(ctx context.Context, leaseClient *lease.BlobClient, lockExpiration int32) {
	renewInterval := time.Duration(lockExpiration) * time.Second * 2 / 3
	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, err := leaseClient.RenewLease(context.Background(), nil)
			if err != nil {
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