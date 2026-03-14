package certmagicazureblob

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"github.com/webedmj/certmagic-azureblob/storage"
)

// Interface guards
var (
	_ caddyfile.Unmarshaler  = (*CaddyStorageAzureBlob)(nil)
	_ caddy.StorageConverter = (*CaddyStorageAzureBlob)(nil)
	_ caddy.Provisioner      = (*CaddyStorageAzureBlob)(nil)
)

// CaddyStorageAzureBlob implements a caddy storage backend for Azure Blob Storage.
//
//nolint:govet // fieldalignment: struct field order optimized for readability over memory
type CaddyStorageAzureBlob struct {
	// AccountName is the Azure Storage account name.
	AccountName string `json:"account_name"`
	// ContainerName is the name of the blob container.
	ContainerName string `json:"container_name"`
	// ConnectionString is the Azure Storage connection string (optional).
	ConnectionString string `json:"connection_string,omitempty"`
	// Credential can be used for authentication (managed identity, etc.)
	Credential azcore.TokenCredential `json:"-"`
}

func init() {
	caddy.RegisterModule(CaddyStorageAzureBlob{})
}

// CaddyModule returns the Caddy module information.
func (CaddyStorageAzureBlob) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.azureblob",
		New: func() caddy.Module {
			return new(CaddyStorageAzureBlob)
		},
	}
}

// CertMagicStorage returns a cert-magic storage.
func (s *CaddyStorageAzureBlob) CertMagicStorage() (certmagic.Storage, error) {
	config := storage.Config{
		AccountName:      s.AccountName,
		ContainerName:    s.ContainerName,
		ConnectionString: s.ConnectionString,
		Credential:       s.Credential,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return storage.NewStorage(ctx, config)
}

// Provision sets up the Azure Blob Storage module and validates configuration.
func (s *CaddyStorageAzureBlob) Provision(ctx caddy.Context) error {
	return s.Validate()
}

// Validate Azure Blob Storage configuration.
func (s *CaddyStorageAzureBlob) Validate() error {
	if s.AccountName == "" {
		return fmt.Errorf("account name must be defined")
	}
	if s.ContainerName == "" {
		return fmt.Errorf("container name must be defined")
	}
	return nil
}

// Unmarshall caddy file.
func (s *CaddyStorageAzureBlob) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		key := d.Val()
		var value string

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "account_name":
			s.AccountName = value
		case "container_name":
			s.ContainerName = value
		case "connection_string":
			s.ConnectionString = value
		default:
			return d.Errf("unrecognised option '%s'", key)
		}
	}
	return nil
}
