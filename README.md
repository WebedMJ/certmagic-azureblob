# Certmagic Storage Backend for Azure Blob Storage

This library allows you to use Azure Blob Storage as key/certificate storage backend for your [Certmagic](https://github.com/caddyserver/certmagic)-enabled HTTPS server.

## Usage

### Azure Storage Account

Recommendations to enhance security:

- **Enable infrastructure encryption** during storage account creation for double encryption at rest.
- **Use customer-managed keys** for storage service encryption and leverage Key Vault key rotation policy.
- **Apply least privilege RBAC controls** - grant only the minimum required permissions (e.g., `Storage Blob Data Contributor` for the container).
- **Use managed identity authentication** where possible and disable storage account key access.
- **Apply storage account network restrictions** - configure firewalls, virtual networks, or private endpoints to limit access.
- **Enable blob versioning and soft delete** to protect against accidental deletion or corruption of certificates.
- **Monitor access** with Azure Storage Analytics and set up alerts for unusual access patterns.

### Caddy

In this section, we create a Caddy config using our Azure Blob Storage backend.

#### Getting started

1. **Set up Azure Storage Account**

   - Create an Azure Storage Account
   - Create a blob container (e.g., `caddy-data`)
   - Get your connection string or configure authentication

2. **Create a `Caddyfile`**

   ```caddy
   {
     storage azureblob {
       account_name YOUR_STORAGE_ACCOUNT
       container_name caddy-data
       connection_string "YOUR_STORAGE_CONNECTION_STRING"
     }
   }

   localhost
   respond "Hello Caddy with Azure Blob Storage!"
   ```

3. **Alternative: Using Managed Identity (recommended for Azure VMs)**

   ```caddy
   {
     storage azureblob {
       account_name myaccount
       container_name caddy-data
       # connection_string omitted - will use managed identity
     }
   }

   localhost
   respond "Hello Caddy with Azure Blob Storage!"
   ```

4. **Start Caddy**

   ```console
   xcaddy run
   ```

5. **Check that it works**

   ```console
   curl https://localhost
   ```

### Azure Storage Emulator (for development)

For local development, you can use Azurite (Azure Storage Emulator):

1. **Start Azurite**

   If using VSCode the simplest way to use Azurite is the [Azurite extension](https://marketplace.visualstudio.com/items?itemName=Azurite.azurite)

2. **Use development connection string**

   ```caddy
   {
     storage azureblob {
       account_name YOUR_AZURITE_ACCOUNT
       container_name caddy-data
       connection_string "YOUR_AZURITE_CONNECTION_STRING"
     }
   }

   localhost
   respond "Hello Caddy with Azurite!"
   ```

### Authentication Methods

This module supports several Azure authentication methods:

1. **Connection String (Simple)**

   ```caddy
   {
   storage azureblob {
       account_name YOUR_STORAGE_ACCOUNT
       container_name certificates
       connection_string "YOUR_STORAGE_CONNECTION_STRING"
   }
   }
   ```

2. **Managed Identity (Recommended for Azure VMs)**

   ```caddy
   {
   storage azureblob {
       account_name YOUR_STORAGE_ACCOUNT
       container_name certificates
       # No connection_string - uses managed identity automatically
   }
   }
   ```

3. **Environment Variables**

   Set these environment variables and omit `connection_string`:

   ```console
   $ export AZURE_STORAGE_ACCOUNT="myaccount"
   $ export AZURE_STORAGE_KEY="......"
   # Or for managed identity:
   $ export AZURE_CLIENT_ID="......"
   $ export AZURE_TENANT_ID="......"
   ```

   The AZURE_CLIENT_ID variable is optional and can be used to supply the client id for a user assigned managed identity. If omitted it will use a system assigned identity by default.

### CertMagic

1. **Add the package**

   ```console
   go get github.com/webedmj/certmagic-azureblob
   ```

2. **Create a `storage.NewStorage` with a `storage.Config`**

   ```golang
   import (
       "context"
       certmagicazureblob "github.com/webedmj/certmagic-azureblob/storage"
   )

   config := certmagicazureblob.Config{
       AccountName:      "YOUR_STORAGE_ACCOUNT",
       ContainerName:    "certificates",
       ConnectionString: "YOUR_STORAGE_CONNECTION_STRING",
       // Or omit ConnectionString to use managed identity
   }

   azureStorage, err := certmagicazureblob.NewStorage(context.Background(), config)
   if err != nil {
       log.Fatal(err)
   }
   ```

3. **Optionally, [register as default storage](https://github.com/caddyserver/certmagic#storage)**

   ```golang
   certmagic.Default.Storage = azureStorage
   ```

## Configuration Options

| Parameter           | Description                                  | Required |
| ------------------- | -------------------------------------------- | -------- |
| `account_name`      | Azure Storage Account name                   | Yes      |
| `container_name`    | Blob container name for storing certificates | Yes      |
| `connection_string` | Azure Storage connection string              | No\*     |

\*When `connection_string` is omitted, the module will attempt to use:

1. Azure Managed Identity (if running on Azure)
2. Environment variables (`AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`)
3. Azure CLI credentials
4. Default Azure credential chain

## Testing

The tests support multiple authentication methods, mirroring the behavior of the real module:

### Local Development with Azurite (Recommended)

**⚠️ Important**: Azurite requires a connection string - it doesn't support Azure CLI or managed identity authentication.

1. **Start Azurite** (Azure Storage Emulator):

   ```console
   # Using Docker
   docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite

   # Or using VS Code extension: "Azurite" by Microsoft
   # Or using npm: npm install -g azurite && azurite
   ```

2. **Configure connection string**:

   ```console
   cp .env.example .env
   # Edit .env and uncomment the AZURE_STORAGE_CONNECTION_STRING line
   # Use the default Azurite connection string provided in the file
   ```

3. **Run tests**:

   ```console
   go test ./...
   ```

### Testing with Real Azure Storage

**✅ Flexible Authentication**: Real Azure storage supports both connection strings and Azure CLI/managed identity.

#### Option 1: Using Connection String

1. **Create `.env` file**:

   ```console
   cp .env.example .env
   ```

2. **Edit `.env`** with your real Azure Storage credentials:

   ```env
   AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT;AccountKey=YOUR_KEY;EndpointSuffix=core.windows.net
   AZURE_STORAGE_ACCOUNT=YOUR_ACCOUNT
   ```

3. **Run tests**:

   ```console
   go test ./...
   ```

#### Option 2: Using Azure CLI (No Connection String Required)

1. **Login to Azure CLI**:

   ```console
   az login
   ```

2. **Set only the account name**:

   ```console
   echo "AZURE_STORAGE_ACCOUNT=your-account-name" > .env
   # Note: No AZURE_STORAGE_CONNECTION_STRING needed!
   ```

3. **Run tests**:

   ```console
   go test ./...
   ```

   Tests will automatically use your Azure CLI credentials.

#### Option 3: Manual Environment Variables

**With Connection String**:

```console
# Windows (PowerShell)
$env:AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."
$env:AZURE_STORAGE_ACCOUNT="your-account-name"
go test ./...

# Linux/macOS
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."
export AZURE_STORAGE_ACCOUNT="your-account-name"
go test ./...
```

**With Azure CLI** (after `az login`):

```console
# Windows (PowerShell)
$env:AZURE_STORAGE_ACCOUNT="your-account-name"
go test ./...

# Linux/macOS
export AZURE_STORAGE_ACCOUNT="your-account-name"
go test ./...
```

### VS Code Testing

Use the provided debug configurations in `.vscode/launch.json`:

- **Debug Tests**: Local testing with Azurite
- **Debug Tests (Real Azure)**: Uses your environment variables
- **Debug Tests (Skip Azurite)**: Skips Azure-dependent tests

### CI/CD

To skip tests in environments without Azure access:

```console
SKIP_AZURITE_TESTS=true go test ./...
```

## License

This module is distributed under [AGPL-3.0-only](LICENSE).
