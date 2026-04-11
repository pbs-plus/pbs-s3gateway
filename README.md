# pbs-s3gateway

An S3-compatible HTTP gateway that translates S3 API calls into [Proxmox Backup Server](https://pbs.proxmox.com/) operations. Designed to let applications that only speak S3 (like Kubernetes operators) store backups directly in PBS.

## How It Works

```
S3 Client (mariadb-operator, etc.)
  │
  │  S3 HTTP (PUT/GET/DELETE/LIST)
  ▼
┌──────────────────────────┐
│       pbs-s3gateway      │
│                          │
│  S3 bucket = PBS namespace       │
│  S3 key    = encoded backup-id   │
│  S3 object = PBS snapshot (1 blob)│
│                          │
│  Optional: AES-256-GCM   │
│  client-side encryption  │
└──────────┬───────────────┘
           │
           │  PBS HTTP API (REST + H2 backup protocol)
           ▼
┌──────────────────────────┐
│   Proxmox Backup Server  │
│   (datastore)            │
└──────────────────────────┘
```

### Namespace Mapping

PBS namespaces map directly to S3 buckets:

| PBS Concept | S3 Equivalent |
|---|---|
| Top-level namespace (`team-a`) | Bucket (`team-a`) |
| Sub-namespace (`team-a/proj-1`) | Folder prefix (`proj-1/`) |
| Backup group (within a namespace) | Object key (base64url-encoded) |
| Snapshot (within a group) | Object version (latest is served) |

- `GET /` → lists all top-level PBS namespaces as S3 buckets
- `GET /team-a` → lists objects in namespace `team-a`, including objects from sub-namespaces like `team-a/proj-1` appearing under prefix `proj-1/`
- `PUT /team-a/backups/db.sql` → uploads to namespace `team-a`, key `backups/db.sql`

### Key Encoding

S3 keys are encoded into PBS-compatible backup-ids using base64url with a `k` prefix. This is reversible — the original S3 key is recovered on GET/LIST. For keys that exceed 128 characters (PBS's backup-id limit), a SHA-256 hash is used instead (not reversible, requires metadata lookup).

### Upload Protocol

Uploads use PBS's HTTP/2 backup protocol:
1. TLS connection to PBS
2. HTTP/1.1 `Upgrade: proxmox-backup-protocol-v1` → 101 Switching Protocols
3. HTTP/2 on the upgraded connection
4. `POST /blob` with PBS-encoded blob data
5. `POST /finish` to commit the snapshot

Downloads, listings, and deletes use the standard PBS REST API over HTTPS.

### Encryption

Optional AES-256-GCM client-side encryption. When enabled:
- Data is encrypted before upload, decrypted on download
- PBS stores only ciphertext — the server never sees plaintext
- Random 12-byte nonce per object, prepended to ciphertext
- ETags are computed on ciphertext (consistent whether encrypted or not)

Without an encryption key, data passes through unchanged.

## S3 API Support

| Operation | S3 Method | Status |
|---|---|---|
| ListBuckets | `GET /` | Supported |
| ListObjects | `GET /bucket` | Supported (prefix, marker) |
| PutObject | `PUT /bucket/key` | Supported |
| GetObject | `GET /bucket/key` | Supported |
| HeadObject | `HEAD /bucket/key` | Supported |
| DeleteObject | `DELETE /bucket/key` | Supported |

Not supported: multipart upload, presigned URLs, bucket CRUD, ACLs, versioning (always serves latest snapshot).

## Usage

```
pbs-s3gateway [flags]
```

### Flags

| Flag | Env Variable | Description |
|---|---|---|
| `--listen` | `LISTEN` | Listen address (default `:8080`) |
| `--pbs-url` | `PBS_URL` | PBS API base URL (e.g. `https://pbs:8007`) |
| `--pbs-datastore` | `PBS_DATASTORE` | PBS datastore name |
| `--pbs-token` | `PBS_TOKEN` | PBS API token (`TOKENID:SECRET`). Optional when using `--credentials`. |
| `--credentials` | `CREDENTIALS_FILE` | Path to S3 credentials file (JSON mapping accessKeyId → secretAccessKey) |
| `--encryption-key` | `ENCRYPTION_KEY` | AES-256 key, hex-encoded (64 chars). Omit for passthrough. |
| `--insecure-tls` | | Skip TLS certificate verification |

### Authentication

The gateway supports two auth modes:

**Static token** — a single PBS token used for all requests:
```bash
pbs-s3gateway --pbs-token "root@pam!s3gateway:secret"
```

**Credential passthrough** — S3 Access Key ID and Secret are mapped to PBS credentials. Each S3 client authenticates with its own credentials, and the gateway translates them into PBS API tokens.

Supported S3 auth methods: AWS Signature V4, AWS Signature V2, HTTP Basic Auth, query string auth.

#### Creating PBS API Tokens

The gateway needs PBS API tokens with access to the target datastore. You can create tokens via the PBS web UI or API.

**Option A: API token under a user (recommended)**

1. In the PBS web UI, go to **Configuration → Access Control → API Tokens**
2. Click **Add**
3. Set **User** to the token owner (e.g. `root@pam`)
4. Set **Token ID** (e.g. `s3gateway`) — the full token ID becomes `user@realm!tokenid` (e.g. `root@pam!s3gateway`)
5. Uncheck **Privilege Separation** so the token inherits the user's permissions
6. Save the generated **Secret** — it looks like `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`

Then grant the user permissions on the datastore:

1. Go to **Configuration → Access Control → Permissions**
2. Add a permission:
   - **Path**: `/datastore/<datastore-name>`
   - **User**: `root@pam` (or your chosen user)
   - **Role**: `PVEDatastoreUser`

`PVEDatastoreUser` includes `Datastore.Audit` and `Datastore.ReadDatastore` and `Datastore.Backup`, which covers listing, downloading, and uploading. To also allow deletes, use `PVEDatastoreAdmin` or assign `Datastore.Modify` and `Datastore.Prune` additionally.

**Option B: Custom role with minimum permissions**

For finer-grained control, create a dedicated user and role with only the required privileges:

1. Go to **Configuration → Access Control → Roles**, click **Add**:
   - **Role ID**: `S3Gateway`
   - **Privileges**: `Datastore.Audit`, `Datastore.Modify`, `Datastore.Backup`, `Datastore.Prune`

2. Create a user under **Configuration → Access Control → User Management**:
   - **User ID**: `s3gateway@pbs`
   - Set a password

3. Grant the role to the user on the datastore under **Permissions**:
   - **Path**: `/datastore/<datastore-name>`
   - **User**: `s3gateway@pbs`
   - **Role**: `S3Gateway`

4. Create an API token for that user under **API Tokens**:
   - **User**: `s3gateway@pbs`
   - **Token ID**: `s3` (full ID becomes `s3gateway@pbs!s3`)
   - **Privilege Separation**: unchecked

#### Setting up the credentials file

Create `credentials.json` mapping each PBS token ID to its secret:

```json
{
  "root@pam!s3gateway": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

The mapping is: S3 Access Key ID = PBS token ID, S3 Secret Access Key = PBS token secret. The gateway forms `TOKENID:SECRET` as the PBS API token.

For multiple clients, add one entry per token:

```json
{
  "root@pam!team-a": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
  "root@pam!team-b": "11111111-2222-3333-4444-555555555555"
}
```

### Example

```bash
# Start the gateway with credentials passthrough
pbs-s3gateway \
  --listen :8080 \
  --pbs-url https://pbs.example.com:8007 \
  --pbs-datastore backup-store \
  --credentials credentials.json

# Use with any S3 client
aws --endpoint-url http://localhost:8080 s3 cp backup.sql s3://team-a/backups/backup.sql
```

### With mariadb-operator

Configure the backup CRD with real PBS credentials as S3 access key and secret:

```yaml
apiVersion: k8s.mariadb.com/v1alpha1
kind: Backup
metadata:
  name: mariadb-backup
spec:
  mariaDbRef:
    name: mariadb
  s3:
    bucket: team-a
    prefix: mariadb/
    endpoint: http://pbs-s3gateway:8080
    accessKeyId: "root@pam!s3gateway"
    secretAccessKey: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

## Project Structure

```
cmd/pbs-s3gateway/        Entry point
internal/
  auth/                   S3 credential extraction and PBS token mapping
  crypto/                 AES-256-GCM encryption (sync.Pool for nonces, pre-computed cipher)
  gateway/                S3 HTTP handler, routing, namespace mapping
  keymapper/              S3 key <-> PBS backup-id encoding (cached via sync.Map)
  pbs/                    PBS REST client + HTTP/2 upload protocol
  s3/                     S3 XML response types and serialization
```

## Performance

The hot path is optimized for minimal allocations:
- `sync.Pool` for body buffers, ETag formatting, encryption nonces
- Pre-computed AES cipher/AEAD at construction (not per-request)
- Cached key encoding via `sync.Map`
- XML streamed directly to `ResponseWriter` (no intermediate `[]byte`)
- Pre-allocated slices based on API response sizes
- `strconv.FormatInt` and `encoding/hex` instead of `fmt.Sprintf`
- Manual byte scanning in `splitPath` instead of `strings.TrimPrefix`
