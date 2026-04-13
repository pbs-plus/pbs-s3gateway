package pbs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// FileMetadata stores metadata about an uploaded file for accurate S3 listing.
type FileMetadata struct {
	OriginalSize  int64 `json:"original_size"`            // Original data size before chunking/encoding
	EncryptedSize int64 `json:"encrypted_size,omitempty"` // Size after encryption
	UploadTime    int64 `json:"upload_time"`              // Unix timestamp
	IsChunked     bool  `json:"is_chunked"`               // Whether this is a .didx file
}

// metaSuffix is the suffix for metadata files.
const metaSuffix = ".s3meta"

const (
	// DefaultChunkThreshold is the size above which files are uploaded
	// as chunked archives with .didx files instead of simple blobs.
	// Set to 4MB as a good balance between overhead and deduplication benefits.
	DefaultChunkThreshold = 4 * 1024 * 1024
)

// Uploader handles uploading data to PBS via the HTTP/2 backup protocol.
type Uploader struct {
	config         Config
	client         *Client
	insecure       bool
	chunkThreshold int64
}

// NewUploader creates a new PBS uploader.
func NewUploader(config Config, client *Client, insecureTLS bool) *Uploader {
	return &Uploader{
		config:         config,
		client:         client,
		insecure:       insecureTLS,
		chunkThreshold: DefaultChunkThreshold,
	}
}

// NewUploaderWithThreshold creates a new PBS uploader with custom chunk threshold.
// chunkThreshold is the minimum file size (in bytes) to use chunked archive upload.
func NewUploaderWithThreshold(config Config, client *Client, insecureTLS bool, chunkThreshold int64) *Uploader {
	return &Uploader{
		config:         config,
		client:         client,
		insecure:       insecureTLS,
		chunkThreshold: chunkThreshold,
	}
}

func (u *Uploader) authForRequest(ctx context.Context) string {
	if v, ok := ctx.Value(authCtxKey{}).(string); ok && v != "" {
		return v
	}
	return u.config.AuthToken
}

// createSession creates a new PBS backup session with the given configuration.
func (u *Uploader) createSession(ctx context.Context, ns, backupID string, backupTime int64) (backupproxy.BackupSession, error) {
	// Ensure BaseURL ends with /api2/json for pxar's backupproxy
	baseURL := strings.TrimSuffix(u.config.BaseURL, "/")
	if !strings.HasSuffix(baseURL, "/api2/json") {
		baseURL += "/api2/json"
	}

	storeConfig := backupproxy.PBSConfig{
		BaseURL:       baseURL,
		Datastore:     u.config.Datastore,
		AuthToken:     u.authForRequest(ctx),
		SkipTLSVerify: u.insecure,
	}

	backupConfig := backupproxy.BackupConfig{
		BackupType: datastore.BackupHost,
		BackupID:   backupID,
		BackupTime: backupTime,
		Namespace:  ns,
	}

	store := backupproxy.NewPBSRemoteStore(storeConfig, buzhash.DefaultConfig(), true)
	session, err := store.StartSession(ctx, backupConfig)
	if err != nil {
		// Auto-create namespace if it doesn't exist (404)
		errMsg := fmt.Sprintf("%v", err)
		if ns != "" && strings.Contains(errMsg, "404") && strings.Contains(errMsg, "namespace not found") {
			if createErr := u.client.CreateNamespace(ctx, ns); createErr == nil {
				session, err = store.StartSession(ctx, backupConfig)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("start session: %w", err)
		}
	}

	return session, nil
}

// Upload uploads data to PBS, automatically choosing between blob and archive upload
// based on the size threshold. For files larger than the threshold, it uses chunked
// archive upload which produces .didx files and enables PBS deduplication.
// The filename parameter determines the archive name (e.g., "data.pxar" produces "data.pxar.didx").
func (u *Uploader) Upload(ctx context.Context, ns, backupID, filename string, size int64, data io.Reader) (int64, error) {
	backupTime := time.Now().Unix()

	// For small files, buffer and use blob upload for efficiency
	if size < u.chunkThreshold && size >= 0 {
		buf, err := io.ReadAll(data)
		if err != nil {
			return 0, fmt.Errorf("read data: %w", err)
		}
		return u.UploadBlobWithMetadata(ctx, ns, backupID, filename, buf, int64(len(buf)))
	}

	// For large files or unknown size, use streaming archive upload
	return u.UploadArchive(ctx, ns, backupID, filename, backupTime, size, data)
}

// UploadBlob uploads data as a blob to PBS within a new backup snapshot.
// This is efficient for small files but doesn't provide chunk-level deduplication.
func (u *Uploader) UploadBlob(ctx context.Context, ns, backupID, filename string, data []byte) (int64, error) {
	return u.UploadBlobWithMetadata(ctx, ns, backupID, filename, data, int64(len(data)))
}

// UploadBlobWithMetadata uploads a blob with a sidecar metadata file for accurate size tracking.
func (u *Uploader) UploadBlobWithMetadata(ctx context.Context, ns, backupID, filename string, data []byte, originalSize int64) (int64, error) {
	backupTime := time.Now().Unix()

	session, err := u.createSession(ctx, ns, backupID, backupTime)
	if err != nil {
		return 0, err
	}

	// Use the actual filename instead of generic "data.blob"
	uploadName := filename
	if uploadName == "" {
		uploadName = "data.blob"
	}

	if err := session.UploadBlob(ctx, uploadName, data); err != nil {
		session.Finish(ctx)
		return 0, fmt.Errorf("upload blob: %w", err)
	}

	// Create and upload metadata file for accurate S3 listing
	meta := FileMetadata{
		OriginalSize: originalSize,
		UploadTime:   backupTime,
		IsChunked:    false,
	}
	if err := u.uploadMetadata(ctx, session, uploadName, meta); err != nil {
		// Log but don't fail - metadata is optional
		// session.Finish(ctx) will be called below
	}

	if _, err := session.Finish(ctx); err != nil {
		return 0, fmt.Errorf("finish: %w", err)
	}

	return backupTime, nil
}

// UploadArchive uploads data as a chunked archive to PBS, producing a .didx file.
// This enables:
//   - Content-defined chunking for deduplication
//   - Streaming upload (lower memory usage)
//   - Resume capability for failed uploads
//   - Proper .didx index files for PBS management
//
// The filename parameter determines the archive name with .didx extension included.
func (u *Uploader) UploadArchive(ctx context.Context, ns, backupID, filename string, backupTime int64, originalSize int64, data io.Reader) (int64, error) {
	session, err := u.createSession(ctx, ns, backupID, backupTime)
	if err != nil {
		return 0, err
	}

	// Determine archive name - must include .didx extension
	archiveName := filename
	if archiveName == "" {
		archiveName = "data.didx"
	}

	// UploadArchive streams the data, chunks it using buzhash, and creates a .didx file
	result, err := session.UploadArchive(ctx, archiveName, data)
	if err != nil {
		session.Finish(ctx)
		return 0, fmt.Errorf("upload archive: %w", err)
	}

	// Create and upload metadata file with original size for accurate S3 listing
	meta := FileMetadata{
		OriginalSize:  originalSize,
		EncryptedSize: int64(result.Size), // This is the index file size
		UploadTime:    backupTime,
		IsChunked:     true,
	}
	if err := u.uploadMetadata(ctx, session, archiveName, meta); err != nil {
		// Log but don't fail - metadata is optional
	}

	if _, err := session.Finish(ctx); err != nil {
		return 0, fmt.Errorf("finish: %w", err)
	}

	return backupTime, nil
}

// uploadMetadata creates and uploads a metadata sidecar file.
func (u *Uploader) uploadMetadata(ctx context.Context, session backupproxy.BackupSession, filename string, meta FileMetadata) error {
	metaData, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	metaName := filename + metaSuffix
	if err := session.UploadBlob(ctx, metaName, metaData); err != nil {
		return fmt.Errorf("upload metadata: %w", err)
	}
	return nil
}

// SetChunkThreshold sets the size threshold for automatic upload method selection.
// Files larger than this will use chunked archive upload with .didx generation.
func (u *Uploader) SetChunkThreshold(threshold int64) {
	u.chunkThreshold = threshold
}

// GetChunkThreshold returns the current chunk threshold.
func (u *Uploader) GetChunkThreshold() int64 {
	return u.chunkThreshold
}
