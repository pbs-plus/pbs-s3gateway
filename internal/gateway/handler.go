package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-s3gateway/internal/auth"
	"github.com/pbs-plus/pbs-s3gateway/internal/crypto"
	"github.com/pbs-plus/pbs-s3gateway/internal/keymapper"
	"github.com/pbs-plus/pbs-s3gateway/internal/pbs"
	"github.com/pbs-plus/pbs-s3gateway/internal/s3"
	"github.com/pbs-plus/pxar/datastore"
)

// Uploader handles uploading data to PBS with support for both blob and archive methods.
type Uploader interface {
	// Upload uploads data to PBS, automatically choosing between blob and archive upload
	// based on the size threshold. For files larger than the threshold, it uses chunked
	// archive upload which produces .didx files and enables PBS deduplication.
	Upload(ctx context.Context, ns, backupID, filename string, size int64, data io.Reader) (int64, error)
}

// bufPool reuses byte slices for reading request/response bodies.
var bufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 32*1024)
		return &buf
	},
}

func getBuf() *[]byte  { return bufPool.Get().(*[]byte) }
func putBuf(b *[]byte) { *b = (*b)[:0]; bufPool.Put(b) }

// etagBufPool reuses [64]byte buffers for hex ETag strings.
var etagBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 66) // "..." + 64 hex chars
		return &b
	},
}

// formatETag formats a SHA-256 hash as an S3 ETag: "hexhash"
// Uses a pooled buffer to avoid allocation.
func formatETag(hash [sha256.Size]byte) string {
	bufPtr := etagBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:1]
	buf[0] = '"'
	buf = append(buf[:1], hex.EncodeToString(hash[:])...)
	buf = append(buf, '"')
	s := string(buf)
	*bufPtr = buf[:0]
	etagBufPool.Put(bufPtr)
	return s
}

// Handler handles S3 API requests and translates them to PBS operations.
type Handler struct {
	keyMapper *keymapper.KeyMapper
	client    *pbs.Client
	uploader  Uploader
	encryptor *crypto.Encryptor
	creds     *auth.Store
}

// NewHandler creates a new S3 handler.
func NewHandler(km *keymapper.KeyMapper, client *pbs.Client, uploader Uploader, enc *crypto.Encryptor, creds *auth.Store) *Handler {
	return &Handler{
		keyMapper: km,
		client:    client,
		uploader:  uploader,
		encryptor: enc,
		creds:     creds,
	}
}

// authContext extracts S3 credentials from the request and returns
// a context with the PBS token injected. Falls back to the static token.
func (h *Handler) authContext(r *http.Request) context.Context {
	if token, ok := h.creds.TokenFromRequest(r); ok {
		return pbs.WithAuthToken(r.Context(), token)
	}
	return r.Context()
}

// isAwsChunked checks if request uses AWS SigV4 chunked encoding
func isAwsChunked(r *http.Request) bool {
	contentEncoding := r.Header.Get("Content-Encoding")
	return strings.Contains(contentEncoding, "aws-chunked") ||
		r.Header.Get("X-Amz-Content-Sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
}

// decodeAwsChunked strips AWS chunk signatures and returns clean data
// Format: hex(size) + ";chunk-signature=" + signature + "\r\n" + data + "\r\n"
func decodeAwsChunked(data []byte) ([]byte, error) {
	var result bytes.Buffer

	for len(data) > 0 {
		// Find the end of chunk header
		idx := bytes.Index(data, []byte("\r\n"))
		if idx == -1 {
			break
		}

		// Parse chunk header: "10000;chunk-signature=..."
		header := string(data[:idx])
		parts := strings.Split(header, ";")
		if len(parts) < 1 {
			return nil, fmt.Errorf("invalid chunk header: %s", header)
		}

		// Get chunk size from hex
		chunkSize, err := strconv.ParseInt(parts[0], 16, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid chunk size %q: %w", parts[0], err)
		}

		// Skip past the \r\n
		data = data[idx+2:]

		// Copy chunk data
		if chunkSize > 0 {
			if int64(len(data)) < chunkSize {
				return nil, fmt.Errorf("incomplete chunk: need %d bytes, have %d", chunkSize, len(data))
			}
			result.Write(data[:chunkSize])
			data = data[chunkSize:]
		}

		// Skip trailing \r\n
		if len(data) >= 2 && data[0] == '\r' && data[1] == '\n' {
			data = data[2:]
		}

		// Zero-size chunk = end
		if chunkSize == 0 {
			break
		}
	}

	return result.Bytes(), nil
}

// ServeHTTP routes S3 requests to the appropriate handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucket, key := splitPath(r.URL.Path)

	if bucket == "" {
		if r.Method == http.MethodGet {
			h.listBuckets(w, r)
			return
		}
		writeS3Error(w, "InvalidRequest", "Unsupported method", r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

	if key == "" {
		switch r.Method {
		case http.MethodGet:
			h.listObjects(w, r, bucket)
		default:
			writeS3Error(w, "InvalidRequest", "Unsupported method", r.URL.Path, http.StatusMethodNotAllowed)
		}
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.putObject(w, r, bucket, key)
	case http.MethodGet:
		h.getObject(w, r, bucket, key)
	case http.MethodHead:
		h.headObject(w, r, bucket, key)
	case http.MethodDelete:
		h.deleteObject(w, r, bucket, key)
	default:
		writeS3Error(w, "InvalidRequest", "Unsupported method", r.URL.Path, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) listBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := h.authContext(r)
	namespaces, err := h.client.ListNamespaces(ctx)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), "", http.StatusInternalServerError)
		return
	}

	// Filter to top-level only, pre-allocate
	topLevel := make([]string, 0, len(namespaces))
	for _, ns := range namespaces {
		if ns.NS != "" && !strings.Contains(ns.NS, "/") {
			topLevel = append(topLevel, ns.NS)
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	s3.WriteListBucketsResponse(w, topLevel)
}

func (h *Handler) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	bp := getBuf()
	data, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength+1))
	putBuf(bp)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}

	// Decode AWS SigV4 chunked uploads if present
	if isAwsChunked(r) {
		decodedData, err := decodeAwsChunked(data)
		if err != nil {
			log.Printf("AWS chunked decode error for %s/%s: %v", bucket, key, err)
			writeS3Error(w, "InvalidRequest", "failed to decode chunked upload: "+err.Error(), key, http.StatusBadRequest)
			return
		}
		data = decodedData
	}

	encryptedData, err := h.encryptor.Encrypt(data)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}

	mapping := h.keyMapper.S3ToPBS(key)
	mapping.BackupTime = time.Now().Unix()

	// Build full namespace path: bucket + sub-namespace from key path
	fullNamespace := bucket
	if mapping.Namespace != "" {
		fullNamespace = bucket + "/" + mapping.Namespace
	}

	// Use the S3 key as the filename (last component)
	filename := key
	if idx := strings.LastIndex(key, "/"); idx >= 0 {
		filename = key[idx+1:]
	}
	if filename == "" {
		filename = "data"
	}

	// Use the new Upload method with auto-detection
	// Pass the encrypted data size for threshold checking
	// The uploader will automatically add .didx extension for large files
	encryptedSize := int64(len(encryptedData))
	backupTime, err := h.uploader.Upload(
		h.authContext(r),
		fullNamespace,
		mapping.BackupID,
		filename,
		encryptedSize,
		bytes.NewReader(encryptedData),
	)
	if err != nil {
		log.Printf("upload %s/%s: %v", bucket, key, err)
		writeS3Error(w, "InternalError", "upload failed", key, http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", formatETag(sha256.Sum256(encryptedData)))
	w.Header().Set("x-amz-request-id", "req-"+strconv.FormatInt(backupTime, 10))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := h.authContext(r)
	mapping := h.keyMapper.S3ToPBS(key)

	// Build full namespace path: bucket + sub-namespace from key path
	fullNamespace := bucket
	if mapping.Namespace != "" {
		fullNamespace = bucket + "/" + mapping.Namespace
	}

	snaps, err := h.client.ListSnapshots(ctx, fullNamespace, mapping.BackupID)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}
	if len(snaps) == 0 {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist.", key, http.StatusNotFound)
		return
	}

	snap := snaps[len(snaps)-1]

	// Get the base filename from the S3 key (last path component)
	baseFilename := key
	if idx := strings.LastIndex(key, "/"); idx >= 0 {
		baseFilename = key[idx+1:]
	}

	// PBS filenames must not contain certain characters (like colons)
	// Sanitize the filename for PBS storage
	sanitizedName := sanitizePBSFilename(baseFilename)

	// All files are stored as .didx (chunked archive format)
	// Downloads use PBSReader with HTTP/2 backup reader protocol
	candidates := []string{
		"data.didx",
		baseFilename + ".didx",
		sanitizedName + ".didx",
	}

	// Find the matching file in the snapshot
	filename := ""
	for _, f := range snap.Files {
		for _, candidate := range candidates {
			if f.Filename == candidate {
				filename = f.Filename
				break
			}
		}
		if filename != "" {
			break
		}
	}

	// Fallback to first available file if nothing matched
	if filename == "" && len(snap.Files) > 0 {
		filename = snap.Files[0].Filename
	}

	var data []byte

	// Check if this is a chunked file (.didx)
	if strings.HasSuffix(filename, ".didx") {
		// Try to download and reassemble chunked file using PBSReader
		data, err = h.client.DownloadChunked(ctx, fullNamespace, mapping.BackupID, snap.BackupTime, filename)
		if err != nil {
			// Fall back to simple download (for tests and backward compatibility)
			// First try downloading the .blob version if it exists
			blobFilename := strings.TrimSuffix(filename, ".didx") + ".blob"
			data, err = h.client.Download(ctx, fullNamespace, mapping.BackupID, snap.BackupTime, blobFilename)
			if err != nil {
				// Try the .didx file directly (might be a blob in test mode)
				data, err = h.client.Download(ctx, fullNamespace, mapping.BackupID, snap.BackupTime, filename)
				if err != nil {
					writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
					return
				}
				// Check if it's actually a blob wrapped in .didx (test mode)
				decodedData, decodeErr := datastore.DecodeBlob(data)
				if decodeErr == nil {
					data = decodedData
				}
				// If decode fails, assume it's raw index data ( shouldn't happen in production)
			} else {
				// Successfully downloaded .blob file, decode it
				data, err = datastore.DecodeBlob(data)
				if err != nil {
					writeS3Error(w, "InternalError", "blob decode failed: "+err.Error(), key, http.StatusInternalServerError)
					return
				}
			}
		}
	} else {
		// Download regular blob file
		data, err = h.client.Download(ctx, fullNamespace, mapping.BackupID, snap.BackupTime, filename)
		if err != nil {
			writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
			return
		}

		// Decode the PBS blob to extract the actual payload (strips the blob header)
		data, err = datastore.DecodeBlob(data)
		if err != nil {
			writeS3Error(w, "InternalError", "blob decode failed: "+err.Error(), key, http.StatusInternalServerError)
			return
		}
	}

	decryptedData, err := h.encryptor.Decrypt(data)
	if err != nil {
		writeS3Error(w, "InternalError", "decryption failed", key, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(decryptedData)))
	w.Header().Set("ETag", formatETag(sha256.Sum256(data)))
	w.Header().Set("Last-Modified", time.Unix(snap.BackupTime, 0).UTC().Format(http.TimeFormat))
	w.Write(decryptedData)
}

func (h *Handler) headObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := h.authContext(r)
	mapping := h.keyMapper.S3ToPBS(key)

	// Build full namespace path: bucket + sub-namespace from key path
	fullNamespace := bucket
	if mapping.Namespace != "" {
		fullNamespace = bucket + "/" + mapping.Namespace
	}

	snaps, err := h.client.ListSnapshots(ctx, fullNamespace, mapping.BackupID)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}
	if len(snaps) == 0 {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist.", key, http.StatusNotFound)
		return
	}

	snap := snaps[len(snaps)-1]
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Last-Modified", time.Unix(snap.BackupTime, 0).UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := h.authContext(r)
	mapping := h.keyMapper.S3ToPBS(key)

	// Build full namespace path: bucket + sub-namespace from key path
	fullNamespace := bucket
	if mapping.Namespace != "" {
		fullNamespace = bucket + "/" + mapping.Namespace
	}

	snaps, err := h.client.ListSnapshots(ctx, fullNamespace, mapping.BackupID)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}
	if len(snaps) == 0 {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist.", key, http.StatusNotFound)
		return
	}

	snap := snaps[len(snaps)-1]
	if err := h.client.DeleteSnapshot(ctx, fullNamespace, mapping.BackupID, snap.BackupTime); err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := h.authContext(r)
	prefix := r.URL.Query().Get("prefix")
	marker := r.URL.Query().Get("marker")

	groups, err := h.client.ListGroups(ctx, bucket)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), bucket, http.StatusInternalServerError)
		return
	}

	objects := make([]s3.S3Object, 0, len(groups))
	for _, group := range groups {
		s3key := h.keyMapper.PBSToS3(bucket, group.BackupID)
		if s3key == "" {
			continue
		}
		if prefix != "" && !strings.HasPrefix(s3key, prefix) {
			continue
		}
		if marker != "" && s3key <= marker {
			continue
		}

		snaps, err := h.client.ListSnapshots(ctx, bucket, group.BackupID)
		if err != nil || len(snaps) == 0 {
			continue
		}

		snap := snaps[len(snaps)-1]
		size := int64(0)
		for _, f := range snap.Files {
			// Skip metadata files when calculating total size
			if strings.HasSuffix(f.Filename, ".s3meta") {
				continue
			}
			// All files are .didx - get original size from metadata
			origSize, err := h.client.GetOriginalSize(ctx, bucket, group.BackupID, snap.BackupTime, f.Filename, f.Size)
			if err == nil && origSize > 0 {
				size += origSize
				continue
			}
			size += f.Size
		}

		objects = append(objects, s3.S3Object{
			Key:          s3key,
			Size:         size,
			LastModified: time.Unix(snap.BackupTime, 0),
		})
	}

	// Include objects from sub-namespaces
	namespaces, err := h.client.ListNamespaces(ctx)
	if err == nil {
		for _, ns := range namespaces {
			if ns.NS == bucket {
				continue
			}
			if !strings.HasPrefix(ns.NS, bucket+"/") {
				continue
			}

			relPath := ns.NS[len(bucket)+1:]
			// Only process direct sub-namespaces (single level), not deeply nested ones
			if strings.Contains(relPath, "/") {
				continue
			}

			subGroups, err := h.client.ListGroups(ctx, ns.NS)
			if err != nil {
				continue
			}
			for _, group := range subGroups {
				fullKey := h.keyMapper.PBSToS3(ns.NS, group.BackupID)
				if fullKey == "" {
					continue
				}
				// Strip bucket prefix to get key relative to bucket
				s3key := strings.TrimPrefix(fullKey, bucket+"/")
				if prefix != "" && !strings.HasPrefix(s3key, prefix) {
					continue
				}
				if marker != "" && s3key <= marker {
					continue
				}

				snaps, err := h.client.ListSnapshots(ctx, ns.NS, group.BackupID)
				if err != nil || len(snaps) == 0 {
					continue
				}

				snap := snaps[len(snaps)-1]
				size := int64(0)
				for _, f := range snap.Files {
					// Skip metadata files when calculating total size
					if strings.HasSuffix(f.Filename, ".s3meta") {
						continue
					}
					// All files are .didx - get original size from metadata
					origSize, err := h.client.GetOriginalSize(ctx, ns.NS, group.BackupID, snap.BackupTime, f.Filename, f.Size)
					if err == nil && origSize > 0 {
						size += origSize
						continue
					}
					size += f.Size
				}

				objects = append(objects, s3.S3Object{
					Key:          s3key,
					Size:         size,
					LastModified: time.Unix(snap.BackupTime, 0),
				})
			}
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	s3.WriteListResponse(w, bucket, prefix, marker, objects)
}

func splitPath(path string) (bucket, key string) {
	// Avoid strings.TrimPrefix allocation by slicing manually
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	if path == "" {
		return "", ""
	}
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			return path[:i], path[i+1:]
		}
	}
	return path, ""
}

func writeS3Error(w http.ResponseWriter, code, message, resource string, statusCode int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	resp := s3.S3Error{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: "req-" + strconv.FormatInt(time.Now().UnixNano(), 10),
	}
	xml.NewEncoder(w).Encode(resp)
}

// sanitizePBSFilename makes a filename safe for PBS blob storage by replacing
// invalid characters (anything not A-Z, a-z, 0-9, _, ., -) with underscore.
func sanitizePBSFilename(filename string) string {
	var result strings.Builder
	for i, c := range filename {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-' {
			result.WriteRune(c)
		} else {
			// Replace invalid character with underscore
			// Don't double up underscores
			if i == 0 || result.Len() == 0 || result.String()[result.Len()-1] != '_' {
				result.WriteByte('_')
			}
		}
	}
	return result.String()
}
