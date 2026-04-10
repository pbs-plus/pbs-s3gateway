package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-s3gateway/internal/crypto"
	"github.com/pbs-plus/pbs-s3gateway/internal/keymapper"
	"github.com/pbs-plus/pbs-s3gateway/internal/pbs"
	"github.com/pbs-plus/pbs-s3gateway/internal/s3"
)

// BlobUploader uploads data as a blob to PBS.
type BlobUploader interface {
	UploadBlob(ctx context.Context, ns, backupID string, data []byte) (int64, error)
}

// bufPool reuses byte slices for reading request/response bodies.
var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 32*1024)
		return &buf
	},
}

func getBuf() *[]byte  { return bufPool.Get().(*[]byte) }
func putBuf(b *[]byte) { *b = (*b)[:0]; bufPool.Put(b) }

// etagBufPool reuses [64]byte buffers for hex ETag strings.
var etagBufPool = sync.Pool{
	New: func() interface{} {
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
	uploader  BlobUploader
	encryptor *crypto.Encryptor
}

// NewHandler creates a new S3 handler.
func NewHandler(km *keymapper.KeyMapper, client *pbs.Client, uploader BlobUploader, enc *crypto.Encryptor) *Handler {
	return &Handler{
		keyMapper: km,
		client:    client,
		uploader:  uploader,
		encryptor: enc,
	}
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
	namespaces, err := h.client.ListNamespaces(r.Context())
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

	encryptedData, err := h.encryptor.Encrypt(data)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}

	mapping := h.keyMapper.S3ToPBS(key)
	mapping.BackupTime = time.Now().Unix()

	backupTime, err := h.uploader.UploadBlob(r.Context(), bucket, mapping.BackupID, encryptedData)
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
	mapping := h.keyMapper.S3ToPBS(key)

	snaps, err := h.client.ListSnapshots(r.Context(), bucket, mapping.BackupID)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}
	if len(snaps) == 0 {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist.", key, http.StatusNotFound)
		return
	}

	snap := snaps[len(snaps)-1]

	data, err := h.client.Download(r.Context(), bucket, mapping.BackupID, snap.BackupTime, mapping.Filename)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
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
	mapping := h.keyMapper.S3ToPBS(key)

	snaps, err := h.client.ListSnapshots(r.Context(), bucket, mapping.BackupID)
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
	mapping := h.keyMapper.S3ToPBS(key)

	snaps, err := h.client.ListSnapshots(r.Context(), bucket, mapping.BackupID)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}
	if len(snaps) == 0 {
		writeS3Error(w, "NoSuchKey", "The specified key does not exist.", key, http.StatusNotFound)
		return
	}

	snap := snaps[len(snaps)-1]
	if err := h.client.DeleteSnapshot(r.Context(), bucket, mapping.BackupID, snap.BackupTime); err != nil {
		writeS3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	marker := r.URL.Query().Get("marker")

	groups, err := h.client.ListGroups(r.Context(), bucket)
	if err != nil {
		writeS3Error(w, "InternalError", err.Error(), bucket, http.StatusInternalServerError)
		return
	}

	objects := make([]s3.S3Object, 0, len(groups))
	for _, group := range groups {
		s3key := h.keyMapper.PBSToS3(group.BackupID)
		if s3key == "" {
			continue
		}
		if prefix != "" && !strings.HasPrefix(s3key, prefix) {
			continue
		}
		if marker != "" && s3key <= marker {
			continue
		}

		snaps, err := h.client.ListSnapshots(r.Context(), bucket, group.BackupID)
		if err != nil || len(snaps) == 0 {
			continue
		}

		snap := snaps[len(snaps)-1]
		size := int64(0)
		for _, f := range snap.Files {
			size += f.Size
		}

		objects = append(objects, s3.S3Object{
			Key:          s3key,
			Size:         size,
			LastModified: time.Unix(snap.BackupTime, 0),
		})
	}

	// Include objects from sub-namespaces
	namespaces, err := h.client.ListNamespaces(r.Context())
	if err == nil {
		for _, ns := range namespaces {
			if ns.NS == bucket {
				continue
			}
			if !strings.HasPrefix(ns.NS, bucket+"/") {
				continue
			}

			relPath := ns.NS[len(bucket)+1:]
			if strings.Contains(relPath, "/") {
				continue
			}
			if prefix != "" && !strings.HasPrefix(relPath+"/", prefix) && !strings.HasPrefix(prefix, relPath+"/") {
				continue
			}

			subGroups, err := h.client.ListGroups(r.Context(), ns.NS)
			if err != nil {
				continue
			}
			for _, group := range subGroups {
				s3key := h.keyMapper.PBSToS3(group.BackupID)
				if s3key == "" {
					continue
				}
				fullKey := relPath + "/" + s3key
				if prefix != "" && !strings.HasPrefix(fullKey, prefix) {
					continue
				}
				if marker != "" && fullKey <= marker {
					continue
				}

				snaps, err := h.client.ListSnapshots(r.Context(), ns.NS, group.BackupID)
				if err != nil || len(snaps) == 0 {
					continue
				}

				snap := snaps[len(snaps)-1]
				size := int64(0)
				for _, f := range snap.Files {
					size += f.Size
				}

				objects = append(objects, s3.S3Object{
					Key:          fullKey,
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
