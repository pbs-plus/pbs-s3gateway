package s3

import (
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	// maxInMemoryPart is the size threshold for keeping parts in memory vs temp files.
	// Set to 5MB to match S3 minimum part size and minimize memory usage.
	maxInMemoryPart = 5 * 1024 * 1024 // 5MB

	// tempDir is where multipart parts are stored if they exceed maxInMemoryPart.
	tempDir = "/tmp/pbs-s3gateway-multipart"
)

// Part represents a single uploaded part - either in memory or on disk.
type Part struct {
	PartNumber   int
	ETag         string
	Size         int64
	data         []byte // In-memory data (nil if on disk)
	tempFile     string // Path to temp file (empty if in memory)
	lastModified time.Time
}

// Reader returns an io.ReadCloser for the part data.
// Caller must close the reader when done.
func (p *Part) Reader() (io.ReadCloser, error) {
	if p.data != nil {
		return io.NopCloser(&byteReader{data: p.data, pos: 0}), nil
	}
	if p.tempFile != "" {
		return os.Open(p.tempFile)
	}
	return nil, fmt.Errorf("part has no data")
}

// Cleanup removes the temp file if one exists.
func (p *Part) Cleanup() {
	if p.tempFile != "" {
		_ = os.Remove(p.tempFile)
		p.tempFile = ""
	}
	p.data = nil
}

// byteReader is a simple in-memory reader that doesn't allocate.
type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// MultipartUpload represents an in-progress multipart upload.
type MultipartUpload struct {
	UploadID  string
	Bucket    string
	Key       string
	CreatedAt time.Time
	mu        sync.RWMutex
	Parts     map[int]*Part
	Completed bool
	tempDir   string // Per-upload temp directory
}

// CreateMultipartUploadResponse is the response for CreateMultipartUpload.
type CreateMultipartUploadResponse struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// CompleteMultipartUploadRequest is the request body for CompleteMultipartUpload.
type CompleteMultipartUploadRequest struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart `xml:"Part"`
}

// CompletePart represents a part in the complete request.
type CompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CompleteMultipartUploadResponse is the response for CompleteMultipartUpload.
type CompleteMultipartUploadResponse struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location,omitempty"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// ListMultipartUploadsResponse is the response for ListMultipartUploads.
type ListMultipartUploadsResponse struct {
	XMLName            xml.Name     `xml:"ListMultipartUploadsResult"`
	Bucket             string       `xml:"Bucket"`
	KeyMarker          string       `xml:"KeyMarker"`
	UploadIDMarker     string       `xml:"UploadIdMarker"`
	NextKeyMarker      string       `xml:"NextKeyMarker"`
	NextUploadIDMarker string       `xml:"NextUploadIdMarker"`
	MaxUploads         int          `xml:"MaxUploads"`
	IsTruncated        bool         `xml:"IsTruncated"`
	Uploads            []UploadInfo `xml:"Upload"`
}

// UploadInfo represents an in-progress upload in ListMultipartUploads.
type UploadInfo struct {
	Key       string    `xml:"Key"`
	UploadID  string    `xml:"UploadId"`
	Initiated time.Time `xml:"Initiated"`
}

// ListPartsResponse is the response for ListParts.
type ListPartsResponse struct {
	XMLName              xml.Name   `xml:"ListPartsResult"`
	Bucket               string     `xml:"Bucket"`
	Key                  string     `xml:"Key"`
	UploadID             string     `xml:"UploadId"`
	PartNumberMarker     int        `xml:"PartNumberMarker"`
	NextPartNumberMarker int        `xml:"NextPartNumberMarker"`
	MaxParts             int        `xml:"MaxParts"`
	IsTruncated          bool       `xml:"IsTruncated"`
	Parts                []PartInfo `xml:"Part"`
}

// PartInfo represents a part in ListParts response.
type PartInfo struct {
	PartNumber   int       `xml:"PartNumber"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	LastModified time.Time `xml:"LastModified"`
}

// Manager handles multipart upload state.
type Manager struct {
	mu      sync.RWMutex
	uploads map[string]*MultipartUpload
}

// NewManager creates a new multipart upload manager.
func NewManager() *Manager {
	// Ensure temp directory exists
	_ = os.MkdirAll(tempDir, 0755)

	m := &Manager{
		uploads: make(map[string]*MultipartUpload),
	}
	// Start cleanup goroutine
	go m.cleanupLoop()
	return m
}

// CreateMultipartUpload creates a new multipart upload.
func (m *Manager) CreateMultipartUpload(bucket, key string) (*MultipartUpload, error) {
	uploadID := generateUploadID()

	// Create a dedicated temp directory for this upload
	uploadTempDir := filepath.Join(tempDir, uploadID)
	if err := os.MkdirAll(uploadTempDir, 0755); err != nil {
		return nil, fmt.Errorf("create upload temp dir: %w", err)
	}

	upload := &MultipartUpload{
		UploadID:  uploadID,
		Bucket:    bucket,
		Key:       key,
		CreatedAt: time.Now(),
		Parts:     make(map[int]*Part),
		tempDir:   uploadTempDir,
	}

	m.mu.Lock()
	m.uploads[upload.UploadID] = upload
	m.mu.Unlock()

	return upload, nil
}

// GetMultipartUpload retrieves an upload by ID.
func (m *Manager) GetMultipartUpload(uploadID string) (*MultipartUpload, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	upload, ok := m.uploads[uploadID]
	return upload, ok
}

// RemoveMultipartUpload removes an upload and cleans up its files.
func (m *Manager) RemoveMultipartUpload(uploadID string) {
	m.mu.Lock()
	upload, ok := m.uploads[uploadID]
	if ok {
		delete(m.uploads, uploadID)
	}
	m.mu.Unlock()

	if ok {
		upload.Cleanup()
	}
}

// AbortMultipartUpload aborts and removes an upload.
func (m *Manager) AbortMultipartUpload(uploadID string) bool {
	m.RemoveMultipartUpload(uploadID)
	return true
}

// ListMultipartUploads lists uploads for a bucket.
func (m *Manager) ListMultipartUploads(bucket string) []UploadInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var uploads []UploadInfo
	for _, upload := range m.uploads {
		if upload.Bucket == bucket {
			uploads = append(uploads, UploadInfo{
				Key:       upload.Key,
				UploadID:  upload.UploadID,
				Initiated: upload.CreatedAt,
			})
		}
	}
	return uploads
}

// AddPart adds a part to an upload. Uses temp files for large parts, memory for small ones.
// Returns the computed ETag for the part.
func (u *MultipartUpload) AddPart(partNumber int, data io.Reader, size int64) (string, error) {
	h := sha256.New()

	// Calculate ETag while streaming to destination
	var part *Part

	if size > 0 && size <= maxInMemoryPart {
		// Small part: read into memory with hash calculation
		buf := make([]byte, size)
		n, err := io.ReadFull(data, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return "", fmt.Errorf("read part data: %w", err)
		}
		buf = buf[:n]
		h.Write(buf)

		part = &Part{
			PartNumber:   partNumber,
			ETag:         fmt.Sprintf("%x", h.Sum(nil)),
			Size:         int64(n),
			data:         buf,
			lastModified: time.Now(),
		}
	} else {
		// Large part: stream to temp file with hash calculation
		tempFile := filepath.Join(u.tempDir, fmt.Sprintf("part-%d", partNumber))
		f, err := os.Create(tempFile)
		if err != nil {
			return "", fmt.Errorf("create temp file: %w", err)
		}

		// Copy with hash calculation
		n, err := io.Copy(f, io.TeeReader(data, h))
		_ = f.Close()

		if err != nil {
			_ = os.Remove(tempFile)
			return "", fmt.Errorf("write temp file: %w", err)
		}

		part = &Part{
			PartNumber:   partNumber,
			ETag:         fmt.Sprintf("%x", h.Sum(nil)),
			Size:         n,
			tempFile:     tempFile,
			lastModified: time.Now(),
		}
	}

	u.mu.Lock()
	// Remove old part if it exists (overwrite semantics)
	if oldPart, exists := u.Parts[partNumber]; exists {
		oldPart.Cleanup()
	}
	u.Parts[partNumber] = part
	u.mu.Unlock()

	return part.ETag, nil
}

// GetPart retrieves a part by number.
func (u *MultipartUpload) GetPart(partNumber int) (*Part, bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	part, ok := u.Parts[partNumber]
	return part, ok
}

// ListParts returns all parts sorted by part number.
func (u *MultipartUpload) ListParts() []*Part {
	u.mu.RLock()
	defer u.mu.RUnlock()

	parts := make([]*Part, 0, len(u.Parts))
	for _, part := range u.Parts {
		parts = append(parts, part)
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	return parts
}

// MultiReader returns an io.ReadCloser that reads all parts sequentially.
// This is a zero-copy streaming approach - it doesn't allocate for the entire file.
func (u *MultipartUpload) MultiReader() (io.ReadCloser, error) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if u.Completed {
		return nil, fmt.Errorf("upload already completed")
	}

	// Get sorted part numbers
	partNumbers := make([]int, 0, len(u.Parts))
	for pn := range u.Parts {
		partNumbers = append(partNumbers, pn)
	}
	sort.Ints(partNumbers)

	// Create readers for each part
	readers := make([]io.Reader, 0, len(partNumbers))
	for _, pn := range partNumbers {
		part := u.Parts[pn]
		r, err := part.Reader()
		if err != nil {
			// Clean up already opened readers
			return nil, fmt.Errorf("open part %d: %w", pn, err)
		}
		readers = append(readers, r)
	}

	// io.MultiReader chains the readers without buffering
	return &multiReadCloser{
		Reader: io.MultiReader(readers...),
		parts:  readers,
	}, nil
}

// multiReadCloser wraps io.MultiReader and closes all underlying readers.
type multiReadCloser struct {
	io.Reader
	parts []io.Reader
}

func (m *multiReadCloser) Close() error {
	for _, r := range m.parts {
		if rc, ok := r.(io.Closer); ok {
			_ = rc.Close()
		}
	}
	return nil
}

// Cleanup removes all temp files and marks upload as completed.
func (u *MultipartUpload) Cleanup() {
	u.mu.Lock()
	u.Completed = true
	for _, part := range u.Parts {
		part.Cleanup()
	}
	u.Parts = nil
	u.mu.Unlock()

	// Remove temp directory
	if u.tempDir != "" {
		_ = os.RemoveAll(u.tempDir)
	}
}

// ComputeETag creates the final ETag for a completed multipart upload.
// S3 uses MD5 of concatenated part MD5s + "-N" where N is part count.
func (u *MultipartUpload) ComputeETag() string {
	parts := u.ListParts()
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(part.ETag))
	}
	return fmt.Sprintf("%x-%d", h.Sum(nil), len(parts))
}

// generateUploadID creates a unique upload ID.
func generateUploadID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

// cleanupLoop removes expired uploads periodically.
func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		m.cleanup()
	}
}

// cleanup removes uploads older than 24 hours.
func (m *Manager) cleanup() {
	m.mu.Lock()
	uploadsToRemove := make([]string, 0)
	cutoff := time.Now().Add(-24 * time.Hour)

	for id, upload := range m.uploads {
		if upload.CreatedAt.Before(cutoff) {
			uploadsToRemove = append(uploadsToRemove, id)
		}
	}
	m.mu.Unlock()

	// Remove outside the lock to avoid holding it during cleanup
	for _, id := range uploadsToRemove {
		m.RemoveMultipartUpload(id)
	}
}

// WriteCreateMultipartUploadResponse writes the XML response.
func WriteCreateMultipartUploadResponse(w io.Writer, bucket, key, uploadID string) error {
	resp := CreateMultipartUploadResponse{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	return enc.Encode(resp)
}

// WriteCompleteMultipartUploadResponse writes the XML response.
func WriteCompleteMultipartUploadResponse(w io.Writer, bucket, key, etag string) error {
	resp := CompleteMultipartUploadResponse{
		Bucket: bucket,
		Key:    key,
		ETag:   fmt.Sprintf("\"%s\"", etag),
	}
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	return enc.Encode(resp)
}

// WriteListPartsResponse writes the XML response for ListParts.
func WriteListPartsResponse(w io.Writer, bucket, key, uploadID string, parts []*Part) error {
	resp := ListPartsResponse{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		MaxParts: 1000,
	}

	for _, p := range parts {
		resp.Parts = append(resp.Parts, PartInfo{
			PartNumber:   p.PartNumber,
			ETag:         p.ETag,
			Size:         p.Size,
			LastModified: p.lastModified,
		})
	}
	if len(parts) > 0 {
		resp.NextPartNumberMarker = parts[len(parts)-1].PartNumber
	}

	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	return enc.Encode(resp)
}
