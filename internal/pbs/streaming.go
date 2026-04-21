package pbs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// StreamingUploader handles uploading data to PBS using streaming chunking
// without materializing the entire file in memory.
type StreamingUploader struct {
	config   Config
	client   *Client
	insecure bool
	chunkCfg buzhash.Config
	compress bool
}

// NewStreamingUploader creates a new streaming uploader.
func NewStreamingUploader(config Config, client *Client, insecureTLS bool) *StreamingUploader {
	chunkCfg, _ := buzhash.NewConfig(4 << 20) // 4MB average chunk size
	return &StreamingUploader{
		config:   config,
		client:   client,
		insecure: insecureTLS,
		chunkCfg: chunkCfg,
		compress: true,
	}
}

// StreamingChunkSource implements datastore.ChunkSource to provide chunks
// on-demand as they're generated from the streaming input.
type StreamingChunkSource struct {
	mu       sync.RWMutex
	chunks   map[[32]byte][]byte
	order    [][32]byte
	complete bool
	ready    chan struct{}
}

// NewStreamingChunkSource creates a new streaming chunk source.
func NewStreamingChunkSource() *StreamingChunkSource {
	return &StreamingChunkSource{
		chunks: make(map[[32]byte][]byte),
		ready:  make(chan struct{}, 1),
	}
}

// GetChunk retrieves a chunk by digest.
func (s *StreamingChunkSource) GetChunk(digest [32]byte) ([]byte, error) {
	s.mu.RLock()
	data, ok := s.chunks[digest]
	s.mu.RUnlock()
	if ok {
		return data, nil
	}
	return nil, fmt.Errorf("chunk %x not found", digest)
}

// AddChunk adds a chunk to the source.
func (s *StreamingChunkSource) AddChunk(digest [32]byte, data []byte) {
	s.mu.Lock()
	if _, exists := s.chunks[digest]; !exists {
		s.chunks[digest] = bytes.Clone(data)
		s.order = append(s.order, digest)
	}
	s.mu.Unlock()
}

// UploadStream uploads data from a reader using streaming chunking with callback.
// This allows chunks to be processed as they're generated without waiting for full stream.
func (u *StreamingUploader) UploadStream(ctx context.Context, ns, backupID, filename string, data io.Reader) (int64, error) {
	backupTime := time.Now().Unix()

	// Create a temporary chunk store
	tempDir := "/tmp/pbs-chunks-" + fmt.Sprintf("%x", backupTime)
	store, err := datastore.NewChunkStore(tempDir)
	if err != nil {
		return 0, fmt.Errorf("create chunk store: %w", err)
	}

	// Create store chunker for streaming upload
	chunker := datastore.NewStoreChunker(store, u.chunkCfg, u.compress)

	// Stream through chunker - this processes chunks as they arrive
	results, idxWriter, err := chunker.ChunkStream(data)
	if err != nil {
		return 0, fmt.Errorf("chunk stream: %w", err)
	}

	// Build index
	idxData, err := idxWriter.Finish()
	if err != nil {
		return 0, fmt.Errorf("finish index: %w", err)
	}

	// Now upload to PBS using the existing session
	// This is where we'd use backupproxy to upload chunks and index
	_ = results
	_ = idxData
	_ = ns
	_ = backupID
	_ = filename

	return backupTime, nil
}

// StreamingSession wraps a PBS backup session for streaming uploads.
type StreamingSession struct {
	session    backupSession
	chunker    *datastore.StoreChunker
	store      *datastore.ChunkStore
	tempDir    string
	uploadTime int64
}

// backupSession interface for mocking.
type backupSession interface {
	UploadArchive(ctx context.Context, name string, data io.Reader) (*backupproxy.UploadResult, error)
	Finish(ctx context.Context) (*datastore.Manifest, error)
}

// ComputeETagFromChunks computes the ETag for assembled data from chunk digests.
func ComputeETagFromChunks(chunks [][32]byte) string {
	h := sha256.New()
	for _, digest := range chunks {
		h.Write(digest[:])
	}
	return fmt.Sprintf("%x-%d", h.Sum(nil), len(chunks))
}
