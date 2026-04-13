package pbs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/datastore"
)

// Client implements PBS REST management API operations.
type Client struct {
	baseURL    string
	baseHost   string
	authHeader string // default auth header "PBSAPIToken <token>"
	authToken  string // raw auth token for PBSReader
	datastore  string
	insecure   bool
	client     *http.Client
}

// NewClient creates a new PBS REST client.
func NewClient(config Config) *Client {
	authHeader := ""
	if config.AuthToken != "" {
		authHeader = "PBSAPIToken " + config.AuthToken
	}

	// Normalize base URL: remove trailing slashes and /api2/json if present
	base := strings.TrimSuffix(config.BaseURL, "/")
	base = strings.TrimSuffix(base, "/api2/json")

	// Configure HTTP client with proper timeouts to prevent hanging
	// These timeouts ensure the client fails fast instead of hanging indefinitely
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	return &Client{
		baseURL:    base + "/api2/json/admin/datastore/" + config.Datastore,
		baseHost:   base,
		authHeader: authHeader,
		authToken:  config.AuthToken, // Raw token for PBSReader
		datastore:  config.Datastore,
		insecure:   config.SkipTLSVerify,
		client:     client,
	}
}

type authCtxKey struct{}

// WithAuthToken returns a context that overrides the PBS auth token for this request.
func WithAuthToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, authCtxKey{}, token)
}

func (c *Client) authForRequest(ctx context.Context) string {
	if v, ok := ctx.Value(authCtxKey{}).(string); ok && v != "" {
		return "PBSAPIToken " + v
	}
	return c.authHeader
}

func (c *Client) doRequest(ctx context.Context, method, path string, params url.Values) (*http.Response, error) {
	rawURL := c.baseURL + path
	if len(params) > 0 {
		rawURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
	if err != nil {
		return nil, err
	}
	if auth := c.authForRequest(ctx); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	return c.client.Do(req)
}

// CreateNamespace creates a new namespace in the datastore.
func (c *Client) CreateNamespace(ctx context.Context, ns string) error {
	params := url.Values{
		"name": {ns},
	}
	resp, err := c.doRequest(ctx, http.MethodPost, "/namespace", params)
	if err != nil {
		return fmt.Errorf("create namespace: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create namespace: HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

// ListNamespaces lists all namespaces in the datastore.
func (c *Client) ListNamespaces(ctx context.Context) ([]Namespace, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/namespace", nil)
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list namespaces: HTTP %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Data []Namespace `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode namespaces: %w", err)
	}
	return result.Data, nil
}

// ListGroups lists all backup groups in the given namespace.
func (c *Client) ListGroups(ctx context.Context, ns string) ([]Group, error) {
	var params url.Values
	if ns != "" {
		params = url.Values{"ns": {ns}}
	}

	resp, err := c.doRequest(ctx, http.MethodGet, "/groups", params)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list groups: HTTP %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Data []Group `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode groups: %w", err)
	}
	return result.Data, nil
}

// ListSnapshots lists snapshots for a specific backup group in the given namespace.
func (c *Client) ListSnapshots(ctx context.Context, ns, backupID string) ([]Snapshot, error) {
	params := url.Values{
		"backup-type": {"host"},
		"backup-id":   {backupID},
	}
	if ns != "" {
		params.Set("ns", ns)
	}

	resp, err := c.doRequest(ctx, http.MethodGet, "/snapshots", params)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list snapshots: HTTP %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Data []Snapshot `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode snapshots: %w", err)
	}
	return result.Data, nil
}

// Download retrieves a file from a PBS snapshot in the given namespace.
func (c *Client) Download(ctx context.Context, ns, backupID string, backupTime int64, filename string) ([]byte, error) {
	params := url.Values{
		"backup-type": {"host"},
		"backup-id":   {backupID},
		"backup-time": {strconv.FormatInt(backupTime, 10)},
		"file-name":   {filename},
	}
	if ns != "" {
		params.Set("ns", ns)
	}

	resp, err := c.doRequest(ctx, http.MethodGet, "/download", params)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("download: HTTP %d: %s", resp.StatusCode, body)
	}
	return io.ReadAll(resp.Body)
}

// DeleteSnapshot deletes a PBS backup snapshot in the given namespace.
func (c *Client) DeleteSnapshot(ctx context.Context, ns, backupID string, backupTime int64) error {
	params := url.Values{
		"backup-type": {"host"},
		"backup-id":   {backupID},
		"backup-time": {strconv.FormatInt(backupTime, 10)},
	}
	if ns != "" {
		params.Set("ns", ns)
	}

	rawURL := c.baseURL + "/snapshots?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, rawURL, nil)
	if err != nil {
		return err
	}
	if auth := c.authForRequest(ctx); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("delete snapshot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete snapshot: HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

// GetOriginalSize returns the original data size for a file in a snapshot.
// It first checks for a metadata sidecar file (.s3meta), and falls back to
// the raw file size if metadata is not found.
func (c *Client) GetOriginalSize(ctx context.Context, ns, backupID string, backupTime int64, filename string, storedSize int64) (int64, error) {
	// Try to read metadata file
	metaName := filename + ".s3meta"
	metaData, err := c.Download(ctx, ns, backupID, backupTime, metaName)
	if err != nil {
		// Metadata not found, return stored size
		return storedSize, nil
	}

	var meta struct {
		OriginalSize int64 `json:"original_size"`
	}
	if err := json.Unmarshal(metaData, &meta); err != nil {
		// Invalid metadata, fall back to stored size
		return storedSize, nil
	}

	if meta.OriginalSize > 0 {
		return meta.OriginalSize, nil
	}
	return storedSize, nil
}

// DownloadChunked downloads a chunked file (.didx) using PBSReader via HTTP/2
// backup reader protocol. Now with namespace support (pxar v0.2.1+).
func (c *Client) DownloadChunked(ctx context.Context, ns, backupID string, backupTime int64, filename string) ([]byte, error) {
	// Create PBSConfig with namespace support
	storeConfig := backupproxy.PBSConfig{
		BaseURL:       c.baseHost + "/api2/json",
		Datastore:     c.datastore,
		AuthToken:     c.authToken,
		Namespace:     ns, // Now supported in pxar v0.2.1+
		SkipTLSVerify: c.insecure,
	}

	// Create and connect PBSReader
	reader := backupproxy.NewPBSReader(storeConfig, "host", backupID, backupTime)
	if err := reader.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect reader: %w", err)
	}
	defer reader.Close()

	// Download the index file
	didxData, err := reader.DownloadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("download index: %w", err)
	}

	// Parse the dynamic index
	idx, err := datastore.ReadDynamicIndex(didxData)
	if err != nil {
		return nil, fmt.Errorf("parse index: %w", err)
	}

	// Reassemble data from chunks using PBSReader
	var result bytes.Buffer
	for i := 0; i < idx.Count(); i++ {
		entry := idx.Entry(i)
		digest := entry.Digest

		// Download the chunk using PBSReader
		chunkData, err := reader.DownloadChunk(digest)
		if err != nil {
			return nil, fmt.Errorf("download chunk %d: %w", i, err)
		}

		// Decode the blob wrapper
		decodedChunk, err := datastore.DecodeBlob(chunkData)
		if err != nil {
			return nil, fmt.Errorf("decode chunk %d: %w", i, err)
		}

		result.Write(decodedChunk)
	}

	return result.Bytes(), nil
}

// HealthCheck verifies PBS is reachable by calling the /api2/json/ping endpoint.
// Expects response: {"data":{"pong":true}}
func (c *Client) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseHost+"/api2/json/ping", nil)
	if err != nil {
		return err
	}

	// Include auth if available (required for PBS 4.x)
	if auth := c.authForRequest(ctx); auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PBS ping failed: HTTP %d: %s", resp.StatusCode, body)
	}

	// Verify response contains {"data":{"pong":true}}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read ping response: %w", err)
	}

	var result struct {
		Data struct {
			Pong bool `json:"pong"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("invalid ping response: %w", err)
	}

	if !result.Data.Pong {
		return fmt.Errorf("unexpected ping response: pong=%v", result.Data.Pong)
	}

	return nil
}
