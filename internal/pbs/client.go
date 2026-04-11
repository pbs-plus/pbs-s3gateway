package pbs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// Client implements PBS REST management API operations.
type Client struct {
	baseURL    string
	baseHost   string
	authHeader string // default auth (static token)
	client     *http.Client
}

// NewClient creates a new PBS REST client.
func NewClient(config Config) *Client {
	authHeader := ""
	if config.AuthToken != "" {
		authHeader = "PBSAPIToken " + config.AuthToken
	}
	return &Client{
		baseURL:    config.BaseURL + "/api2/json/admin/datastore/" + config.Datastore,
		baseHost:   config.BaseURL,
		authHeader: authHeader,
		client:     &http.Client{},
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

// ListNamespaces lists all namespaces in the datastore.
func (c *Client) ListNamespaces(ctx context.Context) ([]Namespace, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/namespaces", nil)
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

// HealthCheck verifies PBS is reachable. With a static token it calls
// ListNamespaces; in passthrough mode (no static token) it does an
// unauthenticated GET to the PBS API root to confirm TCP/TLS connectivity.
func (c *Client) HealthCheck(ctx context.Context) error {
	if c.authHeader != "" {
		_, err := c.ListNamespaces(ctx)
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseHost+"/api2/json/version", nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}