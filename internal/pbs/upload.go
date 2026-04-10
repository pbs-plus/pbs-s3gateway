package pbs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/http2"

	"github.com/pbs-plus/pxar/datastore"
)

// Uploader handles uploading data to PBS via the HTTP/2 backup protocol.
type Uploader struct {
	config  Config
	tlsCfg  *tls.Config
	h2cPool map[string]*http2.ClientConn
}

// NewUploader creates a new PBS uploader.
func NewUploader(config Config, insecureTLS bool) *Uploader {
	tlsCfg := &tls.Config{}
	if insecureTLS {
		tlsCfg.InsecureSkipVerify = true
	}
	return &Uploader{
		config:  config,
		tlsCfg:  tlsCfg,
		h2cPool: make(map[string]*http2.ClientConn),
	}
}

// UploadBlob uploads data as a blob to PBS within a new backup snapshot.
// Returns the backup-time of the created snapshot.
func (u *Uploader) UploadBlob(ctx context.Context, ns, backupID string, data []byte) (int64, error) {
	backupTime := time.Now().Unix()

	h2conn, cleanup, err := u.startBackupSession(ctx, ns, backupID, backupTime)
	if err != nil {
		return 0, fmt.Errorf("start session: %w", err)
	}
	defer cleanup()

	blob, err := datastore.EncodeBlob(data)
	if err != nil {
		return 0, fmt.Errorf("encode blob: %w", err)
	}
	blobData := blob.Bytes()

	if err := u.uploadBlobH2(ctx, h2conn, "data.blob", blobData); err != nil {
		return 0, fmt.Errorf("upload blob: %w", err)
	}

	if err := u.finishBackup(ctx, h2conn); err != nil {
		return 0, fmt.Errorf("finish: %w", err)
	}

	return backupTime, nil
}

// startBackupSession establishes an H2 connection to PBS and starts a backup.
func (u *Uploader) startBackupSession(ctx context.Context, ns, backupID string, backupTime int64) (*http2.ClientConn, func(), error) {
	parsed, err := url.Parse(u.config.BaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse URL: %w", err)
	}
	addr := parsed.Host

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 30 * time.Second}, "tcp", addr, u.tlsCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("tls dial %s: %w", addr, err)
	}

	upgradeReq := fmt.Sprintf("GET /api2/json/backup?store=%s&backup-type=host&backup-id=%s&backup-time=%d",
		u.config.Datastore, backupID, backupTime)
	if ns != "" {
		upgradeReq += fmt.Sprintf("&ns=%s", url.QueryEscape(ns))
	}
	upgradeReq += fmt.Sprintf(" HTTP/1.1\r\nHost: %s\r\nAuthorization: PBSAPIToken %s\r\nConnection: upgrade\r\nUpgrade: proxmox-backup-protocol-v1\r\n\r\n",
		addr, u.config.AuthToken)

	if _, err := conn.Write([]byte(upgradeReq)); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("write upgrade: %w", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("read response: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, nil, fmt.Errorf("expected 101, got %d", resp.StatusCode)
	}

	var netConn net.Conn = conn
	if reader.Buffered() > 0 {
		netConn = &bufferedConn{Conn: conn, reader: reader}
	}

	tr := &http2.Transport{}
	h2conn, err := tr.NewClientConn(netConn)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("h2 handshake: %w", err)
	}

	cleanup := func() {
		h2conn.Close()
		conn.Close()
	}

	return h2conn, cleanup, nil
}

// uploadBlobH2 uploads a blob via the H2 backup protocol.
func (u *Uploader) uploadBlobH2(ctx context.Context, h2conn *http2.ClientConn, filename string, data []byte) error {
	req := &http.Request{
		Method: "POST",
		URL: &url.URL{
			Path: fmt.Sprintf("/blob?file-name=%s&encoded-size=%d", filename, len(data)),
		},
		Header: http.Header{
			"Content-Type": []string{"application/octet-stream"},
		},
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: int64(len(data)),
	}
	req = req.WithContext(ctx)

	resp, err := h2conn.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("post blob: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("blob upload: HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

// finishBackup sends the finish command to PBS.
func (u *Uploader) finishBackup(ctx context.Context, h2conn *http2.ClientConn) error {
	req := &http.Request{
		Method: "POST",
		URL:    &url.URL{Path: "/finish"},
		Body:   http.NoBody,
	}
	req = req.WithContext(ctx)

	resp, err := h2conn.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("post finish: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("finish: HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

// bufferedConn wraps a net.Conn to drain buffered data from a bufio.Reader.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	if c.reader.Buffered() > 0 {
		return c.reader.Read(b)
	}
	return c.Conn.Read(b)
}

// ParsePBSResponse parses a PBS JSON response.
func ParsePBSResponse(data []byte) (json.RawMessage, error) {
	var resp struct {
		Data   json.RawMessage `json:"data"`
		Errors json.RawMessage `json:"errors,omitempty"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Errors != nil {
		return nil, fmt.Errorf("PBS errors: %s", resp.Errors)
	}
	return resp.Data, nil
}
