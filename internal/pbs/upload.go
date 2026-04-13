package pbs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
)

// Uploader handles uploading data to PBS via the HTTP/2 backup protocol.
type Uploader struct {
	config Config
	client *Client
	insecure bool
}

// NewUploader creates a new PBS uploader.
func NewUploader(config Config, client *Client, insecureTLS bool) *Uploader {
	return &Uploader{
		config: config,
		client: client,
		insecure: insecureTLS,
	}
}

func (u *Uploader) authForRequest(ctx context.Context) string {
	if v, ok := ctx.Value(authCtxKey{}).(string); ok && v != "" {
		return v
	}
	return u.config.AuthToken
}

// UploadBlob uploads data as a blob to PBS within a new backup snapshot.
func (u *Uploader) UploadBlob(ctx context.Context, ns, backupID string, data []byte) (int64, error) {
	backupTime := time.Now().Unix()

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
			return 0, fmt.Errorf("start session: %w", err)
		}
	}

	if err := session.UploadBlob(ctx, "data.blob", data); err != nil {
		session.Finish(ctx)
		return 0, fmt.Errorf("upload blob: %w", err)
	}

	if _, err := session.Finish(ctx); err != nil {
		return 0, fmt.Errorf("finish: %w", err)
	}

	return backupTime, nil
}
