package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-s3gateway/internal/crypto"
	"github.com/pbs-plus/pbs-s3gateway/internal/keymapper"
	"github.com/pbs-plus/pbs-s3gateway/internal/pbs"
	"github.com/pbs-plus/pbs-s3gateway/internal/s3"
)

// testGateway sets up a full S3 gateway with a mock PBS backend.
type testGateway struct {
	pbs       *mockPBSServer
	handler   *Handler
	server    *httptest.Server
	uploader  *mockUploader
	encryptor *crypto.Encryptor
}

func newTestGateway(t *testing.T) *testGateway {
	t.Helper()
	return newTestGatewayWithEncryption(t, "")
}

func newTestGatewayWithEncryption(t *testing.T, hexKey string) *testGateway {
	t.Helper()
	pbsSrv := newMockPBSServer(t)

	km := keymapper.NewKeyMapper()
	client := pbs.NewClient(pbs.Config{
		BaseURL:   pbsSrv.URL(),
		Datastore: "teststore",
		AuthToken: "test-token",
	})

	uploader := &mockUploader{pbs: pbsSrv}
	enc, err := crypto.NewEncryptor(hexKey)
	if err != nil {
		t.Fatal(err)
	}

	handler := NewHandler(km, client, uploader, enc, nil)

	mux := http.NewServeMux()
	mux.Handle("/", handler)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return &testGateway{
		pbs:       pbsSrv,
		handler:   handler,
		server:    server,
		uploader:  uploader,
		encryptor: enc,
	}
}

// mockUploader simulates uploads by directly writing to the mock PBS server.
type mockUploader struct {
	pbs *mockPBSServer
}

func (m *mockUploader) UploadBlob(_ context.Context, ns, backupID string, data []byte) (int64, error) {
	backupTime := time.Now().Unix()
	m.pbs.addSnapshot(ns, backupID, backupTime, map[string][]byte{
		"data.blob": data,
	})
	return backupTime, nil
}

// --- mock PBS server (namespace-aware) ---

type mockPBSServer struct {
	server     *httptest.Server
	snapshots  map[string]map[string]map[int64]mockSnapshot // ns → backupID → time → snap
	namespaces map[string]bool
	mu         sync.Mutex
	token      string
}

type mockSnapshot struct {
	backupType string
	backupID   string
	backupTime int64
	files      map[string][]byte
}

func newMockPBSServer(t *testing.T) *mockPBSServer {
	t.Helper()
	m := &mockPBSServer{
		snapshots:  make(map[string]map[string]map[int64]mockSnapshot),
		namespaces: make(map[string]bool),
		token:      "test-token",
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api2/json/admin/datastore/teststore/namespaces", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "PBSAPIToken "+m.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		nsList := []map[string]any{}
		for ns := range m.namespaces {
			nsList = append(nsList, map[string]any{"ns": ns})
		}
		json.NewEncoder(w).Encode(map[string]any{"data": nsList})
	})

	mux.HandleFunc("/api2/json/admin/datastore/teststore/groups", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "PBSAPIToken "+m.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		ns := r.URL.Query().Get("ns")
		m.mu.Lock()
		defer m.mu.Unlock()

		groups := []map[string]any{}
		nsData, ok := m.snapshots[ns]
		if !ok {
			json.NewEncoder(w).Encode(map[string]any{"data": groups})
			return
		}
		for id, times := range nsData {
			if len(times) > 0 {
				for _, snap := range times {
					groups = append(groups, map[string]any{
						"backup-type": snap.backupType,
						"backup-id":   id,
						"count":       len(times),
					})
					break
				}
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"data": groups})
	})

	mux.HandleFunc("/api2/json/admin/datastore/teststore/snapshots", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "PBSAPIToken "+m.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		ns := r.URL.Query().Get("ns")

		switch r.Method {
		case http.MethodGet:
			backupID := r.URL.Query().Get("backup-id")
			m.mu.Lock()
			defer m.mu.Unlock()

			snaps := []map[string]any{}
			nsData, ok := m.snapshots[ns]
			if !ok {
				json.NewEncoder(w).Encode(map[string]any{"data": snaps})
				return
			}
			for id, times := range nsData {
				if backupID != "" && id != backupID {
					continue
				}
				for bt, snap := range times {
					files := []map[string]any{}
					for fname, data := range snap.files {
						files = append(files, map[string]any{
							"filename": fname,
							"size":     len(data),
						})
					}
					snaps = append(snaps, map[string]any{
						"backup-type": snap.backupType,
						"backup-id":   id,
						"backup-time": bt,
						"files":       files,
					})
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"data": snaps})

		case http.MethodDelete:
			backupID := r.URL.Query().Get("backup-id")
			backupTimeStr := r.URL.Query().Get("backup-time")
			var backupTime int64
			fmt.Sscanf(backupTimeStr, "%d", &backupTime)
			m.mu.Lock()
			defer m.mu.Unlock()

			nsData, ok := m.snapshots[ns]
			if ok {
				if times, ok := nsData[backupID]; ok {
					delete(times, backupTime)
					if len(times) == 0 {
						delete(nsData, backupID)
					}
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"data": nil})

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api2/json/admin/datastore/teststore/download", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "PBSAPIToken "+m.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		ns := r.URL.Query().Get("ns")
		backupID := r.URL.Query().Get("backup-id")
		backupTimeStr := r.URL.Query().Get("backup-time")
		var backupTime int64
		fmt.Sscanf(backupTimeStr, "%d", &backupTime)
		fileName := r.URL.Query().Get("file-name")

		m.mu.Lock()
		defer m.mu.Unlock()

		nsData, ok := m.snapshots[ns]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		times, ok := nsData[backupID]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		snap, ok := times[backupTime]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		data, ok := snap.files[fileName]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(data)
	})

	m.server = httptest.NewServer(mux)
	t.Cleanup(m.server.Close)
	return m
}

func (m *mockPBSServer) URL() string { return m.server.URL }

func (m *mockPBSServer) addSnapshot(ns, backupID string, backupTime int64, files map[string][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.namespaces[ns] = true
	if m.snapshots[ns] == nil {
		m.snapshots[ns] = make(map[string]map[int64]mockSnapshot)
	}
	if m.snapshots[ns][backupID] == nil {
		m.snapshots[ns][backupID] = make(map[int64]mockSnapshot)
	}
	m.snapshots[ns][backupID][backupTime] = mockSnapshot{
		backupType: "host",
		backupID:   backupID,
		backupTime: backupTime,
		files:      files,
	}
}

// --- Tests ---

func TestGatewayPutAndGet(t *testing.T) {
	gw := newTestGateway(t)
	data := []byte("hello world")

	req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/mybucket/backups/test.sql", bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT: status %d, body: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	resp, err = http.Get(gw.server.URL + "/mybucket/backups/test.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET: status %d, body: %s", resp.StatusCode, body)
	}

	got, _ := io.ReadAll(resp.Body)
	if string(got) != string(data) {
		t.Errorf("GET data = %q, want %q", string(got), string(data))
	}
}

func TestGatewayGetNotFound(t *testing.T) {
	gw := newTestGateway(t)

	resp, err := http.Get(gw.server.URL + "/mybucket/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET missing key: status %d, want 404", resp.StatusCode)
	}
}

func TestGatewayDelete(t *testing.T) {
	gw := newTestGateway(t)

	data := []byte("to be deleted")
	req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/mybucket/test.txt", bytes.NewReader(data))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodDelete, gw.server.URL+"/mybucket/test.txt", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("DELETE: status %d, want 204", resp.StatusCode)
	}

	resp, err = http.Get(gw.server.URL + "/mybucket/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("after delete: status %d, want 404", resp.StatusCode)
	}
}

func TestGatewayList(t *testing.T) {
	gw := newTestGateway(t)

	for _, name := range []string{"backups/a.sql", "backups/b.sql", "logs/c.log"} {
		data := []byte("data-" + name)
		req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/mybucket/"+name, bytes.NewReader(data))
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
	}

	// List all
	resp, err := http.Get(gw.server.URL + "/mybucket/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var result s3.ListBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse XML: %v", err)
	}
	if len(result.Contents) != 3 {
		t.Errorf("list count = %d, want 3", len(result.Contents))
	}

	// List with prefix
	result = s3.ListBucketResult{}
	resp, err = http.Get(gw.server.URL + "/mybucket/?prefix=backups/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Contents) != 2 {
		t.Errorf("list with prefix 'backups/': count = %d, want 2", len(result.Contents))
	}
}

func TestGatewayHeadObject(t *testing.T) {
	gw := newTestGateway(t)

	data := make([]byte, 256)
	rand.Read(data)
	req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/mybucket/headtest.bin", bytes.NewReader(data))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodHead, gw.server.URL+"/mybucket/headtest.bin", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("HEAD: status %d, want 200", resp.StatusCode)
	}
}

func TestGatewayPutLargeObject(t *testing.T) {
	gw := newTestGateway(t)

	data := make([]byte, 1<<20)
	rand.Read(data)

	req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/mybucket/large.bin", bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT large: status %d", resp.StatusCode)
	}

	resp, err = http.Get(gw.server.URL + "/mybucket/large.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, data) {
		t.Errorf("large file mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestGatewaySpecialCharacters(t *testing.T) {
	gw := newTestGateway(t)

	key := "backups/backup.2024-01-15T10:30:00Z.sql.gz"
	data := []byte("special chars")

	req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/mybucket/"+key, bytes.NewReader(data))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT special chars: status %d", resp.StatusCode)
	}

	resp, err := http.Get(gw.server.URL + "/mybucket/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET special chars: status %d", resp.StatusCode)
	}
}

func TestGatewayListBuckets(t *testing.T) {
	gw := newTestGateway(t)

	// Upload to two different "buckets" (= PBS namespaces)
	for _, bucket := range []string{"team-a", "team-b"} {
		data := []byte("data-in-" + bucket)
		req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/"+bucket+"/test.txt", bytes.NewReader(data))
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
	}

	// List buckets
	resp, err := http.Get(gw.server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListBuckets: status %d, body: %s", resp.StatusCode, body)
	}

	var result s3.ListAllMyBucketsResult
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse XML: %v", err)
	}

	if len(result.Buckets) != 2 {
		t.Errorf("bucket count = %d, want 2", len(result.Buckets))
	}

	names := map[string]bool{}
	for _, b := range result.Buckets {
		names[b.Name] = true
	}
	if !names["team-a"] || !names["team-b"] {
		t.Errorf("expected team-a and team-b, got %v", result.Buckets)
	}
}

func TestGatewayEncryptionRoundTrip(t *testing.T) {
	key := hex.EncodeToString(make([]byte, 32))
	gw := newTestGatewayWithEncryption(t, key)

	data := []byte("secret data that should be encrypted")

	// PUT
	req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/mybucket/secret.txt", bytes.NewReader(data))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT: status %d", resp.StatusCode)
	}

	// Verify stored data is NOT plaintext (it's encrypted)
	gw.pbs.mu.Lock()
	nsData := gw.pbs.snapshots["mybucket"]
	for _, times := range nsData {
		for _, snap := range times {
			stored := snap.files["data.blob"]
			if string(stored) == string(data) {
				t.Error("stored data should be encrypted, not plaintext")
			}
		}
	}
	gw.pbs.mu.Unlock()

	// GET should return decrypted plaintext
	resp, err := http.Get(gw.server.URL + "/mybucket/secret.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	got, _ := io.ReadAll(resp.Body)
	if string(got) != string(data) {
		t.Errorf("GET = %q, want %q", string(got), string(data))
	}
}

func TestGatewayPassthroughNoKey(t *testing.T) {
	gw := newTestGateway(t) // no encryption key

	data := []byte("plain data")

	req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/mybucket/plain.txt", bytes.NewReader(data))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Stored data should be plaintext
	gw.pbs.mu.Lock()
	nsData := gw.pbs.snapshots["mybucket"]
	found := false
	for _, times := range nsData {
		for _, snap := range times {
			stored := snap.files["data.blob"]
			if string(stored) == string(data) {
				found = true
			}
		}
	}
	gw.pbs.mu.Unlock()
	if !found {
		t.Error("passthrough mode should store plaintext")
	}

	// GET should also return plaintext
	resp, err := http.Get(gw.server.URL + "/mybucket/plain.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != string(data) {
		t.Errorf("GET = %q, want %q", string(got), string(data))
	}
}

func TestGatewayNamespaceIsolation(t *testing.T) {
	gw := newTestGateway(t)

	// Upload same key to two different buckets
	data1 := []byte("data in bucket-a")
	data2 := []byte("data in bucket-b")

	req, _ := http.NewRequest(http.MethodPut, gw.server.URL+"/bucket-a/same-key.txt", bytes.NewReader(data1))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPut, gw.server.URL+"/bucket-b/same-key.txt", bytes.NewReader(data2))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	// GET from bucket-a
	resp, err := http.Get(gw.server.URL + "/bucket-a/same-key.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != string(data1) {
		t.Errorf("bucket-a: got %q, want %q", string(got), string(data1))
	}

	// GET from bucket-b
	resp, err = http.Get(gw.server.URL + "/bucket-b/same-key.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != string(data2) {
		t.Errorf("bucket-b: got %q, want %q", string(got), string(data2))
	}
}

func TestSplitPath(t *testing.T) {
	tests := []struct {
		path   string
		bucket string
		key    string
	}{
		{"/bucket/key", "bucket", "key"},
		{"/bucket/a/b/c", "bucket", "a/b/c"},
		{"/bucket/", "bucket", ""},
		{"/bucket", "bucket", ""},
		{"", "", ""},
		{"/", "", ""},
	}

	for _, tt := range tests {
		b, k := splitPath(tt.path)
		if b != tt.bucket || k != tt.key {
			t.Errorf("splitPath(%q) = (%q, %q), want (%q, %q)", tt.path, b, k, tt.bucket, tt.key)
		}
	}
}
