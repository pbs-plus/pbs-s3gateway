package pbs

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// mockPBSServer simulates PBS REST management API with namespace support.
type mockPBSServer struct {
	server     *httptest.Server
	snapshots  map[string]map[string]map[int64]mockSnapshot // ns → backupID → backupTime → snapshot
	namespaces map[string]bool
	mu         sync.Mutex
	token      string
	store      string
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
		store:      "teststore",
	}

	mux := http.NewServeMux()

	// List namespaces
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
		writeJSON(w, map[string]any{"data": nsList})
	})

	// List groups
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
			writeJSON(w, map[string]any{"data": groups})
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
		writeJSON(w, map[string]any{"data": groups})
	})

	// List/Delete snapshots
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
				writeJSON(w, map[string]any{"data": snaps})
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
			writeJSON(w, map[string]any{"data": snaps})

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
			writeJSON(w, map[string]any{"data": nil})

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	// Download file
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

func (m *mockPBSServer) URL() string {
	return m.server.URL
}

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

func newTestPBSClient(t *testing.T, srv *mockPBSServer) *Client {
	t.Helper()
	return NewClient(Config{
		BaseURL:   srv.URL(),
		Datastore: "teststore",
		AuthToken: "test-token",
	})
}

func TestPBSListNamespaces(t *testing.T) {
	srv := newMockPBSServer(t)
	client := newTestPBSClient(t, srv)

	srv.addSnapshot("team-a", "backup-1", 100, map[string][]byte{"data.blob": {1, 2, 3}})
	srv.addSnapshot("team-b", "backup-2", 200, map[string][]byte{"data.blob": {4, 5, 6}})
	srv.addSnapshot("team-a/project-1", "backup-3", 300, map[string][]byte{"data.blob": {7, 8, 9}})

	namespaces, err := client.ListNamespaces(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(namespaces) != 3 {
		t.Errorf("got %d namespaces, want 3", len(namespaces))
	}
}

func TestPBSListGroupsWithNamespace(t *testing.T) {
	srv := newMockPBSServer(t)
	client := newTestPBSClient(t, srv)

	srv.addSnapshot("team-a", "backup-1", 100, map[string][]byte{"data.blob": {1, 2, 3}})
	srv.addSnapshot("team-b", "backup-2", 200, map[string][]byte{"data.blob": {4, 5, 6}})

	groups, err := client.ListGroups(context.Background(), "team-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 {
		t.Errorf("got %d groups in team-a, want 1", len(groups))
	}
	if groups[0].BackupID != "backup-1" {
		t.Errorf("group ID = %q, want backup-1", groups[0].BackupID)
	}
}

func TestPBSListSnapshotsWithNamespace(t *testing.T) {
	srv := newMockPBSServer(t)
	client := newTestPBSClient(t, srv)

	srv.addSnapshot("team-a", "my-group", 100, map[string][]byte{"data.blob": {1, 2, 3}})
	srv.addSnapshot("team-a", "my-group", 200, map[string][]byte{"data.blob": {4, 5, 6}})

	snaps, err := client.ListSnapshots(context.Background(), "team-a", "my-group")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 2 {
		t.Errorf("got %d snapshots, want 2", len(snaps))
	}
}

func TestPBSDownloadWithNamespace(t *testing.T) {
	srv := newMockPBSServer(t)
	client := newTestPBSClient(t, srv)

	data := make([]byte, 100)
	rand.Read(data)
	srv.addSnapshot("team-a", "my-group", 100, map[string][]byte{"data.blob": data})

	got, err := client.Download(context.Background(), "team-a", "my-group", 100, "data.blob")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(data) {
		t.Errorf("got %d bytes, want %d", len(got), len(data))
	}
}

func TestPBSDownloadNotFound(t *testing.T) {
	srv := newMockPBSServer(t)
	client := newTestPBSClient(t, srv)

	_, err := client.Download(context.Background(), "team-a", "nonexistent", 100, "data.blob")
	if err == nil {
		t.Error("expected error for missing object")
	}
}

func TestPBSDeleteSnapshotWithNamespace(t *testing.T) {
	srv := newMockPBSServer(t)
	client := newTestPBSClient(t, srv)

	srv.addSnapshot("team-a", "my-group", 100, map[string][]byte{"data.blob": {1, 2, 3}})

	err := client.DeleteSnapshot(context.Background(), "team-a", "my-group", 100)
	if err != nil {
		t.Fatal(err)
	}

	snaps, _ := client.ListSnapshots(context.Background(), "team-a", "my-group")
	if len(snaps) != 0 {
		t.Errorf("snapshot should be deleted, got %d", len(snaps))
	}
}

func TestPBSNamespaceIsolation(t *testing.T) {
	srv := newMockPBSServer(t)
	client := newTestPBSClient(t, srv)

	srv.addSnapshot("team-a", "shared-id", 100, map[string][]byte{"data.blob": {1, 2, 3}})
	srv.addSnapshot("team-b", "shared-id", 200, map[string][]byte{"data.blob": {4, 5, 6}})

	// team-a should see 1 group
	groups, _ := client.ListGroups(context.Background(), "team-a")
	if len(groups) != 1 {
		t.Errorf("team-a groups = %d, want 1", len(groups))
	}

	// team-b should see 1 group
	groups, _ = client.ListGroups(context.Background(), "team-b")
	if len(groups) != 1 {
		t.Errorf("team-b groups = %d, want 1", len(groups))
	}

	// Download from team-a should return its data
	data, err := client.Download(context.Background(), "team-a", "shared-id", 100, "data.blob")
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 1 {
		t.Errorf("team-a data[0] = %d, want 1", data[0])
	}
}

func TestPBSAuthFailure(t *testing.T) {
	srv := newMockPBSServer(t)
	client := NewClient(Config{
		BaseURL:   srv.URL(),
		Datastore: "teststore",
		AuthToken: "wrong-token",
	})

	_, err := client.ListGroups(context.Background(), "")
	if err == nil {
		t.Error("expected auth error")
	}
}

func writeJSON(w http.ResponseWriter, v map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
