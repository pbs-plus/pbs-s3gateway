package auth

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func testStore() *Store {
	return &Store{
		creds: map[string]string{
			"root@pam!s3gateway": "root@pam!s3gateway:secret123",
			"backup@pbs!token":   "backup@pbs!token:other-secret",
		},
	}
}

func TestSigV4(t *testing.T) {
	s := testStore()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=root@pam!s3gateway/20240115/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123")

	token, ok := s.TokenFromRequest(r)
	if !ok {
		t.Fatal("expected to find token")
	}
	if token != "root@pam!s3gateway:secret123" {
		t.Errorf("token = %q, want %q", token, "root@pam!s3gateway:secret123")
	}
}

func TestSigV2(t *testing.T) {
	s := testStore()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	r.Header.Set("Authorization", "AWS backup@pbs!token:sig123")

	token, ok := s.TokenFromRequest(r)
	if !ok {
		t.Fatal("expected to find token")
	}
	if token != "backup@pbs!token:other-secret" {
		t.Errorf("token = %q, want %q", token, "backup@pbs!token:other-secret")
	}
}

func TestBasicAuth(t *testing.T) {
	s := testStore()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("root@pam!s3gateway:secret123")))

	token, ok := s.TokenFromRequest(r)
	if !ok {
		t.Fatal("expected to find token")
	}
	if token != "root@pam!s3gateway:secret123" {
		t.Errorf("token = %q, want %q", token, "root@pam!s3gateway:secret123")
	}
}

func TestBasicAuthWrongSecret(t *testing.T) {
	s := testStore()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("root@pam!s3gateway:wrongsecret")))

	_, ok := s.TokenFromRequest(r)
	if ok {
		t.Error("should reject wrong secret")
	}
}

func TestUnknownAccessKey(t *testing.T) {
	s := testStore()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=unknown/20240115/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc")

	_, ok := s.TokenFromRequest(r)
	if ok {
		t.Error("should reject unknown access key")
	}
}

func TestNoAuth(t *testing.T) {
	s := testStore()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)

	_, ok := s.TokenFromRequest(r)
	if ok {
		t.Error("should return false with no auth header")
	}
}

func TestNilStore(t *testing.T) {
	var s *Store
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	r.Header.Set("Authorization", "AWS key:sig")

	_, ok := s.TokenFromRequest(r)
	if ok {
		t.Error("nil store should return false")
	}
}

func TestQueryStringAuth(t *testing.T) {
	s := testStore()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key?AWSAccessKeyId=root@pam!s3gateway", nil)

	token, ok := s.TokenFromRequest(r)
	if !ok {
		t.Fatal("expected to find token")
	}
	if token != "root@pam!s3gateway:secret123" {
		t.Errorf("token = %q, want %q", token, "root@pam!s3gateway:secret123")
	}
}

func TestLoadStore(t *testing.T) {
	creds := map[string]string{
		"key1": "secret1",
		"key2": "secret2",
	}
	data, _ := json.Marshal(creds)

	f, err := os.CreateTemp("", "creds-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.Write(data)
	f.Close()

	s, err := LoadStore(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	token, ok := s.creds["key1"]
	if !ok {
		t.Fatal("expected key1")
	}
	if token != "key1:secret1" {
		t.Errorf("token = %q, want %q", token, "key1:secret1")
	}
}

func TestLoadStoreInvalidJSON(t *testing.T) {
	f, err := os.CreateTemp("", "creds-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString("not json")
	f.Close()

	_, err = LoadStore(f.Name())
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestStoreReload(t *testing.T) {
	// Create initial credentials file
	creds := map[string]string{"key1": "secret1"}
	data, _ := json.Marshal(creds)

	f, err := os.CreateTemp("", "creds-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.Write(data)
	f.Close()

	// Load store
	s, err := LoadStore(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Verify initial credentials
	token, ok := s.lookup("key1")
	if !ok || token != "key1:secret1" {
		t.Fatalf("initial creds wrong: got %q", token)
	}

	// Update file with new credentials
	newCreds := map[string]string{"key2": "secret2"}
	newData, _ := json.Marshal(newCreds)
	if err := os.WriteFile(f.Name(), newData, 0644); err != nil {
		t.Fatal(err)
	}

	// Force reload by calling load directly
	if err := s.load(); err != nil {
		t.Fatal(err)
	}

	// Verify new credentials loaded
	token, ok = s.lookup("key2")
	if !ok || token != "key2:secret2" {
		t.Errorf("reloaded creds wrong: got %q, want %q", token, "key2:secret2")
	}

	// Verify old credentials gone
	_, ok = s.lookup("key1")
	if ok {
		t.Error("old key1 should be gone after reload")
	}
}
