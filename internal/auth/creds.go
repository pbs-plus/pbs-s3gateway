package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Store maps S3 access key IDs to PBS tokens.
// If no store is configured, the gateway falls back to a static PBS token.
type Store struct {
	creds map[string]string // accessKeyId → PBSAPIToken value (e.g., "root@pam!token:secret")
}

// LoadStore reads a credentials file. Format: JSON object mapping
// accessKeyId → secretAccessKey. The PBS token is formed as
// "accessKeyId:secretAccessKey".
//
// Example file:
//
//	{
//	  "root@pam!s3gateway": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
//	}
func LoadStore(path string) (*Store, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read credentials: %w", err)
	}

	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse credentials: %w", err)
	}

	creds := make(map[string]string, len(raw))
	for accessKey, secret := range raw {
		creds[accessKey] = accessKey + ":" + secret
	}

	return &Store{creds: creds}, nil
}

// TokenFromRequest extracts an S3 auth token from the HTTP request
// and returns the PBS token to use.
func (s *Store) TokenFromRequest(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if header == "" {
		// Try query string auth
		if ak := r.URL.Query().Get("AWSAccessKeyId"); ak != "" {
			return s.lookup(ak)
		}
		return "", false
	}

	if strings.HasPrefix(header, "AWS4-HMAC-SHA256") {
		return s.extractSigV4(header)
	}
	if strings.HasPrefix(header, "AWS ") {
		return s.extractSigV2(header)
	}
	if strings.HasPrefix(header, "Basic ") {
		return s.extractBasic(header)
	}

	return "", false
}

func (s *Store) extractSigV4(header string) (string, bool) {
	if s == nil {
		return "", false
	}
	// AWS4-HMAC-SHA256 Credential=ACCESSKEYID/20240115/us-east-1/s3/aws4_request, ...
	for part := range strings.SplitSeq(header, " ") {
		if after, ok := strings.CutPrefix(part, "Credential="); ok {
			cred := after
			cred = strings.TrimSuffix(cred, ",")
			// Format: ACCESSKEYID/date/region/s3/aws4_request
			if before, _, ok := strings.Cut(cred, "/"); ok {
				return s.lookup(before)
			}
		}
	}
	return "", false
}

func (s *Store) extractSigV2(header string) (string, bool) {
	if s == nil {
		return "", false
	}
	// AWS ACCESSKEYID:signature
	parts := strings.SplitN(strings.TrimPrefix(header, "AWS "), ":", 2)
	if len(parts) >= 1 {
		return s.lookup(parts[0])
	}
	return "", false
}

func (s *Store) extractBasic(header string) (string, bool) {
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(header, "Basic "))
	if err != nil {
		return "", false
	}
	pair := strings.SplitN(string(decoded), ":", 2)
	if len(pair) != 2 {
		return "", false
	}

	// If a store exists, we MUST verify the secret against it.
	// This prevents unauthorized access when some keys are restricted.
	if s != nil {
		token, ok := s.lookup(pair[0])
		if ok {
			// Verify the secret matches what we have in the store
			expected := pair[0] + ":" + pair[1]
			if token == expected {
				return token, true
			}
			return "", false
		}
	}

	// If no store is configured, or key is not in store, we allow "blind" passthrough
	// for Basic Auth because it carries the secret in the clear.
	return pair[0] + ":" + pair[1], true
}

func (s *Store) lookup(accessKey string) (string, bool) {
	if s == nil {
		return "", false
	}
	token, ok := s.creds[accessKey]
	return token, ok
}
