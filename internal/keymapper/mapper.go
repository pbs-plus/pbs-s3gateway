package keymapper

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"path"
	"strings"
	"sync"
)

// PBSMapping represents the PBS snapshot identifiers for an S3 object.
type PBSMapping struct {
	BackupType string
	Namespace  string // PBS namespace (derived from S3 key path)
	BackupID   string // PBS backup-id (derived from S3 key filename)
	BackupTime int64
	Filename   string
	S3Key      string
}

// KeyMapper handles bidirectional mapping between S3 keys and PBS identifiers.
type KeyMapper struct {
	cache sync.Map // s3Key → *PBSMapping
}

// NewKeyMapper creates a new KeyMapper.
func NewKeyMapper() *KeyMapper {
	return &KeyMapper{}
}

// S3ToPBS maps an S3 object key to PBS snapshot identifiers.
// The S3 key path is split: directory components become the namespace,
// the filename component becomes the backup-id.
func (km *KeyMapper) S3ToPBS(s3Key string) PBSMapping {
	// Check cache first
	if v, ok := km.cache.Load(s3Key); ok {
		mapping := v.(PBSMapping)
		return mapping
	}

	// Split the S3 key into namespace (directories) and backup-id (filename)
	namespace, backupID := splitS3Key(s3Key)

	mapping := PBSMapping{
		BackupType: "host",
		Namespace:  namespace,
		BackupID:   backupID,
		Filename:   "data.blob",
		S3Key:      s3Key,
	}

	km.cache.Store(s3Key, mapping)
	return mapping
}

// splitS3Key splits an S3 key into namespace (path) and backup-id (filename).
// e.g., "test/test-db.sql" → ("test", "test-db.sql")
// e.g., "backups/2024/backup.sql" → ("backups/2024", "backup.sql")
// e.g., "simple-file.txt" → ("", "simple-file.txt")
func splitS3Key(s3Key string) (namespace, backupID string) {
	dir := path.Dir(s3Key)
	filename := path.Base(s3Key)

	// Handle special cases
	if dir == "." || dir == "/" {
		dir = ""
	}

	// Encode the filename to be a valid PBS backup-id
	encodedFilename := encodeToPBSID(filename)

	return dir, encodedFilename
}

// PBSToS3 recovers the original S3 key from a PBS backup-id and namespace.
func (km *KeyMapper) PBSToS3(namespace, backupID string) string {
	decodedFilename := decodeFromPBSID(backupID)
	if decodedFilename == "" {
		return ""
	}

	if namespace == "" {
		return decodedFilename
	}
	return namespace + "/" + decodedFilename
}

// FilterByPrefix returns backup-IDs whose decoded S3 keys start with the given prefix.
func (km *KeyMapper) FilterByPrefix(backupIDs []string, prefix string) []string {
	var result []string
	for _, id := range backupIDs {
		// We need to recover the full key, but we don't have the namespace here
		// For now, decode the backupID part only
		key := km.PBSToS3("", id)
		if strings.HasPrefix(key, prefix) {
			result = append(result, id)
		}
	}
	return result
}

// encodeToPBSID encodes a filename into a valid PBS backup-id.
// Uses human-readable encoding where possible:
// - Valid PBS chars (A-Z, a-z, 0-9, ., -) are preserved as-is
// - / is not expected here (should be handled by namespace split)
// - _ is escaped as __
// - Other chars are escaped as _XX hex
// Prefix 'r' indicates readable encoding.
func encodeToPBSID(filename string) string {
	var result strings.Builder
	result.WriteByte('r') // 'r' for readable encoding

	for i := 0; i < len(filename); i++ {
		c := filename[i]
		switch {
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '-':
			// Valid PBS chars - write as-is
			result.WriteByte(c)
		case c == '_':
			// Escape underscore with double underscore
			result.WriteString("__")
		default:
			// Escape other chars as hex
			result.WriteString(fmt.Sprintf("_%02X", c))
		}
	}

	encoded := result.String()
	if len(encoded) <= 128 && IsValidPBSID(encoded) {
		return encoded
	}

	// Fallback to hash if too long
	digest := sha256.Sum256([]byte(filename))
	return fmt.Sprintf("h%x", digest[:16])
}

// decodeFromPBSID decodes a PBS backup-id back to the original filename.
func decodeFromPBSID(backupID string) string {
	if len(backupID) == 0 {
		return ""
	}

	switch backupID[0] {
	case 'r':
		return decodeReadable(backupID[1:])
	case 'h':
		return ""
	default:
		// Try legacy base64 encoding for backwards compatibility
		if backupID[0] == 'k' {
			return decodeLegacyBase64(backupID[1:])
		}
		return ""
	}
}

// decodeReadable decodes the human-readable encoding format.
func decodeReadable(encoded string) string {
	var result strings.Builder
	for i := 0; i < len(encoded); i++ {
		c := encoded[i]
		if c == '_' && i+1 < len(encoded) {
			next := encoded[i+1]
			if next == '_' {
				// Double underscore = single underscore
				result.WriteByte('_')
				i++ // Skip the second underscore
			} else if i+2 < len(encoded) {
				// Hex escape: _XX
				var b byte
				_, err := fmt.Sscanf(encoded[i+1:i+3], "%2X", &b)
				if err == nil {
					result.WriteByte(b)
					i += 2
				} else {
					result.WriteByte(c)
				}
			} else {
				result.WriteByte(c)
			}
		} else {
			result.WriteByte(c)
		}
	}
	return result.String()
}

// decodeLegacyBase64 decodes legacy base64 encoding for backwards compatibility.
func decodeLegacyBase64(encoded string) string {
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return ""
	}
	return string(decoded)
}

// IsValidPBSID checks if a string matches PBS backup-id format:
// [A-Za-z0-9_][A-Za-z0-9._\-]*
func IsValidPBSID(s string) bool {
	if len(s) == 0 {
		return false
	}
	c := s[0]
	if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
		return false
	}
	for i := 1; i < len(s); i++ {
		c = s[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-') {
			return false
		}
	}
	return true
}

// IsValidPBSFilename checks if a string is a valid PBS blob filename.
func IsValidPBSFilename(s string) bool {
	if len(s) < 6 {
		return false
	}
	if s[len(s)-5:] != ".blob" {
		return false
	}
	return IsValidPBSID(s[:len(s)-5])
}
