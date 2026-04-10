package keymapper

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
)

// PBSMapping represents the PBS snapshot identifiers for an S3 object.
type PBSMapping struct {
	BackupType string
	BackupID   string
	BackupTime int64
	Filename   string
	S3Key      string
}

// KeyMapper handles bidirectional mapping between S3 keys and PBS identifiers.
type KeyMapper struct {
	cache sync.Map // s3Key → backupID
}

// NewKeyMapper creates a new KeyMapper.
func NewKeyMapper() *KeyMapper {
	return &KeyMapper{}
}

// S3ToPBS maps an S3 object key to PBS snapshot identifiers.
func (km *KeyMapper) S3ToPBS(s3Key string) PBSMapping {
	// Check cache first
	if v, ok := km.cache.Load(s3Key); ok {
		return PBSMapping{
			BackupType: "host",
			BackupID:   v.(string),
			Filename:   "data.blob",
			S3Key:      s3Key,
		}
	}

	backupID := encodeToPBSID(s3Key)
	km.cache.Store(s3Key, backupID)

	return PBSMapping{
		BackupType: "host",
		BackupID:   backupID,
		Filename:   "data.blob",
		S3Key:      s3Key,
	}
}

// PBSToS3 recovers the original S3 key from a PBS backup-id.
func (km *KeyMapper) PBSToS3(backupID string) string {
	return decodeFromPBSID(backupID)
}

// FilterByPrefix returns backup-IDs whose decoded S3 keys start with the given prefix.
func (km *KeyMapper) FilterByPrefix(backupIDs []string, prefix string) []string {
	var result []string
	for _, id := range backupIDs {
		key := km.PBSToS3(id)
		if strings.HasPrefix(key, prefix) {
			result = append(result, id)
		}
	}
	return result
}

// encodeToPBSID encodes an S3 key into a valid PBS backup-id.
func encodeToPBSID(s3Key string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(s3Key))
	id := "k" + encoded

	if len(id) <= 128 && IsValidPBSID(id) {
		return id
	}

	digest := sha256.Sum256([]byte(s3Key))
	return fmt.Sprintf("h%x", digest[:16])
}

// decodeFromPBSID decodes a PBS backup-id back to the original S3 key.
func decodeFromPBSID(backupID string) string {
	if len(backupID) == 0 {
		return ""
	}

	switch backupID[0] {
	case 'k':
		decoded, err := base64.RawURLEncoding.DecodeString(backupID[1:])
		if err != nil {
			return ""
		}
		return string(decoded)
	case 'h':
		return ""
	default:
		return ""
	}
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
