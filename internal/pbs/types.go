package pbs

// Config holds PBS connection configuration.
type Config struct {
	BaseURL       string // PBS API base URL (e.g., "https://pbs:8007")
	Datastore     string // target datastore name
	AuthToken     string // PBS API token ("TOKENID:SECRET")
	SkipTLSVerify bool   // skip TLS certificate verification
}

// Snapshot represents a PBS backup snapshot.
type Snapshot struct {
	BackupType string `json:"backup-type"`
	BackupID   string `json:"backup-id"`
	BackupTime int64  `json:"backup-time"`
	Files      []struct {
		Filename string `json:"filename"`
		Size     int64  `json:"size"`
	} `json:"files"`
}

// Group represents a PBS backup group.
type Group struct {
	BackupType string `json:"backup-type"`
	BackupID   string `json:"backup-id"`
	Count      int    `json:"count"`
}

// Namespace represents a PBS backup namespace.
type Namespace struct {
	NS string `json:"ns"`
}
