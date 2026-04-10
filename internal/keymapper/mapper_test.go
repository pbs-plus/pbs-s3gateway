package keymapper

import (
	"testing"
)

func TestS3KeyToPBSBasic(t *testing.T) {
	km := NewKeyMapper()

	mapping := km.S3ToPBS("backups/backup.sql.gz")

	if mapping.BackupType != "host" {
		t.Errorf("BackupType = %q, want %q", mapping.BackupType, "host")
	}
	if mapping.S3Key != "backups/backup.sql.gz" {
		t.Errorf("S3Key = %q, want %q", mapping.S3Key, "backups/backup.sql.gz")
	}
	if mapping.Filename != "data.blob" {
		t.Errorf("Filename = %q, want %q", mapping.Filename, "data.blob")
	}
	if mapping.BackupID == "" {
		t.Error("BackupID should not be empty")
	}
	if mapping.BackupTime != 0 {
		t.Error("BackupTime should be 0 until set by caller")
	}
}

func TestS3KeyToPBSRoundTrip(t *testing.T) {
	km := NewKeyMapper()

	keys := []string{
		"backups/backup.sql.gz",
		"backups/backup.2024-01-15T10:30:00Z.sql.gz",
		"binlogs/server-1/mariadb-bin.000001.gz",
		"config.json",
		"data/subdir/deep/nested.file",
		"simple-file.txt",
	}

	for _, key := range keys {
		mapping := km.S3ToPBS(key)
		recovered := km.PBSToS3(mapping.BackupID)
		if recovered != key {
			t.Errorf("round-trip failed for %q: got %q (backupID=%q)", key, recovered, mapping.BackupID)
		}
	}
}

func TestS3KeyToPBSBackupIDUnique(t *testing.T) {
	km := NewKeyMapper()

	m1 := km.S3ToPBS("backups/a.sql")
	m2 := km.S3ToPBS("backups/b.sql")

	if m1.BackupID == m2.BackupID {
		t.Errorf("different S3 keys produced same backupID: %q", m1.BackupID)
	}
}

func TestS3KeyToPBSBackupIDValidChars(t *testing.T) {
	km := NewKeyMapper()

	keys := []string{
		"backups/backup.sql.gz",
		"logs/2024/01/app.log",
		"weird!@#$%^&*()/file.txt",
		"spaces in name/data",
	}

	for _, key := range keys {
		mapping := km.S3ToPBS(key)
		if !IsValidPBSID(mapping.BackupID) {
			t.Errorf("backupID %q for key %q is not a valid PBS identifier", mapping.BackupID, key)
		}
	}
}

func TestS3KeyToPBSBackupIDLength(t *testing.T) {
	km := NewKeyMapper()

	mapping := km.S3ToPBS("some/very/long/path/that/might/exceed/the/limit/if/we/keep/adding/directories/to/it/backup.sql.gz")
	if len(mapping.BackupID) > 128 {
		t.Errorf("backupID length %d exceeds PBS max of 128: %q", len(mapping.BackupID), mapping.BackupID)
	}
}

func TestS3KeyToPBSFilenameValid(t *testing.T) {
	km := NewKeyMapper()

	keys := []string{
		"backups/backup.sql.gz",
		"logs/2024/01/app.log",
		"config.json",
		"weird:file.txt",
	}

	for _, key := range keys {
		mapping := km.S3ToPBS(key)
		if !IsValidPBSFilename(mapping.Filename) {
			t.Errorf("filename %q for key %q is not a valid PBS blob name", mapping.Filename, key)
		}
	}
}

func TestS3PrefixToListGroups(t *testing.T) {
	km := NewKeyMapper()

	backupIDs := []string{}
	keys := []string{
		"backups/a.sql",
		"backups/b.sql",
		"binlogs/c.log",
		"other/d.txt",
	}
	for _, k := range keys {
		backupIDs = append(backupIDs, km.S3ToPBS(k).BackupID)
	}

	matching := km.FilterByPrefix(backupIDs, "backups/")
	if len(matching) != 2 {
		t.Errorf("prefix 'backups/' matched %d keys, want 2: %v", len(matching), matching)
	}

	matching = km.FilterByPrefix(backupIDs, "binlogs/")
	if len(matching) != 1 {
		t.Errorf("prefix 'binlogs/' matched %d keys, want 1: %v", len(matching), matching)
	}

	matching = km.FilterByPrefix(backupIDs, "")
	if len(matching) != 4 {
		t.Errorf("empty prefix matched %d keys, want 4: %v", len(matching), matching)
	}
}

func TestS3KeyDeterministic(t *testing.T) {
	km := NewKeyMapper()

	m1 := km.S3ToPBS("backups/backup.sql.gz")
	m2 := km.S3ToPBS("backups/backup.sql.gz")

	if m1.BackupID != m2.BackupID {
		t.Errorf("same key produced different backupIDs: %q vs %q", m1.BackupID, m2.BackupID)
	}
}
