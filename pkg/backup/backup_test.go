package backup

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	ctrl "sigs.k8s.io/controller-runtime"
)

func TestGetFilePath(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		fileName     string
		wantFilePath string
	}{
		{
			name:         "empty path",
			path:         "",
			fileName:     "backup.2023-12-22T13:00:00Z.foo.sql",
			wantFilePath: "backup.2023-12-22T13:00:00Z.foo.sql",
		},
		{
			name:         "add path",
			path:         "/backup",
			fileName:     "backup.2023-12-22T13:00:00Z.foo.sql",
			wantFilePath: "/backup/backup.2023-12-22T13:00:00Z.foo.sql",
		},
		{
			name:         "already has relative path",
			path:         "/backup",
			fileName:     "mariadb/backup.2023-12-22T13:00:00Z.foo.sql",
			wantFilePath: "/backup/mariadb/backup.2023-12-22T13:00:00Z.foo.sql",
		},
		{
			name:         "already has absolute path",
			path:         "/backup",
			fileName:     "/backup/backup.2023-12-22T13:00:00Z.foo.sql",
			wantFilePath: "/backup/backup.2023-12-22T13:00:00Z.foo.sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := GetFilePath(tt.path, tt.fileName)
			if tt.wantFilePath != filePath {
				t.Fatalf("unexpected file path, expected: %v got: %v", tt.wantFilePath, filePath)
			}
		})
	}
}

func TestFileSystemCompletionAndCleanup(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	storage := NewFileSystemBackupStorage(basePath, NewPhysicalBackupProcessor(), ctrl.Log)

	backupFile := "physicalbackup-20231222130000.xb"
	uploadingFile := "physicalbackup-20231222140000.xb"
	if err := os.WriteFile(filepath.Join(basePath, backupFile), []byte("backup"), 0644); err != nil {
		t.Fatalf("error writing backup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(basePath, uploadingFile), []byte("uploading"), 0644); err != nil {
		t.Fatalf("error writing uploading backup: %v", err)
	}

	// Before the marker is written, listings contain the in-progress file but no marker.
	files, err := storage.List(ctx)
	if err != nil {
		t.Fatalf("unexpected error listing: %v", err)
	}
	if got, want := files, []string{backupFile, uploadingFile}; !reflect.DeepEqual(wantSorted(want), wantSorted(got)) {
		t.Fatalf("unexpected listing, expected: %v got: %v", want, got)
	}

	if err := storage.Complete(ctx, backupFile); err != nil {
		t.Fatalf("unexpected error writing completion marker: %v", err)
	}
	files, err = storage.List(ctx)
	if err != nil {
		t.Fatalf("unexpected error listing after completion: %v", err)
	}
	wantFilesWithMarker := []string{backupFile, uploadingFile, MarkerFileName(backupFile)}
	if got, want := files, wantSorted(wantFilesWithMarker); !reflect.DeepEqual(want, wantSorted(got)) {
		t.Fatalf("unexpected listing after completion, expected: %v got: %v", want, got)
	}

	// Only the completed backup is selectable.
	complete := CompleteBackupFiles(files)
	if got, want := complete, []string{backupFile}; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected complete files, expected: %v got: %v", want, got)
	}

	// Cleanup removes both the backup and its marker, tolerating an already missing marker.
	if err := CleanupBackup(ctx, storage, backupFile); err != nil {
		t.Fatalf("unexpected error cleaning up backup: %v", err)
	}
	if err := CleanupBackup(ctx, storage, backupFile); err != nil {
		t.Fatalf("cleanup must be idempotent: %v", err)
	}
	entries, err := os.ReadDir(basePath)
	if err != nil {
		t.Fatalf("unexpected error reading dir: %v", err)
	}
	if got, want := entryNames(entries), []string{uploadingFile}; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected remaining files, expected: %v got: %v", want, got)
	}
}

func wantSorted(values []string) []string {
	sort.Strings(values)
	return values
}

func entryNames(entries []os.DirEntry) []string {
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}
