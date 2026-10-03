package backup

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"github.com/hashicorp/go-multierror"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/azure"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/interfaces"
	mariadbminio "github.com/mariadb-operator/mariadb-operator/v26/pkg/minio"
)

type BackupStorage interface {
	List(ctx context.Context) ([]string, error)
	Push(ctx context.Context, fileName string) error
	// Complete marks a backup file as fully written/uploaded by creating its completion marker.
	Complete(ctx context.Context, fileName string) error
	Pull(ctx context.Context, fileName string) error
	Delete(ctx context.Context, fileName string) error
	IsNotFound(err error) bool
	shouldProcessBackupFile(fileName string, logger logr.Logger) bool
}

type FileSystemBackupStorage struct {
	basePath  string
	processor BackupProcessor
	logger    logr.Logger
}

func NewFileSystemBackupStorage(basePath string, processor BackupProcessor, logger logr.Logger) BackupStorage {
	return &FileSystemBackupStorage{
		basePath:  basePath,
		processor: processor,
		logger:    logger,
	}
}

func (f *FileSystemBackupStorage) List(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(f.basePath)
	if err != nil {
		return nil, err
	}
	var fileNames []string
	for _, e := range entries {
		fileName := e.Name()
		if f.shouldProcessBackupFile(fileName, f.logger) {
			fileNames = append(fileNames, fileName)
		}
	}
	return fileNames, nil
}

func (f *FileSystemBackupStorage) Push(ctx context.Context, fileName string) error {
	return nil // noop
}

func (f *FileSystemBackupStorage) Complete(ctx context.Context, fileName string) error {
	markerPath := GetFilePath(f.basePath, MarkerFileName(fileName))
	return os.WriteFile(markerPath, nil, 0644)
}

func (f *FileSystemBackupStorage) Pull(ctx context.Context, fileName string) error {
	return nil // noop
}

func (f *FileSystemBackupStorage) Delete(ctx context.Context, fileName string) error {
	return os.Remove(GetFilePath(f.basePath, fileName))
}

func (f *FileSystemBackupStorage) IsNotFound(err error) bool {
	return os.IsNotExist(err)
}

func (f *FileSystemBackupStorage) shouldProcessBackupFile(fileName string, logger logr.Logger) bool {
	logger.V(1).Info("processing backup file", "file", fileName)
	if f.processor.IsValidBackupFile(fileName) {
		return true
	}
	// Completion markers of valid backups are listed too, so they can be used to
	// determine whether a backup upload has finished.
	if IsBackupMarker(fileName) && f.processor.IsValidBackupFile(strings.TrimSuffix(fileName, BackupCompleteSuffix)) {
		return true
	}
	logger.V(1).Info("ignoring file", "file", fileName)
	return false
}

type BlobBackupStorage struct {
	processor BackupProcessor
	client    interfaces.BlobStorage
}

// NewBlobBackupStorage wraps an already configured blob storage client (S3 or Azure Blob).
func NewBlobBackupStorage(client interfaces.BlobStorage, processor BackupProcessor) BackupStorage {
	return &BlobBackupStorage{
		client:    client,
		processor: processor,
	}
}

func NewBlobBackupStorageWithS3(basePath, bucket, endpoint string, processor BackupProcessor,
	mOpts ...mariadbminio.MinioOpt) (BackupStorage, error) {
	client, err := mariadbminio.NewMinioClient(basePath, bucket, endpoint, mOpts...)
	if err != nil {
		return nil, fmt.Errorf("error creating S3 client: %v", err)
	}

	return &BlobBackupStorage{
		client:    client,
		processor: processor,
	}, nil
}

func NewBlobBackupStorageWithABS(basePath, containerName, serviceURL string, processor BackupProcessor,
	absOpts ...azure.AzBlobOpt) (BackupStorage, error) {
	client, err := azure.NewAzBlobClient(basePath, containerName, serviceURL, absOpts...)
	if err != nil {
		return nil, fmt.Errorf("error creating Azure Blob Client: %v", err)
	}

	return &BlobBackupStorage{
		client:    client,
		processor: processor,
	}, nil
}

func (s *BlobBackupStorage) Delete(ctx context.Context, fileName string) error {
	return s.client.RemoveWithOptions(ctx, fileName)
}

func (s *BlobBackupStorage) IsNotFound(err error) bool {
	return s.client.IsNotFound(err)
}

func (s *BlobBackupStorage) List(ctx context.Context) ([]string, error) {
	return s.client.ListObjectsWithOptions(ctx)
}

func (s *BlobBackupStorage) Push(ctx context.Context, fileName string) error {
	return s.client.FPutObjectWithOptions(ctx, fileName)
}

func (s *BlobBackupStorage) Complete(ctx context.Context, fileName string) error {
	return s.client.PutObjectWithOptions(ctx, MarkerFileName(fileName), strings.NewReader(""), 0)
}

func (s *BlobBackupStorage) Pull(ctx context.Context, fileName string) error {
	return s.client.FGetObjectWithOptions(ctx, fileName)
}

func (s *BlobBackupStorage) shouldProcessBackupFile(fileName string, logger logr.Logger) bool {
	logger.V(1).Info("processing backup file", "file", fileName)
	if s.processor.IsValidBackupFile(s.client.UnprefixedFilename(fileName)) {
		return true
	}
	logger.V(1).Info("ignoring file", "file", fileName)
	return false
}

// CleanupBackup deletes a backup and its completion marker. Missing objects are
// not considered errors, making cleanup idempotent and safe to retry.
func CleanupBackup(ctx context.Context, storage BackupStorage, fileName string) error {
	var errBundle *multierror.Error
	if err := storage.Delete(ctx, fileName); err != nil && !storage.IsNotFound(err) {
		errBundle = multierror.Append(errBundle, fmt.Errorf("error deleting backup %q: %w", fileName, err))
	}
	marker := MarkerFileName(fileName)
	if err := storage.Delete(ctx, marker); err != nil && !storage.IsNotFound(err) {
		errBundle = multierror.Append(errBundle, fmt.Errorf("error deleting backup marker %q: %w", marker, err))
	}
	return errBundle.ErrorOrNil()
}
