package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/hashicorp/go-multierror"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/azure"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/backup"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/builder"
	mdbcompression "github.com/mariadb-operator/mariadb-operator/v26/pkg/compression"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/log"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/metadata"
	mdbminio "github.com/mariadb-operator/mariadb-operator/v26/pkg/minio"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/replication"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	scheme            = runtime.NewScheme()
	logger            = ctrl.Log
	path              string
	targetFilePath    string
	backupContentType string
	cleanupTargetFile bool

	s3           bool
	s3Bucket     string
	s3Endpoint   string
	s3Region     string
	s3TLS        bool
	s3CACertPath string
	s3Prefix     string

	abs           bool
	absContainer  string
	absServiceURL string
	absTLS        bool
	absCACertPath string
	absPrefix     string

	physicalBackupDirPath   string
	physicalBackupMeta      bool
	physicalBackupName      string
	physicalBackupNamespace string

	maxRetention time.Duration

	compression        string
	compressionThreads int
)

func init() {
	utilruntime.Must(mariadbv1alpha1.AddToScheme(scheme))

	RootCmd.PersistentFlags().StringVar(&path, "path", "/backup", "Directory path where the backup files are located."+
		"When S3 is enabled, it is used as staging area and the source of truth of backups remains in S3.")
	RootCmd.PersistentFlags().StringVar(&targetFilePath, "target-file-path", "/backup/0-backup-target.txt",
		"Path to a file that contains the name of the backup target file.")
	if err := RootCmd.MarkPersistentFlagRequired("path"); err != nil {
		fmt.Printf("error marking 'path' flag as required: %v", err)
		os.Exit(1)
	}
	if err := RootCmd.MarkPersistentFlagRequired("target-file-path"); err != nil {
		fmt.Printf("error marking 'target-file-path' flag as required: %v", err)
		os.Exit(1)
	}
	RootCmd.PersistentFlags().StringVar(&backupContentType, "backup-content-type", string(mariadbv1alpha1.BackupContentTypeLogical),
		"Backup type: Logical (mariadb-dump) or Physical (mariadb-backup).")
	RootCmd.PersistentFlags().BoolVar(&cleanupTargetFile, "cleanup-target-file", false,
		"Whether to clean up the target file after S3 backups are completed."+
			"This option should be used exclusively with external backups, such as S3.")

	RootCmd.PersistentFlags().BoolVar(&s3, "s3", false, "Enable S3 backup storage.")
	RootCmd.PersistentFlags().StringVar(&s3Bucket, "s3-bucket", "backups", "Name of the bucket to store backups.")
	RootCmd.PersistentFlags().StringVar(&s3Endpoint, "s3-endpoint", "s3.amazonaws.com", "S3 API endpoint without scheme.")
	RootCmd.PersistentFlags().StringVar(&s3Region, "s3-region", "us-east-1", "S3 region name to use.")
	RootCmd.PersistentFlags().BoolVar(&s3TLS, "s3-tls", false, "Enable S3 TLS connections.")
	RootCmd.PersistentFlags().StringVar(&s3CACertPath, "s3-ca-cert-path", "", "Path to the CA to be trusted when connecting to S3.")
	RootCmd.PersistentFlags().StringVar(&s3Prefix, "s3-prefix", "", "S3 bucket prefix name to use.")

	RootCmd.PersistentFlags().BoolVar(&abs, "abs", false, "Enable Azure Blob backup storage.")
	RootCmd.PersistentFlags().StringVar(&absContainer, "abs-container", "backups", "Name of the container to store backups.")
	RootCmd.PersistentFlags().StringVar(&absServiceURL, "abs-service-url", "", "Full abs service url to use: http(s)://<account>.blob.core.windows.net/.")
	RootCmd.PersistentFlags().BoolVar(&absTLS, "abs-tls", false, "Enable Azure Blob Storage TLS connections.")
	RootCmd.PersistentFlags().StringVar(&absCACertPath, "abs-ca-cert-path", "", "Path to the CA to be trusted when connecting to ABS.")
	RootCmd.PersistentFlags().StringVar(&absPrefix, "abs-prefix", "", "ABS container prefix to use.")

	RootCmd.PersistentFlags().StringVar(&compression, "compression", string(mariadbv1alpha1.CompressNone),
		"Compression algorithm: none, bzip2, gzip or zstd.")
	RootCmd.PersistentFlags().IntVar(&compressionThreads, "compression-threads", 0,
		"Number of CPU threads to use for zstd compression. Defaults to all available CPUs when set to 0.")

	RootCmd.PersistentFlags().StringVar(&physicalBackupDirPath, "physical-backup-dir-path", "",
		"Directory path where the physical backup is located. Only considered when backup-content-type is Physical.")
	RootCmd.Flags().BoolVar(&physicalBackupMeta, "physical-backup-meta", false,
		"Enable tracking physical backup metadata in the PhysicalBackup custom resource. Only considered when backup-content-type is Physical.")
	RootCmd.Flags().StringVar(&physicalBackupName, "physical-backup-name", "",
		"PhysicalBackup custom resource name to track physical backup metadata. Only considered when physical-backup-meta is enabled.")
	RootCmd.Flags().StringVar(&physicalBackupNamespace, "physical-backup-namespace", "",
		"PhysicalBackup custom resource namespace to track physical backup metadata. Only considered when physical-backup-meta is enabled.")

	RootCmd.Flags().DurationVar(&maxRetention, "max-retention", 30*24*time.Hour,
		"Defines the retention policy for backups. Older backups will be deleted.")

	RootCmd.AddCommand(restoreCommand)
}

var RootCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup.",
	Long:  `Manages backup files, stores them in multiple storage types and implements retention policy.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if err := log.SetupLoggerWithCommand(cmd); err != nil {
			fmt.Printf("error setting up logger: %v\n", err)
			os.Exit(1)
		}
		logger.Info("starting backup")

		ctx, cancel := newContext()
		defer cancel()

		backupProcessor, err := getBackupProcessor()
		if err != nil {
			logger.Error(err, "error getting backup processor")
			os.Exit(1)
		}
		backupStorage, err := getBackupStorage(backupProcessor)
		if err != nil {
			logger.Error(err, "error getting backup storage")
			os.Exit(1)
		}
		backupCompressor, err := getBackupCompressor(backupProcessor)
		if err != nil {
			logger.Error(err, "error getting backup compressor")
			os.Exit(1)
		}

		logger.Info("reading target file", "file", targetFilePath)
		backupTargetFile, err := readTargetFile()
		if err != nil {
			logger.Error(err, "error reading target file", "file", targetFilePath)
			os.Exit(1)
		}
		logger.Info("obtained target backup", "file", backupTargetFile)

		if err := backupCompressor.Compress(backupTargetFile); err != nil {
			logger.Error(err, "error compressing backup", "file", backupTargetFile)
			os.Exit(1)
		}

		logger.Info("pushing target backup", "file", backupTargetFile)
		if err := backupStorage.Push(ctx, backupTargetFile); err != nil {
			logger.Error(err, "error pushing target backup", "file", backupTargetFile)
			os.Exit(1)
		}

		// The marker is only written once the backup file has been fully uploaded,
		// so listings used by restores and retention never see half-uploaded backups.
		logger.Info("marking target backup as complete", "file", backupTargetFile)
		if err := backupStorage.Complete(ctx, backupTargetFile); err != nil {
			logger.Error(err, "error marking target backup as complete", "file", backupTargetFile)
			os.Exit(1)
		}

		if err := cleanupOldBackups(ctx, backupStorage, backupProcessor, logger.WithName("backup-cleanup")); err != nil {
			logger.Error(err, "error cleaning up old backups")
			os.Exit(1)
		}

		if err := cleanupFile(backupTargetFile, logger.WithName("cleanup")); err != nil && os.IsNotExist(err) {
			logger.Error(err, "error cleaning up target file", "file", backupTargetFile)
			os.Exit(1)
		}

		if err := handleBackupMeta(ctx, logger.WithName("backup-meta")); err != nil {
			logger.Error(err, "error handling backup meta")
			os.Exit(1)
		}
	},
}

// cleanupOldBackups deletes backups older than the retention policy.
// Physical backups are protected: deletions are skipped while a restoration references the
// PhysicalBackup, and the outcome is tracked on the PhysicalBackup status so the operator
// can retry failed cleanups.
func cleanupOldBackups(ctx context.Context, backupStorage backup.BackupStorage, backupProcessor backup.BackupProcessor,
	logger logr.Logger) error {
	logger.Info("cleaning up old backups")
	backupNames, err := backupStorage.List(ctx)
	if err != nil {
		return fmt.Errorf("error listing backup files: %w", err)
	}
	oldBackups := backupProcessor.GetOldBackupFiles(backupNames, maxRetention, logger)
	logger.Info("old backups to delete", "backups", len(oldBackups))
	if len(oldBackups) == 0 {
		return nil
	}

	if backupContentType != string(mariadbv1alpha1.BackupContentTypePhysical) {
		// Logical backups: keep the historical behavior, log deletion errors without failing.
		for _, oldBackup := range oldBackups {
			logger.V(1).Info("deleting old backup", "backup", oldBackup)
			if err := backup.CleanupBackup(ctx, backupStorage, oldBackup); err != nil {
				logger.Error(err, "error removing old backup", "backup", oldBackup)
			}
		}
		return nil
	}

	k8sClient, pb, err := getPhysicalBackupForRetention()
	if err != nil {
		// Without Kubernetes access we cannot tell whether a restoration is in progress,
		// so deletion is deferred to the next run / operator reconciliation.
		logger.Error(err, "error getting PhysicalBackup, deferring retention cleanup")
		return nil
	}

	blocked, reason, err := physicalBackupRetentionBlocked(ctx, k8sClient, pb)
	if err != nil {
		logger.Error(err, "error checking restorations referencing the PhysicalBackup, deferring retention cleanup")
		_ = patchPhysicalBackupRetentionCondition(ctx, k8sClient, pb,
			metav1.ConditionFalse, mariadbv1alpha1.ConditionReasonRetentionFailed, err.Error())
		return nil
	}
	if blocked {
		logger.Info("skipping retention cleanup: restoration in progress", "reason", reason)
		_ = patchPhysicalBackupRetentionCondition(ctx, k8sClient, pb,
			metav1.ConditionFalse, mariadbv1alpha1.ConditionReasonRetentionBlocked,
			fmt.Sprintf("Retention cleanup is blocked by an in-progress restoration: %s", reason))
		return nil
	}

	var delErr *multierror.Error
	for _, oldBackup := range oldBackups {
		logger.V(1).Info("deleting old backup", "backup", oldBackup)
		if err := backup.CleanupBackup(ctx, backupStorage, oldBackup); err != nil {
			logger.Error(err, "error removing old backup", "backup", oldBackup)
			delErr = multierror.Append(delErr, err)
		}
	}
	if delErr.ErrorOrNil() != nil {
		_ = patchPhysicalBackupRetentionCondition(ctx, k8sClient, pb,
			metav1.ConditionFalse, mariadbv1alpha1.ConditionReasonRetentionFailed, delErr.Error())
		// The backup itself succeeded; do not fail the Job. The operator retries the cleanup.
		return nil
	}
	_ = patchPhysicalBackupRetentionCondition(ctx, k8sClient, pb,
		metav1.ConditionTrue, mariadbv1alpha1.ConditionReasonRetentionHealthy, "Old backups successfully cleaned up")
	return nil
}

// getPhysicalBackupForRetention returns the Kubernetes client and the PhysicalBackup tracked
// by this backup Job. It is only available for physical backups, where the operator always
// passes the PhysicalBackup name and namespace.
func getPhysicalBackupForRetention() (client.Client, *mariadbv1alpha1.PhysicalBackup, error) {
	if physicalBackupName == "" || physicalBackupNamespace == "" {
		return nil, nil, errors.New("physical backup name or namespace is not set")
	}
	k8sClient, err := getK8sClient()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting Kubernetes client: %w", err)
	}
	key := types.NamespacedName{
		Name:      physicalBackupName,
		Namespace: physicalBackupNamespace,
	}
	var pb mariadbv1alpha1.PhysicalBackup
	if err := k8sClient.Get(context.Background(), key, &pb); err != nil {
		return nil, nil, fmt.Errorf("error getting PhysicalBackup: %w", err)
	}
	return k8sClient, &pb, nil
}

// physicalBackupRetentionBlocked reports whether an in-progress MariaDB restoration consumes
// backups produced by the PhysicalBackup (either directly, via point-in-time recovery, or from
// the same remote storage location).
func physicalBackupRetentionBlocked(ctx context.Context, c client.Client,
	pb *mariadbv1alpha1.PhysicalBackup) (bool, string, error) {
	var mariadbList mariadbv1alpha1.MariaDBList
	if err := c.List(ctx, &mariadbList, client.InNamespace(pb.Namespace)); err != nil {
		return false, "", fmt.Errorf("error listing MariaDBs: %w", err)
	}
	for i := range mariadbList.Items {
		mariadb := &mariadbList.Items[i]
		if mariadb.IsInitialized() || mariadb.Spec.BootstrapFrom == nil {
			continue
		}
		bootstrap := mariadb.Spec.BootstrapFrom

		if bootstrap.BackupRef != nil &&
			bootstrap.BackupRef.Kind == mariadbv1alpha1.PhysicalBackupKind &&
			bootstrap.BackupRef.Name == pb.Name {
			return true, fmt.Sprintf("MariaDB %q is being restored from this PhysicalBackup", mariadb.Name), nil
		}
		if bootstrap.PointInTimeRecoveryRef != nil {
			var pitr mariadbv1alpha1.PointInTimeRecovery
			key := types.NamespacedName{
				Name:      bootstrap.PointInTimeRecoveryRef.Name,
				Namespace: mariadb.Namespace,
			}
			if err := c.Get(ctx, key, &pitr); err != nil {
				return false, "", fmt.Errorf("error getting PointInTimeRecovery %q: %w", key.Name, err)
			}
			if pitr.Spec.PhysicalBackupRef.Name == pb.Name {
				return true, fmt.Sprintf("MariaDB %q point-in-time recovery uses this PhysicalBackup", mariadb.Name), nil
			}
		}
		if pb.Spec.Storage.S3 != nil && bootstrap.S3 != nil &&
			pb.Spec.Storage.S3.Bucket == bootstrap.S3.Bucket &&
			pb.Spec.Storage.S3.Endpoint == bootstrap.S3.Endpoint &&
			pb.Spec.Storage.S3.Prefix == bootstrap.S3.Prefix {
			return true, fmt.Sprintf("MariaDB %q is being restored from the same S3 storage", mariadb.Name), nil
		}
		if pb.Spec.Storage.AzureBlob != nil && bootstrap.AzureBlob != nil &&
			pb.Spec.Storage.AzureBlob.ContainerName == bootstrap.AzureBlob.ContainerName &&
			pb.Spec.Storage.AzureBlob.ServiceURL == bootstrap.AzureBlob.ServiceURL &&
			pb.Spec.Storage.AzureBlob.Prefix == bootstrap.AzureBlob.Prefix {
			return true, fmt.Sprintf("MariaDB %q is being restored from the same Azure Blob storage", mariadb.Name), nil
		}
	}
	return false, "", nil
}

func patchPhysicalBackupRetentionCondition(ctx context.Context, c client.Client, pb *mariadbv1alpha1.PhysicalBackup,
	status metav1.ConditionStatus, reason, message string) error {
	key := types.NamespacedName{
		Name:      pb.Name,
		Namespace: pb.Namespace,
	}
	var current mariadbv1alpha1.PhysicalBackup
	if err := c.Get(ctx, key, &current); err != nil {
		return fmt.Errorf("error getting PhysicalBackup: %w", err)
	}
	patch := client.MergeFrom(current.DeepCopy())
	current.Status.SetCondition(metav1.Condition{
		Type:    mariadbv1alpha1.ConditionTypeRetention,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
	if err := c.Status().Patch(ctx, &current, patch); err != nil {
		return fmt.Errorf("error patching PhysicalBackup retention condition: %w", err)
	}
	return nil
}

func newContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), []os.Signal{
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGKILL,
		syscall.SIGHUP,
		syscall.SIGQUIT}...,
	)
}

func getBackupProcessor() (backup.BackupProcessor, error) {
	backType := mariadbv1alpha1.BackupContentType(backupContentType)
	if err := backType.Validate(); err != nil {
		return nil, fmt.Errorf("backup type not supported: %v", err)
	}
	switch backType {
	case mariadbv1alpha1.BackupContentTypeLogical:
		logger.Info("configuring logical backup processor")
		return backup.NewLogicalBackupProcessor(), nil
	case mariadbv1alpha1.BackupContentTypePhysical:
		logger.Info("configuring physical backup processor")
		return backup.NewPhysicalBackupProcessor(), nil
	default:
		return nil, fmt.Errorf("unsupported backup type: %v", backType)
	}
}

func getBackupStorage(processor backup.BackupProcessor) (backup.BackupStorage, error) {
	if s3 {
		logger.Info("configuring S3 backup storage")
		opts := []mdbminio.MinioOpt{
			mdbminio.WithTLS(s3TLS),
			mdbminio.WithCACertPath(s3CACertPath),
			mdbminio.WithRegion(s3Region),
			mdbminio.WithPrefix(s3Prefix),
		}
		if ssecKey := os.Getenv(builder.S3SSECCustomerKey); ssecKey != "" {
			logger.Info("configuring S3 SSE-C encryption")
			opts = append(opts, mdbminio.WithSSECCustomerKey(ssecKey))
		}
		return backup.NewBlobBackupStorageWithS3(
			path,
			s3Bucket,
			s3Endpoint,
			processor,
			opts...,
		)
	}
	if abs {
		logger.Info("Configuring ABS backup storage")
		opts := []azure.AzBlobOpt{
			azure.WithTLSEnabled(absTLS),
			azure.WithTLSCACertPath(absCACertPath),
			azure.WithPrefix(absPrefix),
		}
		if accountKey := os.Getenv(builder.ABSStorageAccountKey); accountKey != "" {
			opts = append(opts, azure.WithAccountKey(accountKey))
		}

		if accountName := os.Getenv(builder.ABSStorageAccountName); accountName != "" {
			opts = append(opts, azure.WithAccountName(accountName))
		}
		return backup.NewBlobBackupStorageWithABS(
			path,
			absContainer,
			absServiceURL,
			processor,
			opts...,
		)
	}
	logger.Info("configuring filesystem backup storage")
	return backup.NewFileSystemBackupStorage(path, processor, logger.WithName("file-system-storage")), nil
}

func getBackupCompressor(processor backup.BackupProcessor) (mdbcompression.BackupCompressor, error) {
	calg := mariadbv1alpha1.CompressAlgorithm(compression)
	if err := calg.Validate(); err != nil {
		return nil, fmt.Errorf("compression algorithm not supported: %v", err)
	}
	return mdbcompression.NewBackupCompressor(calg, compressionThreads, path, processor.GetUncompressedBackupFile, logger)
}

func readTargetFile() (string, error) {
	bytes, err := os.ReadFile(targetFilePath)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func cleanupFile(fileName string, logger logr.Logger) error {
	if (!s3 && !abs) || !cleanupTargetFile {
		return nil
	}
	filePath := backup.GetFilePath(path, fileName)
	logger.Info("cleaning up target file", "file", filePath)

	return os.Remove(filePath)
}

func handleBackupMeta(ctx context.Context, backupLogger logr.Logger) error {
	if backupContentType != string(mariadbv1alpha1.BackupContentTypePhysical) {
		return nil
	}
	if !physicalBackupMeta || physicalBackupName == "" || physicalBackupNamespace == "" {
		return nil
	}
	key := types.NamespacedName{
		Name:      physicalBackupName,
		Namespace: physicalBackupNamespace,
	}
	logger := backupLogger.WithValues("physicalbackup", key.Name)
	logger.Info("handling physical backup meta")

	k8sClient, err := getK8sClient()
	if err != nil {
		return fmt.Errorf("error getting Kubernetes client: %v", err)
	}
	var physicalBackup mariadbv1alpha1.PhysicalBackup
	if err := k8sClient.Get(ctx, key, &physicalBackup); err != nil {
		return fmt.Errorf("error getting PhysicalBackup: %v", err)
	}

	rawGTID, err := getBackupGTID()
	if err != nil {
		return fmt.Errorf("error getting backup GTID: %v", err)
	}
	// TODO: support multiple GTID domain IDs
	gtid, err := replication.ParseGtid(rawGTID)
	if err != nil {
		return fmt.Errorf("error parsing GTID %s: %v", rawGTID, err)
	}

	patch := client.MergeFrom(physicalBackup.DeepCopy())
	if physicalBackup.Annotations == nil {
		physicalBackup.Annotations = make(map[string]string)
	}
	physicalBackup.Annotations[metadata.LastGtidAnnotation] = gtid.String()
	if err := k8sClient.Patch(ctx, &physicalBackup, patch); err != nil {
		return fmt.Errorf("error patching PhysicalBackup: %v", err)
	}

	logger.Info("patched PhysicalBackup with GTID", "gtid", gtid.String())
	return nil
}

func getK8sClient() (client.Client, error) {
	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("error getting REST config: %v", err)
	}
	k8sClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("error creating Kubernetes client: %v", err)
	}
	return k8sClient, nil
}

func getBackupGTID() (string, error) {
	metaFilePath := filepath.Join(physicalBackupDirPath, replication.MariaDBOperatorFileName)
	bytes, err := os.ReadFile(metaFilePath)
	if err != nil {
		return "", fmt.Errorf("error reading backup meta file %s: %v", metaFilePath, err)
	}
	rawGtid, err := replication.ParseRawGtidInMetaFile(bytes)
	if err != nil {
		return "", fmt.Errorf("error parsing GTID in meta file %s: %v", metaFilePath, err)
	}
	return rawGtid, nil
}
