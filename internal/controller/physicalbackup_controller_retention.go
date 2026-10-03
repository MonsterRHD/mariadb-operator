package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/hashicorp/go-multierror"
	volumesnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/azure"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/backup"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/minio"
	mdbsnapshot "github.com/mariadb-operator/mariadb-operator/v26/pkg/volumesnapshot"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	// retentionFailureRequeue is the backoff applied when old backups could not be deleted.
	retentionFailureRequeue = 30 * time.Second
	// retentionBlockedRequeue is the interval at which retention is retried while a restoration references the backups.
	retentionBlockedRequeue = 1 * time.Minute
)

//+kubebuilder:rbac:groups=k8s.mariadb.com,resources=mariadbs,verbs=get;list;watch
//+kubebuilder:rbac:groups=k8s.mariadb.com,resources=pointintimerecoveries,verbs=get;list;watch

func (r *PhysicalBackupReconciler) reconcileRetention(ctx context.Context, pb *mariadbv1alpha1.PhysicalBackup,
	snapshotList *volumesnapshotv1.VolumeSnapshotList, logger logr.Logger) (ctrl.Result, error) {
	protection, err := r.getRestoreProtection(ctx, pb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error checking restorations referencing PhysicalBackup: %v", err)
	}

	switch {
	case pb.Spec.Storage.VolumeSnapshot != nil:
		return r.reconcileSnapshotRetention(ctx, pb, snapshotList, protection, logger)
	case pb.Spec.Storage.S3 != nil || pb.Spec.Storage.AzureBlob != nil:
		return r.reconcileObjectRetention(ctx, pb, protection, logger)
	default:
		// PVC and Volume storage are only reachable from backup Jobs, which perform
		// retention cleanup after uploading. There is nothing to reconcile here.
		return ctrl.Result{}, nil
	}
}

// reconcileObjectRetention enforces the retention policy against S3/Azure Blob storage.
// Unlike the one-shot cleanup performed by backup Jobs, this runs on every reconcile,
// so deletion failures are surfaced as a condition and retried after an operator restart.
func (r *PhysicalBackupReconciler) reconcileObjectRetention(ctx context.Context, pb *mariadbv1alpha1.PhysicalBackup,
	protection *restoreProtection, logger logr.Logger) (ctrl.Result, error) {
	if protection.blocked {
		msg := fmt.Sprintf("Retention cleanup deferred: %s", protection.reason)
		logger.Info(msg)
		if err := r.reportRetentionBlocked(ctx, pb, protection.reason); err != nil {
			return ctrl.Result{}, fmt.Errorf("error patching retention condition: %v", err)
		}
		return ctrl.Result{RequeueAfter: retentionBlockedRequeue}, nil
	}

	storage, err := r.getPhysicalBackupStorage(ctx, pb)
	if err != nil {
		if reportErr := r.reportRetentionFailure(ctx, pb, fmt.Errorf("error getting backup storage: %w", err)); reportErr != nil {
			return ctrl.Result{}, reportErr
		}
		return ctrl.Result{RequeueAfter: retentionFailureRequeue}, nil
	}

	fileNames, err := storage.List(ctx)
	if err != nil {
		if reportErr := r.reportRetentionFailure(ctx, pb, fmt.Errorf("error listing backups: %w", err)); reportErr != nil {
			return ctrl.Result{}, reportErr
		}
		return ctrl.Result{RequeueAfter: retentionFailureRequeue}, nil
	}

	// A fresh processor is required here: r.BackupProcessor is configured to parse
	// Kubernetes object names (Jobs/VolumeSnapshots), not backup file names.
	processor := backup.NewPhysicalBackupProcessor()
	maxRetention := pb.Spec.MaxRetention.Duration
	oldFiles := processor.GetOldBackupFiles(fileNames, maxRetention, logger)
	if len(oldFiles) == 0 {
		return ctrl.Result{}, r.reportRetentionHealthy(ctx, pb)
	}

	var delErr *multierror.Error
	for _, fileName := range oldFiles {
		logger.V(1).Info("Deleting old backup", "backup", fileName, "physicalbackup", pb.Name)
		if err := backup.CleanupBackup(ctx, storage, fileName); err != nil {
			delErr = multierror.Append(delErr, err)
		}
	}
	if delErr.ErrorOrNil() != nil {
		if reportErr := r.reportRetentionFailure(ctx, pb, delErr); reportErr != nil {
			return ctrl.Result{}, reportErr
		}
		return ctrl.Result{RequeueAfter: retentionFailureRequeue}, nil
	}

	logger.V(1).Info("Deleted old backups", "backups", len(oldFiles), "physicalbackup", pb.Name)
	return ctrl.Result{}, r.reportRetentionHealthy(ctx, pb)
}

func (r *PhysicalBackupReconciler) reconcileSnapshotRetention(ctx context.Context, pb *mariadbv1alpha1.PhysicalBackup,
	snapshotList *volumesnapshotv1.VolumeSnapshotList, protection *restoreProtection, logger logr.Logger) (ctrl.Result, error) {
	if pb.Spec.Schedule == nil {
		return ctrl.Result{}, nil
	}

	var readySnapshotNames []string
	for _, snapshot := range snapshotList.Items {
		if mdbsnapshot.IsVolumeSnapshotReady(&snapshot) {
			readySnapshotNames = append(readySnapshotNames, snapshot.Name)
		}
	}
	maxRetention := pb.Spec.MaxRetention.Duration
	if maxRetention == 0 {
		maxRetention = mariadbv1alpha1.DefaultPhysicalBackupMaxRetention.Duration
	}

	oldSnapshotNames := r.BackupProcessor.GetOldBackupFiles(readySnapshotNames, maxRetention, logger)
	var delErr *multierror.Error
	protected := 0
	for _, snapshotName := range oldSnapshotNames {
		if _, ok := protection.snapshotNames[snapshotName]; ok {
			logger.Info("Skipping old VolumeSnapshot deletion, it is referenced by a restoration",
				"snapshot", snapshotName, "physicalbackup", pb.Name)
			protected++
			continue
		}
		key := types.NamespacedName{
			Name:      snapshotName,
			Namespace: pb.Namespace,
		}
		var snapshot volumesnapshotv1.VolumeSnapshot
		if err := r.Get(ctx, key, &snapshot); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			delErr = multierror.Append(delErr, fmt.Errorf("error getting VolumeSnapshot %q: %w", key.Name, err))
			continue
		}
		if err := r.Delete(ctx, &snapshot); err != nil && !apierrors.IsNotFound(err) {
			delErr = multierror.Append(delErr, fmt.Errorf("error deleting VolumeSnapshot %q: %w", snapshot.Name, err))
			continue
		}
		logger.V(1).Info("Deleted old VolumeSnapshot", "snapshot", key.Name, "physicalbackup", pb.Name)
	}

	if delErr.ErrorOrNil() != nil {
		if reportErr := r.reportRetentionFailure(ctx, pb, delErr); reportErr != nil {
			return ctrl.Result{}, reportErr
		}
		return ctrl.Result{RequeueAfter: retentionFailureRequeue}, nil
	}

	if protected > 0 {
		if err := r.reportRetentionBlocked(ctx, pb, protection.reason); err != nil {
			return ctrl.Result{}, fmt.Errorf("error patching retention condition: %v", err)
		}
		return ctrl.Result{RequeueAfter: retentionBlockedRequeue}, nil
	}
	return ctrl.Result{}, r.reportRetentionHealthy(ctx, pb)
}

func (r *PhysicalBackupReconciler) getPhysicalBackupStorage(ctx context.Context,
	pb *mariadbv1alpha1.PhysicalBackup) (backup.BackupStorage, error) {
	if s3Storage := pb.Spec.Storage.S3; s3Storage != nil {
		s3Client, err := minio.NewMinioClientFromS3Config(
			ctx,
			*r.RefResolver,
			*s3Storage,
			"",
			pb.Namespace,
		)
		if err != nil {
			return nil, fmt.Errorf("error creating S3 client: %w", err)
		}
		return backup.NewBlobBackupStorage(s3Client, backup.NewPhysicalBackupProcessor()), nil
	}
	if absStorage := pb.Spec.Storage.AzureBlob; absStorage != nil {
		absClient, err := azure.NewAzBlobClientFromConfig(
			ctx,
			*r.RefResolver,
			*absStorage,
			"",
			pb.Namespace,
		)
		if err != nil {
			return nil, fmt.Errorf("error creating Azure Blob client: %w", err)
		}
		return backup.NewBlobBackupStorage(absClient, backup.NewPhysicalBackupProcessor()), nil
	}
	return nil, nil
}

// restoreProtection describes the restorations currently consuming a PhysicalBackup.
type restoreProtection struct {
	// blocked indicates that backup objects must not be deleted while a restoration is in progress.
	blocked bool
	// reason explains which restoration is blocking the cleanup.
	reason string
	// snapshotNames contains the VolumeSnapshot names referenced by in-progress restorations.
	snapshotNames map[string]struct{}
}

func (r *PhysicalBackupReconciler) getRestoreProtection(ctx context.Context,
	pb *mariadbv1alpha1.PhysicalBackup) (*restoreProtection, error) {
	protection := &restoreProtection{
		snapshotNames: map[string]struct{}{},
	}

	var mariadbList mariadbv1alpha1.MariaDBList
	if err := r.List(ctx, &mariadbList, client.InNamespace(pb.Namespace)); err != nil {
		return nil, fmt.Errorf("error listing MariaDBs: %w", err)
	}
	for i := range mariadbList.Items {
		mariadb := &mariadbList.Items[i]
		if mariadb.IsInitialized() || mariadb.Spec.BootstrapFrom == nil {
			continue
		}
		bootstrap := mariadb.Spec.BootstrapFrom

		if bootstrap.VolumeSnapshotRef != nil {
			protection.snapshotNames[bootstrap.VolumeSnapshotRef.Name] = struct{}{}
		}

		reason := ""
		switch {
		case bootstrap.VolumeSnapshotRef != nil && pb.Spec.Storage.VolumeSnapshot != nil:
			reason = fmt.Sprintf("MariaDB %q is being restored from VolumeSnapshot %q",
				mariadb.Name, bootstrap.VolumeSnapshotRef.Name)
		case bootstrap.BackupRef != nil &&
			bootstrap.BackupRef.Kind == mariadbv1alpha1.PhysicalBackupKind &&
			bootstrap.BackupRef.Name == pb.Name:
			reason = fmt.Sprintf("MariaDB %q is being restored from this PhysicalBackup", mariadb.Name)
		case bootstrap.PointInTimeRecoveryRef != nil:
			pitr, err := r.RefResolver.PointInTimeRecovery(ctx, bootstrap.PointInTimeRecoveryRef, mariadb.Namespace)
			if err != nil {
				return nil, fmt.Errorf("error getting PointInTimeRecovery %q: %w", bootstrap.PointInTimeRecoveryRef.Name, err)
			}
			if pitr.Spec.PhysicalBackupRef.Name == pb.Name {
				reason = fmt.Sprintf("MariaDB %q point-in-time recovery uses this PhysicalBackup", mariadb.Name)
			}
		case pb.Spec.Storage.S3 != nil && bootstrap.S3 != nil &&
			sameS3Storage(pb.Spec.Storage.S3, bootstrap.S3):
			reason = fmt.Sprintf("MariaDB %q is being restored from the same S3 storage", mariadb.Name)
		case pb.Spec.Storage.AzureBlob != nil && bootstrap.AzureBlob != nil &&
			sameAzureBlobStorage(pb.Spec.Storage.AzureBlob, bootstrap.AzureBlob):
			reason = fmt.Sprintf("MariaDB %q is being restored from the same Azure Blob storage", mariadb.Name)
		}
		if reason != "" {
			protection.blocked = true
			if protection.reason == "" {
				protection.reason = reason
			}
		}
	}
	return protection, nil
}

func sameS3Storage(a, b *mariadbv1alpha1.S3) bool {
	return a.Bucket == b.Bucket && a.Endpoint == b.Endpoint && a.Prefix == b.Prefix
}

func sameAzureBlobStorage(a, b *mariadbv1alpha1.AzureBlob) bool {
	return a.ContainerName == b.ContainerName && a.ServiceURL == b.ServiceURL && a.Prefix == b.Prefix
}

func (r *PhysicalBackupReconciler) reportRetentionHealthy(ctx context.Context, pb *mariadbv1alpha1.PhysicalBackup) error {
	return r.patchRetentionCondition(ctx, pb, metav1.ConditionTrue,
		mariadbv1alpha1.ConditionReasonRetentionHealthy, "Old backups successfully cleaned up")
}

// reportRetentionFailure sets the Retention condition to False and emits a warning event.
// The condition makes the cleanup retryable: the requeue and the startup enqueue
// (enqueuePhysicalBackupsOnStart) both drive the reconcile again after a restart.
func (r *PhysicalBackupReconciler) reportRetentionFailure(ctx context.Context, pb *mariadbv1alpha1.PhysicalBackup, failure error) error {
	reason := mariadbv1alpha1.ConditionReasonRetentionFailed
	if !r.hasRetentionCondition(pb, metav1.ConditionFalse, reason) {
		r.Recorder.Eventf(
			pb,
			nil,
			corev1.EventTypeWarning,
			mariadbv1alpha1.ReasonRetentionDeleteFailed,
			mariadbv1alpha1.ReasonRetentionDeleteFailed,
			"Retention cleanup failed: %v",
			failure,
		)
	}
	return r.patchRetentionCondition(ctx, pb, metav1.ConditionFalse, reason, failure.Error())
}

func (r *PhysicalBackupReconciler) reportRetentionBlocked(ctx context.Context, pb *mariadbv1alpha1.PhysicalBackup,
	reason string) error {
	if !r.hasRetentionCondition(pb, metav1.ConditionFalse, mariadbv1alpha1.ConditionReasonRetentionBlocked) {
		r.Recorder.Eventf(
			pb,
			nil,
			corev1.EventTypeNormal,
			mariadbv1alpha1.ReasonRetentionBlocked,
			mariadbv1alpha1.ReasonRetentionBlocked,
			"Skipping backup retention cleanup: %s",
			reason,
		)
	}
	message := "Retention cleanup is blocked by an in-progress restoration"
	if reason != "" {
		message = fmt.Sprintf("%s: %s", message, reason)
	}
	return r.patchRetentionCondition(ctx, pb, metav1.ConditionFalse,
		mariadbv1alpha1.ConditionReasonRetentionBlocked, message)
}

func (r *PhysicalBackupReconciler) hasRetentionCondition(pb *mariadbv1alpha1.PhysicalBackup,
	status metav1.ConditionStatus, reason string) bool {
	condition := meta.FindStatusCondition(pb.Status.Conditions, mariadbv1alpha1.ConditionTypeRetention)
	return condition != nil && condition.Status == status && condition.Reason == reason
}

func (r *PhysicalBackupReconciler) patchRetentionCondition(ctx context.Context, pb *mariadbv1alpha1.PhysicalBackup,
	status metav1.ConditionStatus, reason, message string) error {
	return r.patchStatus(ctx, pb, func(pbs *mariadbv1alpha1.PhysicalBackupStatus) {
		pbs.SetCondition(metav1.Condition{
			Type:    mariadbv1alpha1.ConditionTypeRetention,
			Status:  status,
			Reason:  reason,
			Message: message,
		})
	})
}

// enqueuePhysicalBackupsOnStart re-enqueues all PhysicalBackups when the controller
// starts, so retention cleanup interrupted by an operator restart is resumed.
func (r *PhysicalBackupReconciler) enqueuePhysicalBackupsOnStart(mgr ctrl.Manager) source.Source {
	logger := mgr.GetLogger().WithName("physicalbackup").WithName("startup")
	return source.Func(func(ctx context.Context, q workqueue.TypedRateLimitingInterface[ctrl.Request]) error {
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return fmt.Errorf("timed out waiting for cache sync before enqueuing PhysicalBackups")
		}
		var list mariadbv1alpha1.PhysicalBackupList
		if err := mgr.GetClient().List(ctx, &list); err != nil {
			return fmt.Errorf("error listing PhysicalBackups on startup: %w", err)
		}
		logger.Info("Enqueuing PhysicalBackups for retention reconciliation", "physicalbackups", len(list.Items))
		for i := range list.Items {
			q.Add(ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      list.Items[i].Name,
					Namespace: list.Items[i].Namespace,
				},
			})
		}
		return nil
	})
}
