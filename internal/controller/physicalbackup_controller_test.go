package controller

import (
	"context"
	"errors"

	"github.com/go-logr/logr"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/refresolver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("PhysicalBackup", Label("basic"), func() {
	BeforeEach(func() {
		By("Waiting for MariaDB to be ready")
		expectMariadbReady(testCtx, k8sClient, testMdbkey)
	})

	DescribeTable("Creating a PhysicalBackup",
		func(
			resourceName string,
			builderFn physicalBackupBuilder,
			testFn func(*mariadbv1alpha1.PhysicalBackup),
		) {
			key := types.NamespacedName{
				Name:      resourceName,
				Namespace: testNamespace,
			}
			backup := builderFn(key)
			testFn(backup)
		},
		Entry(
			"should reconcile a Job with PVC storage",
			"physicalbackup-job-pvc-test",
			buildPhysicalBackupWithPVCStorage(testMdbkey),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with Volume storage",
			"physicalbackup-job-volume-test",
			buildPhysicalBackupWithVolumeStorage(testMdbkey),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with ABS storage",
			"physicalbackup-job-abs-test",
			buildPhysicalBackupWithABSStorage(testMdbkey, "test-physicalbackup", ""),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with S3 storage",
			"physicalbackup-job-s3-test",
			buildPhysicalBackupWithS3Storage(testMdbkey, "test-physicalbackup", ""),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with ABS storage with prefix",
			"physicalbackup-job-abs-prefix-test",
			buildPhysicalBackupWithABSStorage(testMdbkey, "test-physicalbackup", "mariadb"),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with S3 storage with prefix",
			"physicalbackup-job-s3-prefix-test",
			buildPhysicalBackupWithS3Storage(testMdbkey, "test-physicalbackup", "mariadb"),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with ABS storage and bzip2 compression",
			"physicalbackup-job-abs-bzip2-test",
			applyDecoratorChain(
				buildPhysicalBackupWithABSStorage(testMdbkey, "test-physicalbackup", ""),
				decoratePhysicalBackupWithBzip2Compression,
			),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with S3 storage and bzip2 compression",
			"physicalbackup-job-s3-bzip2-test",
			applyDecoratorChain(
				buildPhysicalBackupWithS3Storage(testMdbkey, "test-physicalbackup", ""),
				decoratePhysicalBackupWithBzip2Compression,
			),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with ABS storage and gzip compression",
			"physicalbackup-job-abs-gzip-test",
			applyDecoratorChain(
				buildPhysicalBackupWithABSStorage(testMdbkey, "test-physicalbackup", ""),
				decoratePhysicalBackupWithGzipCompression,
			),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with S3 storage and gzip compression",
			"physicalbackup-job-s3-gzip-test",
			applyDecoratorChain(
				buildPhysicalBackupWithS3Storage(testMdbkey, "test-physicalbackup", ""),
				decoratePhysicalBackupWithGzipCompression,
			),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with S3 storage and SSEC",
			"physicalbackup-job-s3-ssec-test",
			applyDecoratorChain(
				buildPhysicalBackupWithS3Storage(testMdbkey, "test-physicalbackup", ""),
				decoratePhysicalBackupWithSSEC,
			),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with ABS storage and staging storage",
			"physicalbackup-job-abs-staging-test",
			applyDecoratorChain(
				buildPhysicalBackupWithABSStorage(testMdbkey, "test-physicalbackup", ""),
				decoratePhysicalBackupWithStagingStorage,
			),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a Job with S3 storage and staging storage",
			"physicalbackup-job-s3-staging-test",
			applyDecoratorChain(
				buildPhysicalBackupWithS3Storage(testMdbkey, "test-physicalbackup", ""),
				decoratePhysicalBackupWithStagingStorage,
			),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a scheduled Job with ABS storage",
			"physicalbackup-scheduled-job-abs-test",
			applyDecoratorChain(
				buildPhysicalBackupWithABSStorage(testMdbkey, "test-physicalbackup", ""),
				decoratePhysicalBackupWithSchedule,
			),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a scheduled Job with S3 storage",
			"physicalbackup-scheduled-job-s3-test",
			applyDecoratorChain(
				buildPhysicalBackupWithS3Storage(testMdbkey, "test-physicalbackup", ""),
				decoratePhysicalBackupWithSchedule,
			),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a VolumeSnapshot",
			"physicalbackup-volumesnapshot-test",
			buildPhysicalBackupWithVolumeSnapshotStorage(testMdbkey),
			testPhysicalBackup,
		),
		Entry(
			"should reconcile a scheduled VolumeSnapshot",
			"physicalbackup-scheduled-volumesnapshot-test",
			applyDecoratorChain(
				buildPhysicalBackupWithVolumeSnapshotStorage(testMdbkey),
				decoratePhysicalBackupWithSchedule,
			),
			testPhysicalBackup,
		),
	)
})

var _ = Describe("PhysicalBackup retention protection", Label("basic"), func() {
	const (
		testPBName      = "physicalbackup-retention"
		testPITRName    = "pitr-retention"
		testNamespace   = "test-retention"
		testSnapshot    = testPBName + "-20230101000000"
		testInitialized = "mdb-initialized"
		testRestoring   = "mdb-restoring"
	)

	newReconciler := func(objects ...client.Object) *PhysicalBackupReconciler {
		scheme := runtime.NewScheme()
		Expect(mariadbv1alpha1.AddToScheme(scheme)).To(Succeed())
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objects...).
			Build()
		return &PhysicalBackupReconciler{
			Client:      fakeClient,
			RefResolver: refresolver.New(fakeClient),
		}
	}

	newPhysicalBackup := func(storage mariadbv1alpha1.PhysicalBackupStorage) *mariadbv1alpha1.PhysicalBackup {
		return &mariadbv1alpha1.PhysicalBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testPBName,
				Namespace: testNamespace,
			},
			Spec: mariadbv1alpha1.PhysicalBackupSpec{
				Storage: storage,
			},
		}
	}

	newMariaDB := func(name string, bootstrap *mariadbv1alpha1.BootstrapFrom, initialized bool) *mariadbv1alpha1.MariaDB {
		mdb := &mariadbv1alpha1.MariaDB{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: testNamespace,
			},
			Spec: mariadbv1alpha1.MariaDBSpec{
				BootstrapFrom: bootstrap,
			},
		}
		if initialized {
			mdb.Status.Conditions = []metav1.Condition{
				{
					Type:   mariadbv1alpha1.ConditionTypeInitialized,
					Status: metav1.ConditionTrue,
				},
			}
		}
		return mdb
	}

	s3Storage := mariadbv1alpha1.PhysicalBackupStorage{
		S3: &mariadbv1alpha1.S3{
			Bucket:   "backups",
			Endpoint: "s3.example.com",
			Prefix:   "mariadb",
		},
	}
	snapshotStorage := mariadbv1alpha1.PhysicalBackupStorage{
		VolumeSnapshot: &mariadbv1alpha1.PhysicalBackupVolumeSnapshot{
			VolumeSnapshotClassName: "csi",
		},
	}

	DescribeTable("computing restore protection",
		func(
			pb *mariadbv1alpha1.PhysicalBackup,
			objects []client.Object,
			wantBlocked bool,
			wantSnapshot string,
		) {
			reconciler := newReconciler(objects...)
			protection, err := reconciler.getRestoreProtection(context.Background(), pb)
			Expect(err).ToNot(HaveOccurred())
			Expect(protection.blocked).To(Equal(wantBlocked))
			if wantSnapshot != "" {
				Expect(protection.snapshotNames).To(HaveKey(wantSnapshot))
			} else {
				Expect(protection.snapshotNames).To(BeEmpty())
			}
		},
		Entry("no MariaDBs restores anything",
			newPhysicalBackup(s3Storage),
			nil,
			false,
			"",
		),
		Entry("initialized MariaDB does not block retention",
			newPhysicalBackup(s3Storage),
			[]client.Object{
				newMariaDB(testInitialized, &mariadbv1alpha1.BootstrapFrom{
					BackupRef: &mariadbv1alpha1.TypedLocalObjectReference{
						Name: testPBName,
						Kind: mariadbv1alpha1.PhysicalBackupKind,
					},
				}, true),
			},
			false,
			"",
		),
		Entry("direct PhysicalBackup reference blocks retention",
			newPhysicalBackup(s3Storage),
			[]client.Object{
				newMariaDB(testRestoring, &mariadbv1alpha1.BootstrapFrom{
					BackupRef: &mariadbv1alpha1.TypedLocalObjectReference{
						Name: testPBName,
						Kind: mariadbv1alpha1.PhysicalBackupKind,
					},
				}, false),
			},
			true,
			"",
		),
		Entry("reference to a different PhysicalBackup does not block",
			newPhysicalBackup(s3Storage),
			[]client.Object{
				newMariaDB(testRestoring, &mariadbv1alpha1.BootstrapFrom{
					BackupRef: &mariadbv1alpha1.TypedLocalObjectReference{
						Name: "another-physicalbackup",
						Kind: mariadbv1alpha1.PhysicalBackupKind,
					},
				}, false),
			},
			false,
			"",
		),
		Entry("point-in-time recovery reference blocks retention",
			newPhysicalBackup(s3Storage),
			[]client.Object{
				newMariaDB(testRestoring, &mariadbv1alpha1.BootstrapFrom{
					PointInTimeRecoveryRef: &mariadbv1alpha1.LocalObjectReference{
						Name: testPITRName,
					},
				}, false),
				&mariadbv1alpha1.PointInTimeRecovery{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testPITRName,
						Namespace: testNamespace,
					},
					Spec: mariadbv1alpha1.PointInTimeRecoverySpec{
						PhysicalBackupRef: mariadbv1alpha1.LocalObjectReference{
							Name: testPBName,
						},
					},
				},
			},
			true,
			"",
		),
		Entry("restore from the same S3 storage blocks retention",
			newPhysicalBackup(s3Storage),
			[]client.Object{
				newMariaDB(testRestoring, &mariadbv1alpha1.BootstrapFrom{
					S3: &mariadbv1alpha1.S3{
						Bucket:   "backups",
						Endpoint: "s3.example.com",
						Prefix:   "mariadb",
					},
				}, false),
			},
			true,
			"",
		),
		Entry("restore from a different S3 bucket does not block",
			newPhysicalBackup(s3Storage),
			[]client.Object{
				newMariaDB(testRestoring, &mariadbv1alpha1.BootstrapFrom{
					S3: &mariadbv1alpha1.S3{
						Bucket:   "other-backups",
						Endpoint: "s3.example.com",
						Prefix:   "mariadb",
					},
				}, false),
			},
			false,
			"",
		),
		Entry("VolumeSnapshot reference blocks snapshot retention and protects the snapshot",
			newPhysicalBackup(snapshotStorage),
			[]client.Object{
				newMariaDB(testRestoring, &mariadbv1alpha1.BootstrapFrom{
					VolumeSnapshotRef: &mariadbv1alpha1.LocalObjectReference{
						Name: testSnapshot,
					},
				}, false),
			},
			true,
			testSnapshot,
		),
		Entry("VolumeSnapshot reference does not block object storage retention",
			newPhysicalBackup(s3Storage),
			[]client.Object{
				newMariaDB(testRestoring, &mariadbv1alpha1.BootstrapFrom{
					VolumeSnapshotRef: &mariadbv1alpha1.LocalObjectReference{
						Name: testSnapshot,
					},
				}, false),
			},
			false,
			testSnapshot,
		),
	)
})

var _ = Describe("PhysicalBackup target", Label("basic"), func() {
	buildTargetFn := func(podIndex *int, err error) targetFn {
		return func(_ context.Context, _ *mariadbv1alpha1.MariaDB, _ logr.Logger) (*int, error) {
			return podIndex, err
		}
	}
	logger := logr.Discard()

	DescribeTable("Getting target Pod index",
		func(
			backup *mariadbv1alpha1.PhysicalBackup,
			mdb *mariadbv1alpha1.MariaDB,
			primaryFn targetFn,
			replicaFn targetFn,
			wantPodIndex *int,
			wantErr bool,
		) {
			podIndex, err := physicalBackupTargetWithFuncs(
				testCtx,
				backup,
				mdb,
				primaryFn,
				replicaFn,
				logger,
			)
			Expect(podIndex).To(BeEquivalentTo(wantPodIndex))

			if wantErr {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).ToNot(HaveOccurred())
			}
		},
		Entry(
			"standalone",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: nil,
				},
			},
			&mariadbv1alpha1.MariaDB{},
			buildTargetFn(ptr.To(0), nil),
			nil,
			ptr.To(0),
			false,
		),
		Entry(
			"standalone - no target available",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: nil,
				},
			},
			&mariadbv1alpha1.MariaDB{},
			buildTargetFn(nil, errPhysicalBackupNoTargetPodsAvailable),
			nil,
			nil,
			true,
		),
		Entry(
			"HA - default target",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: nil,
				},
			},
			&mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
			},
			buildTargetFn(ptr.To(0), nil),
			buildTargetFn(ptr.To(1), nil),
			ptr.To(1),
			false,
		),
		Entry(
			"HA - Replica target",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: ptr.To(mariadbv1alpha1.PhysicalBackupTargetReplica),
				},
			},
			&mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
			},
			nil,
			buildTargetFn(ptr.To(1), nil),
			ptr.To(1),
			false,
		),
		Entry(
			"HA - Replica target, no replicas available",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: ptr.To(mariadbv1alpha1.PhysicalBackupTargetReplica),
				},
			},
			&mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
			},
			nil,
			buildTargetFn(nil, errPhysicalBackupNoTargetPodsAvailable),
			nil,
			true,
		),
		Entry(
			"HA - Replica target, get target error",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: ptr.To(mariadbv1alpha1.PhysicalBackupTargetReplica),
				},
			},
			&mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
			},
			nil,
			buildTargetFn(nil, errors.New("error getting target")),
			nil,
			true,
		),
		Entry(
			"HA - PreferReplica target",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: ptr.To(mariadbv1alpha1.PhysicalBackupTargetPreferReplica),
				},
			},
			&mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
			},
			buildTargetFn(ptr.To(0), nil),
			buildTargetFn(ptr.To(1), nil),
			ptr.To(1),
			false,
		),
		Entry(
			"HA - PreferReplica target, no replicas available",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: ptr.To(mariadbv1alpha1.PhysicalBackupTargetPreferReplica),
				},
			},
			&mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
			},
			buildTargetFn(ptr.To(0), nil),
			buildTargetFn(nil, errPhysicalBackupNoTargetPodsAvailable),
			ptr.To(0),
			false,
		),
		Entry(
			"HA - PreferReplica target, no replicas nor primary available",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: ptr.To(mariadbv1alpha1.PhysicalBackupTargetPreferReplica),
				},
			},
			&mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
			},
			buildTargetFn(nil, errPhysicalBackupNoTargetPodsAvailable),
			buildTargetFn(nil, errPhysicalBackupNoTargetPodsAvailable),
			nil,
			true,
		),
		Entry(
			"HA - PreferReplica target, get target error",
			&mariadbv1alpha1.PhysicalBackup{
				Spec: mariadbv1alpha1.PhysicalBackupSpec{
					Target: ptr.To(mariadbv1alpha1.PhysicalBackupTargetPreferReplica),
				},
			},
			&mariadbv1alpha1.MariaDB{
				Spec: mariadbv1alpha1.MariaDBSpec{
					Replication: &mariadbv1alpha1.Replication{
						Enabled: true,
					},
				},
			},
			buildTargetFn(nil, errors.New("error getting target")),
			buildTargetFn(nil, errors.New("error getting target")),
			nil,
			true,
		),
	)
})
