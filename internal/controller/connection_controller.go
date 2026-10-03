package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/hashicorp/go-multierror"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	condition "github.com/mariadb-operator/mariadb-operator/v26/pkg/condition"
	connectionpkg "github.com/mariadb-operator/mariadb-operator/v26/pkg/connection"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/controller/secret"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/health"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/interfaces"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/pki"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/refresolver"
	clientsql "github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ConnectionReconciler reconciles a Connection object
type ConnectionReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	SecretReconciler *secret.SecretReconciler
	RefResolver      *refresolver.RefResolver
	ConditionReady   *condition.Ready
	RequeueInterval  time.Duration
}

//+kubebuilder:rbac:groups=k8s.mariadb.com,resources=connections,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=k8s.mariadb.com,resources=connections/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=k8s.mariadb.com,resources=connections/finalizers,verbs=update
//+kubebuilder:rbac:groups=k8s.mariadb.com,resources=mariadbs;maxscales;externalmariadbs,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=secrets,verbs=list;watch;create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *ConnectionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var conn mariadbv1alpha1.Connection
	if err := r.Get(ctx, req.NamespacedName, &conn); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	connRefs, err := r.getRefs(ctx, &conn)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error getting references: %v", err)
	}

	if result, err := r.waitForRefs(ctx, &conn, connRefs); !result.IsZero() || err != nil {
		return result, err
	}

	if err := r.reconcileDefaults(ctx, &conn, connRefs); err != nil {
		return ctrl.Result{}, fmt.Errorf("error setting defaults: %v", err)
	}

	if result, err := r.checkInfraHealth(ctx, &conn, connRefs); !result.IsZero() || err != nil {
		return result, err
	}

	// Build the candidate connection for the latest generation of the references and credentials.
	sqlOpts, err := r.getSqlOpts(ctx, &conn, connRefs)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error getting SQL options: %v", err)
	}
	candidateData, err := connectionpkg.BuildSecretData(&conn, sqlOpts)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error building connection data: %v", err)
	}
	candidateVersion, err := connectionpkg.Version(candidateData)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error computing connection version: %v", err)
	}

	// The candidate must pass the health check before being published; while it fails, the Secret
	// from the previous generation is kept untouched and only the retryable status is updated.
	if conn.Spec.HealthCheck != nil {
		if err := r.checkConnectivity(ctx, sqlOpts); err != nil {
			log.FromContext(ctx).Info("Connection candidate failed health check, keeping previous Secret", "err", err)
			if patchErr := r.patchStatus(ctx, &conn, r.ConditionReady.PatcherHealthy(err)); patchErr != nil {
				return ctrl.Result{}, fmt.Errorf("error checking connection health: %v: %v", err, patchErr)
			}
			return r.retryResult(&conn)
		}
	}

	published := ptr.Deref(conn.Status.SecretVersion, "")
	if published != candidateVersion || !r.secretExists(ctx, &conn) {
		if err := r.publishSecret(ctx, &conn, connRefs, candidateData); err != nil {
			return ctrl.Result{}, fmt.Errorf("error creating Secret: %v", err)
		}
	}

	conn.Status.SecretVersion = ptr.To(candidateVersion)
	if err := r.patchStatus(ctx, &conn, r.ConditionReady.PatcherHealthy(nil)); err != nil {
		return ctrl.Result{}, fmt.Errorf("error patching connection status: %v", err)
	}
	return r.healthResult(&conn)
}

func (r *ConnectionReconciler) getRefs(ctx context.Context, conn *mariadbv1alpha1.Connection) (*mariadbv1alpha1.ConnectionRefs, error) {
	if conn.Spec.MaxScaleRef != nil {
		mxs, refErr := r.RefResolver.MaxScale(ctx, conn.Spec.MaxScaleRef, conn.Namespace)
		if refErr != nil {
			var mariaDbErr *multierror.Error
			mariaDbErr = multierror.Append(mariaDbErr, refErr)

			patchErr := r.patchStatus(ctx, conn, r.ConditionReady.PatcherRefResolver(refErr, mxs))
			mariaDbErr = multierror.Append(mariaDbErr, patchErr)

			return nil, fmt.Errorf("error getting MaxScale: %w", mariaDbErr)
		}

		var mdb *mariadbv1alpha1.MariaDB
		var err error
		if mxs.Spec.MariaDBRef != nil {
			mdb, err = r.getMariadb(ctx, mxs.Spec.MariaDBRef, conn)
			if err != nil {
				return nil, err
			}
		}

		return &mariadbv1alpha1.ConnectionRefs{
			MaxScale: mxs,
			MariaDB:  mdb,
		}, nil
	}

	if conn.Spec.MariaDBRef != nil {
		if conn.Spec.MariaDBRef.Kind == mariadbv1alpha1.ExternalMariaDBKind {
			emdb, err := r.getExternalMariadb(ctx, conn.Spec.MariaDBRef, conn)
			if err != nil {
				return nil, err
			}
			return &mariadbv1alpha1.ConnectionRefs{
				ExternalMariaDB: emdb,
			}, nil
		}

		mdb, err := r.getMariadb(ctx, conn.Spec.MariaDBRef, conn)
		if err != nil {
			return nil, err
		}
		return &mariadbv1alpha1.ConnectionRefs{
			MariaDB: mdb,
		}, nil
	}

	return nil, errors.New("no references found")
}

func (r *ConnectionReconciler) getMariadb(ctx context.Context, ref *mariadbv1alpha1.MariaDBRef,
	conn *mariadbv1alpha1.Connection) (*mariadbv1alpha1.MariaDB, error) {
	mdb, refErr := r.RefResolver.MariaDB(ctx, ref, conn.Namespace)
	if refErr != nil {
		var mariaDbErr *multierror.Error
		mariaDbErr = multierror.Append(mariaDbErr, refErr)

		patchErr := r.patchStatus(ctx, conn, r.ConditionReady.PatcherRefResolver(refErr, mdb))
		mariaDbErr = multierror.Append(mariaDbErr, patchErr)

		return nil, fmt.Errorf("error getting MariaDB: %w", mariaDbErr)
	}
	return mdb, nil
}

func (r *ConnectionReconciler) getExternalMariadb(ctx context.Context, ref *mariadbv1alpha1.MariaDBRef,
	conn *mariadbv1alpha1.Connection) (*mariadbv1alpha1.ExternalMariaDB, error) {
	emdb, refErr := r.RefResolver.ExternalMariaDB(ctx, &ref.ObjectReference, conn.Namespace)
	if refErr != nil {
		var mariaDbErr *multierror.Error
		mariaDbErr = multierror.Append(mariaDbErr, refErr)

		patchErr := r.patchStatus(ctx, conn, r.ConditionReady.PatcherRefResolver(refErr, emdb))
		mariaDbErr = multierror.Append(mariaDbErr, patchErr)

		return nil, fmt.Errorf("error getting MariaDB: %w", mariaDbErr)
	}
	return emdb, nil
}

func (r *ConnectionReconciler) waitForRefs(ctx context.Context, conn *mariadbv1alpha1.Connection,
	refs *mariadbv1alpha1.ConnectionRefs) (ctrl.Result, error) {
	if conn.Spec.MariaDBRef != nil && refs.MariaDB != nil {
		if conn.Spec.MariaDBRef.WaitForIt && !refs.MariaDB.IsReady() {
			if err := r.patchStatus(ctx, conn, r.ConditionReady.PatcherFailed("MariaDB not ready")); err != nil {
				return ctrl.Result{}, fmt.Errorf("error patching Connection: %v", err)
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}
	if conn.Spec.MariaDBRef != nil && refs.ExternalMariaDB != nil {
		if conn.Spec.MariaDBRef.WaitForIt && !refs.ExternalMariaDB.IsReady() {
			if err := r.patchStatus(ctx, conn, r.ConditionReady.PatcherFailed("External MariaDB not ready")); err != nil {
				return ctrl.Result{}, fmt.Errorf("error patching Connection: %v", err)
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}
	if conn.Spec.MaxScaleRef != nil && refs.MaxScale != nil {
		if !refs.MaxScale.IsReady() {
			if err := r.patchStatus(ctx, conn, r.ConditionReady.PatcherFailed("MaxScale not ready")); err != nil {
				return ctrl.Result{}, fmt.Errorf("error patching Connection: %v", err)
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}
	return ctrl.Result{}, nil
}

// reconcileDefaults migrates values persisted by older operator versions and applies the
// parent-independent defaults. Explicit host, port and serviceName are never touched.
func (r *ConnectionReconciler) reconcileDefaults(ctx context.Context, conn *mariadbv1alpha1.Connection,
	refs *mariadbv1alpha1.ConnectionRefs) error {
	before := conn.DeepCopy()
	if _, err := conn.MigrateDerivedFields(refs); err != nil {
		return fmt.Errorf("error migrating derived fields: %v", err)
	}
	conn.SetDefaults()

	if reflect.DeepEqual(before.Spec, conn.Spec) && reflect.DeepEqual(before.Annotations, conn.Annotations) {
		return nil
	}
	if err := r.Patch(ctx, conn, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("error patching Connection: %v", err)
	}
	return nil
}

func (r *ConnectionReconciler) checkInfraHealth(ctx context.Context, conn *mariadbv1alpha1.Connection,
	refs *mariadbv1alpha1.ConnectionRefs) (ctrl.Result, error) {
	port, err := conn.EffectivePort(refs)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error getting effective port: %v", err)
	}
	if refs.MariaDB != nil {
		healthy, err := health.IsStatefulSetHealthy(
			ctx,
			r.Client,
			client.ObjectKeyFromObject(refs.MariaDB),
			health.WithDesiredReplicas(refs.MariaDB.Spec.Replicas),
			health.WithPort(port),
			health.WithEndpointPolicy(health.EndpointPolicyAtLeastOne),
		)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("error checking MariaDB health: %v", err)
		}
		if !healthy {
			if err := r.patchStatus(ctx, conn, r.ConditionReady.PatcherFailed("MariaDB not healthy")); err != nil {
				return ctrl.Result{}, fmt.Errorf("error patching Connection: %v", err)
			}
			return r.retryResult(conn)
		}
	}
	if refs.MaxScale != nil {
		healthy, err := health.IsStatefulSetHealthy(
			ctx,
			r.Client,
			client.ObjectKeyFromObject(refs.MaxScale),
			health.WithDesiredReplicas(refs.MaxScale.Spec.Replicas),
			health.WithPort(port),
			health.WithEndpointPolicy(health.EndpointPolicyAtLeastOne),
		)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("error checking MaxScale health: %v", err)
		}
		if !healthy {
			if err := r.patchStatus(ctx, conn, r.ConditionReady.PatcherFailed("MaxScale not healthy")); err != nil {
				return ctrl.Result{}, fmt.Errorf("error patching Connection: %v", err)
			}
			return r.retryResult(conn)
		}
	}
	return ctrl.Result{}, nil
}

func (r *ConnectionReconciler) secretExists(ctx context.Context, conn *mariadbv1alpha1.Connection) bool {
	var secret corev1.Secret
	key := types.NamespacedName{
		Name:      conn.SecretName(),
		Namespace: conn.Namespace,
	}
	if err := r.Get(ctx, key, &secret); err != nil {
		return false
	}
	return true
}

// publishSecret writes the whole candidate generation (DSN and split fields) in a single Secret
// create or patch.
func (r *ConnectionReconciler) publishSecret(ctx context.Context, conn *mariadbv1alpha1.Connection,
	refs *mariadbv1alpha1.ConnectionRefs, data map[string][]byte) error {
	var meta []*mariadbv1alpha1.Metadata
	if refs.MariaDB != nil && refs.MariaDB.Spec.InheritMetadata != nil {
		meta = append(meta, refs.MariaDB.Spec.InheritMetadata)
	}
	if conn.Spec.SecretTemplate.Metadata != nil {
		meta = append(meta, conn.Spec.SecretTemplate.Metadata)
	}

	req := secret.SecretRequest{
		Owner:    conn,
		Metadata: meta,
		Key: types.NamespacedName{
			Name:      conn.SecretName(),
			Namespace: conn.Namespace,
		},
		Data: data,
	}
	if err := r.SecretReconciler.Reconcile(ctx, &req); err != nil {
		return fmt.Errorf("error reconciling Secret: %v", err)
	}
	return nil
}

func (r *ConnectionReconciler) getSqlOpts(ctx context.Context, conn *mariadbv1alpha1.Connection,
	refs *mariadbv1alpha1.ConnectionRefs) (clientsql.Opts, error) {
	host, err := conn.EffectiveHost(refs)
	if err != nil {
		return clientsql.Opts{}, fmt.Errorf("error getting effective host: %v", err)
	}
	port, err := conn.EffectivePort(refs)
	if err != nil {
		return clientsql.Opts{}, fmt.Errorf("error getting effective port: %v", err)
	}
	sqlOpts := clientsql.Opts{
		Username: conn.Spec.Username,
		Host:     host,
		Port:     port,
		Params:   conn.Spec.Params,
	}
	if conn.Spec.PasswordSecretKeyRef != nil {
		password, err := r.RefResolver.SecretKeyRef(ctx, *conn.Spec.PasswordSecretKeyRef, conn.Namespace)
		if err != nil {
			return clientsql.Opts{}, fmt.Errorf("error getting password for connection DSN: %v", err)
		}
		sqlOpts.Password = password
	}

	if conn.Spec.Database != nil {
		sqlOpts.Database = *conn.Spec.Database
	}
	if mxs := refs.MaxScale; mxs != nil && mxs.IsTLSEnabled() {
		caBundle, err := r.RefResolver.SecretKeyRef(ctx, mxs.TLSCABundleSecretKeyRef(), mxs.Namespace)
		if err != nil {
			return clientsql.Opts{}, fmt.Errorf("error getting MaxScale CA bundle: %v", err)
		}
		sqlOpts.TLSCACert = []byte(caBundle)
		sqlOpts.MaxscaleName = mxs.Name
		sqlOpts.Namespace = mxs.Namespace

	} else if mdb := refs.MariaDB; mdb != nil && mdb.IsTLSEnabled() {
		caBundle, err := r.RefResolver.SecretKeyRef(ctx, mdb.TLSCABundleSecretKeyRef(), mdb.Namespace)
		if err != nil {
			return clientsql.Opts{}, fmt.Errorf("error getting MariaDB CA bundle: %v", err)
		}
		sqlOpts.TLSCACert = []byte(caBundle)
		sqlOpts.MariadbName = mdb.Name
		sqlOpts.Namespace = mdb.Namespace
	} else if emdb := refs.ExternalMariaDB; emdb != nil && emdb.IsTLSEnabled() {
		caBundle, err := r.RefResolver.SecretKeyRef(ctx, emdb.TLSCABundleSecretKeyRef(), emdb.Namespace)
		if err != nil {
			return clientsql.Opts{}, fmt.Errorf("error getting External MariaDB CA bundle: %v", err)
		}
		sqlOpts.TLSCACert = []byte(caBundle)
		sqlOpts.ExternalMariadbName = emdb.Name
		sqlOpts.Namespace = emdb.Namespace
		if err := r.addSqlClientCertOpts(ctx, conn, refs.ExternalMariaDB, &sqlOpts); err != nil {
			return clientsql.Opts{}, fmt.Errorf("error adding SQL client opts: %v", err)
		}
		return sqlOpts, nil
	} else if emdb := refs.ExternalMariaDB; emdb != nil && !emdb.IsTLSEnabled() {
		return sqlOpts, nil
	}

	if err := r.addSqlClientCertOpts(ctx, conn, refs.MariaDB, &sqlOpts); err != nil {
		return clientsql.Opts{}, fmt.Errorf("error adding SQL client opts: %v", err)
	}
	return sqlOpts, nil
}

func (r *ConnectionReconciler) addSqlClientCertOpts(ctx context.Context, conn *mariadbv1alpha1.Connection,
	mdb interfaces.MariaDBObject, opts *clientsql.Opts) error {
	secretKey := r.clientCertSecretKey(conn, mdb)
	if secretKey == nil {
		return nil
	}
	opts.ClientName = secretKey.Name

	clientCertSelector := mariadbv1alpha1.SecretKeySelector{
		LocalObjectReference: mariadbv1alpha1.LocalObjectReference{
			Name: secretKey.Name,
		},
		Key: pki.TLSCertKey,
	}
	clientCert, err := r.RefResolver.SecretKeyRef(ctx, clientCertSelector, secretKey.Namespace)
	if err != nil {
		return fmt.Errorf("error getting client certificate: %v", err)
	}
	opts.TLSClientCert = []byte(clientCert)

	clientPrivateKeySelector := mariadbv1alpha1.SecretKeySelector{
		LocalObjectReference: mariadbv1alpha1.LocalObjectReference{
			Name: secretKey.Name,
		},
		Key: pki.TLSKeyKey,
	}
	clientPrivateKey, err := r.RefResolver.SecretKeyRef(ctx, clientPrivateKeySelector, secretKey.Namespace)
	if err != nil {
		return fmt.Errorf("error getting client certificate: %v", err)
	}
	opts.TLSClientPrivateKey = []byte(clientPrivateKey)

	return nil
}
func (r *ConnectionReconciler) clientCertSecretKey(conn *mariadbv1alpha1.Connection,
	mdb interfaces.MariaDBObject) *types.NamespacedName {
	if conn != nil && conn.Spec.TLSClientCertSecretRef != nil {
		return &types.NamespacedName{
			Name:      conn.Spec.TLSClientCertSecretRef.Name,
			Namespace: conn.Namespace,
		}
	}
	if mdb != nil && mdb.IsTLSEnabled() && mdb.IsTLSMutual() {
		return &types.NamespacedName{
			Name:      mdb.TLSClientCertSecretKey().Name,
			Namespace: mdb.GetNamespace(),
		}
	}
	return nil
}

// checkConnectivity verifies the candidate can actually open a database connection.
func (r *ConnectionReconciler) checkConnectivity(ctx context.Context, clientOpts clientsql.Opts) error {
	log.FromContext(ctx).V(1).Info("Checking connection health")
	db, err := clientsql.ConnectWithOpts(clientOpts)
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer db.Close()
	return nil
}

func (r *ConnectionReconciler) retryResult(conn *mariadbv1alpha1.Connection) (ctrl.Result, error) {
	if conn.Spec.HealthCheck != nil && conn.Spec.HealthCheck.RetryInterval != nil {
		return ctrl.Result{RequeueAfter: (*conn.Spec.HealthCheck.RetryInterval).Duration}, nil
	}
	return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
}

func (r *ConnectionReconciler) healthResult(conn *mariadbv1alpha1.Connection) (ctrl.Result, error) {
	if conn.Spec.HealthCheck != nil && conn.Spec.HealthCheck.Interval != nil {
		return ctrl.Result{RequeueAfter: (*conn.Spec.HealthCheck.Interval).Duration}, nil
	}
	return ctrl.Result{}, nil
}

func (r *ConnectionReconciler) patchStatus(ctx context.Context, conn *mariadbv1alpha1.Connection,
	patcher condition.Patcher) error {
	patch := client.MergeFrom(conn.DeepCopy())
	patcher(&conn.Status)

	if err := r.Client.Status().Patch(ctx, conn, patch); err != nil {
		return fmt.Errorf("error patching connection status: %v", err)
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ConnectionReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	builder := ctrl.NewControllerManagedBy(mgr).
		For(&mariadbv1alpha1.Connection{}).
		Owns(&corev1.Secret{})

	if err := mariadbv1alpha1.IndexConnection(ctx, mgr, builder, r.Client); err != nil {
		return fmt.Errorf("error indexing Connection: %v", err)
	}

	return builder.Complete(r)
}
