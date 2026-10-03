package controller

import (
	"context"
	"fmt"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	maxscaleconfigchange "github.com/mariadb-operator/mariadb-operator/v26/internal/controller/maxscaleconfigchange"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// reconcileConfigChange delegates the generation-scoped, recoverable graph transaction to the
// injectable change runner. The previous MaxScale graph keeps serving until every replica has
// converged on the prepared graph, and only managed objects are cleaned afterwards.
func (r *MaxScaleReconciler) reconcileConfigChange(ctx context.Context, req *requestMaxScale) (ctrl.Result, error) {
	mxs := req.mxs
	writerAPI := newMxsConfigAPI(mxs, req.client, r.RefResolver)
	podAPIs := make(map[string]maxscaleconfigchange.REST, len(req.podClientSet))
	for podName, podClient := range req.podClientSet {
		podAPIs[podName] = newMxsConfigAPI(mxs, podClient, r.RefResolver)
	}

	runner := &maxscaleconfigchange.Runner{
		Status: configChangeStatus{reconciler: r, mxs: mxs},
		Events: r.Recorder,
		Refs:   configChangeRefs{reconciler: r},
		Now:    metav1.Now,
		DatabaseVersion: func(ctx context.Context, mxs *mariadbv1alpha1.MaxScale) (int, error) {
			return r.getDatabaseConfigSyncVersion(ctx, mxs)
		},
	}
	return runner.Reconcile(ctx, mxs, writerAPI, podAPIs)
}

type configChangeStatus struct {
	reconciler *MaxScaleReconciler
	mxs        *mariadbv1alpha1.MaxScale
}

func (s configChangeStatus) PatchConfigChange(ctx context.Context,
	patcher func(*mariadbv1alpha1.MaxScaleConfigChangeStatus) error) error {
	return s.reconciler.patchStatus(ctx, s.mxs, func(mss *mariadbv1alpha1.MaxScaleStatus) error {
		if mss.ConfigChange == nil {
			mss.ConfigChange = &mariadbv1alpha1.MaxScaleConfigChangeStatus{}
		}
		return patcher(mss.ConfigChange)
	})
}

type configChangeRefs struct {
	reconciler *MaxScaleReconciler
}

func (c configChangeRefs) Validate(ctx context.Context, mxs *mariadbv1alpha1.MaxScale) error {
	return c.reconciler.validateConfigChangeRefs(ctx, mxs)
}

// validateConfigChangeRefs resolves the dynamic references required by the desired graph before
// any REST mutation happens: unresolved monitor/service passwords or TLS CA abort the change.
func (r *MaxScaleReconciler) validateConfigChangeRefs(ctx context.Context,
	mxs *mariadbv1alpha1.MaxScale) error {
	if _, err := r.RefResolver.SecretKeyRef(ctx,
		mxs.Spec.Auth.MonitorPasswordSecretKeyRef.SecretKeySelector, mxs.Namespace); err != nil {
		return fmt.Errorf("monitor password reference is not resolvable: %v", err)
	}
	if _, err := r.RefResolver.SecretKeyRef(ctx,
		mxs.Spec.Auth.ServerPasswordSecretKeyRef.SecretKeySelector, mxs.Namespace); err != nil {
		return fmt.Errorf("server password reference is not resolvable: %v", err)
	}
	if mxs.IsTLSEnabled() {
		if _, err := r.RefResolver.SecretKeyRef(ctx, mxs.TLSCABundleSecretKeyRef(), mxs.Namespace); err != nil {
			return fmt.Errorf("TLS CA bundle reference is not resolvable: %v", err)
		}
	}
	return nil
}
