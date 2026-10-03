package v1alpha1

import (
	"context"
	"fmt"

	"github.com/mariadb-operator/mariadb-operator/v26/pkg/metadata"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/predicate"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/watch"
	corev1 "k8s.io/api/core/v1"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	connectionPasswordSecretFieldPath      = ".spec.passwordSecretKeyRef.name"
	connectionTLSClientCertSecretFieldPath = ".spec.tlsClientCertSecretRef.name"
	connectionMariaDBRefFieldPath          = ".spec.mariaDbRef.name"
	// connectionExternalMariaDBRefFieldPath is an index-only key (not a real spec field path)
	// used to distinguish ExternalMariaDB references, which share spec.mariaDbRef.name.
	connectionExternalMariaDBRefFieldPath = ".spec.mariaDbRef.externalName"
	connectionMaxScaleRefFieldPath        = ".spec.maxScaleRef.name"
)

// IndexerFuncForFieldPath returns an indexer function for a given field path.
func (c *Connection) IndexerFuncForFieldPath(fieldPath string) (client.IndexerFunc, error) {
	switch fieldPath {
	case connectionPasswordSecretFieldPath:
		return func(obj client.Object) []string {
			connection, ok := obj.(*Connection)
			if !ok {
				return nil
			}
			if connection.Spec.PasswordSecretKeyRef != nil && connection.Spec.PasswordSecretKeyRef.Name != "" {
				return []string{connection.Spec.PasswordSecretKeyRef.Name}
			}
			return nil
		}, nil
	case connectionTLSClientCertSecretFieldPath:
		return func(obj client.Object) []string {
			connection, ok := obj.(*Connection)
			if !ok {
				return nil
			}
			if connection.Spec.TLSClientCertSecretRef != nil && connection.Spec.TLSClientCertSecretRef.Name != "" {
				return []string{connection.Spec.TLSClientCertSecretRef.Name}
			}
			return nil
		}, nil
	case connectionMariaDBRefFieldPath, connectionExternalMariaDBRefFieldPath:
		external := fieldPath == connectionExternalMariaDBRefFieldPath
		return func(obj client.Object) []string {
			connection, ok := obj.(*Connection)
			if !ok || connection.Spec.MariaDBRef == nil || connection.Spec.MariaDBRef.Name == "" {
				return nil
			}
			if (connection.Spec.MariaDBRef.Kind == ExternalMariaDBKind) != external {
				return nil
			}
			return []string{connection.Spec.MariaDBRef.Name}
		}, nil
	case connectionMaxScaleRefFieldPath:
		return func(obj client.Object) []string {
			connection, ok := obj.(*Connection)
			if !ok {
				return nil
			}
			if connection.Spec.MaxScaleRef != nil && connection.Spec.MaxScaleRef.Name != "" {
				return []string{connection.Spec.MaxScaleRef.Name}
			}
			return nil
		}, nil
	default:
		return nil, fmt.Errorf("unsupported field path: %s", fieldPath)
	}
}

// IndexConnection watches and indexes external resources referred by Connection resources.
func IndexConnection(ctx context.Context, mgr manager.Manager, builder *ctrlbuilder.Builder, cl client.Client) error {
	watcherIndexer := watch.NewWatcherIndexer(mgr, builder, cl)

	secretFieldPaths := []string{
		connectionPasswordSecretFieldPath,
		connectionTLSClientCertSecretFieldPath,
	}
	for _, fieldPath := range secretFieldPaths {
		if err := watcherIndexer.Watch(
			ctx,
			&corev1.Secret{},
			&Connection{},
			&ConnectionList{},
			fieldPath,
			ctrlbuilder.WithPredicates(
				predicate.PredicateWithLabel(metadata.WatchLabel),
			),
		); err != nil {
			return fmt.Errorf("error watching '%s': %v", fieldPath, err)
		}
	}

	// Recompute the connection when the referenced object spec changes (port, service entry,
	// credentials, certificates or embedded templates).
	refWatches := []struct {
		obj       client.Object
		fieldPath string
	}{
		{obj: &MariaDB{}, fieldPath: connectionMariaDBRefFieldPath},
		{obj: &ExternalMariaDB{}, fieldPath: connectionExternalMariaDBRefFieldPath},
		{obj: &MaxScale{}, fieldPath: connectionMaxScaleRefFieldPath},
	}
	for _, refWatch := range refWatches {
		if err := watcherIndexer.Watch(
			ctx,
			refWatch.obj,
			&Connection{},
			&ConnectionList{},
			refWatch.fieldPath,
			ctrlbuilder.WithPredicates(
				ctrlpredicate.GenerationChangedPredicate{},
			),
		); err != nil {
			return fmt.Errorf("error watching '%s': %v", refWatch.fieldPath, err)
		}
	}

	return nil
}
