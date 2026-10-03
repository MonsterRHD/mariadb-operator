package controller

import (
	"context"
	"fmt"
	"reflect"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// reconcileEmbeddedConnection creates a Connection owned by a parent (MariaDB, MaxScale or
// ExternalMariaDB) or reconciles the spec derived from the parent embedded template. Template,
// port and initial credential changes made on the parent are propagated to the child, while
// fields the child owns itself (the explicit host and the immutable Secret name) are preserved.
func reconcileEmbeddedConnection(ctx context.Context, c client.Client, desired *mariadbv1alpha1.Connection) error {
	desired.SetDefaults()

	key := client.ObjectKeyFromObject(desired)
	var existing mariadbv1alpha1.Connection
	err := c.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		if err := c.Create(ctx, desired); err != nil {
			return fmt.Errorf("error creating Connection: %v", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("error getting Connection: %v", err)
	}

	before := existing.DeepCopy()
	existing.Spec.Username = desired.Spec.Username
	existing.Spec.PasswordSecretKeyRef = desired.Spec.PasswordSecretKeyRef
	existing.Spec.Database = desired.Spec.Database
	existing.Spec.SecretTemplate = desired.Spec.SecretTemplate
	existing.Spec.HealthCheck = desired.Spec.HealthCheck
	existing.Spec.Params = desired.Spec.Params
	existing.Spec.Port = desired.Spec.Port
	existing.Spec.ServiceName = desired.Spec.ServiceName

	if reflect.DeepEqual(before.Spec, existing.Spec) {
		return nil
	}
	if err := c.Patch(ctx, &existing, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("error patching Connection: %v", err)
	}
	return nil
}
