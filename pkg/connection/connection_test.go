package connection

import (
	"testing"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	clientsql "github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	"k8s.io/utils/ptr"
)

func newTestConnection() *mariadbv1alpha1.Connection {
	return &mariadbv1alpha1.Connection{
		Spec: mariadbv1alpha1.ConnectionSpec{
			ConnectionTemplate: mariadbv1alpha1.ConnectionTemplate{
				SecretTemplate: &mariadbv1alpha1.SecretTemplate{
					Key: ptr.To("dsn"),
				},
			},
		},
	}
}

func newTestOpts() clientsql.Opts {
	return clientsql.Opts{
		Username: "app",
		Password: "pass",
		Host:     "mariadb.default.svc.cluster.local",
		Port:     3306,
		Database: "app",
	}
}

func TestBuildSecretData(t *testing.T) {
	conn := newTestConnection()
	conn.Spec.SecretTemplate.UsernameKey = ptr.To("username")
	conn.Spec.SecretTemplate.PasswordKey = ptr.To("password")
	conn.Spec.SecretTemplate.HostKey = ptr.To("host")
	conn.Spec.SecretTemplate.PortKey = ptr.To("port")
	conn.Spec.SecretTemplate.DatabaseKey = ptr.To("database")
	conn.Spec.SecretTemplate.Format = ptr.To("mysql://{{ .Username }}:{{ .Password }}@{{ .Host }}:{{ .Port }}/{{ .Database }}")

	data, err := BuildSecretData(conn, newTestOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := map[string]string{
		"dsn":      "mysql://app:pass@mariadb.default.svc.cluster.local:3306/app",
		"username": "app",
		"password": "pass",
		"host":     "mariadb.default.svc.cluster.local",
		"port":     "3306",
		"database": "app",
	}
	for key, value := range want {
		if string(data[key]) != value {
			t.Errorf("key %q = %q, want %q", key, string(data[key]), value)
		}
	}
}

func TestBuildSecretDataInvalidTemplate(t *testing.T) {
	conn := newTestConnection()
	conn.Spec.SecretTemplate.Format = ptr.To("mysql://{{ .Invalid ")

	if _, err := BuildSecretData(conn, newTestOpts()); err == nil {
		t.Fatal("expected error for invalid template, got nil")
	}
}

func TestVersionChangesWithGeneration(t *testing.T) {
	conn := newTestConnection()
	opts := newTestOpts()

	data, err := BuildSecretData(conn, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	version, err := Version(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Same inputs must always produce the same version (restart safe).
	sameData, err := BuildSecretData(conn, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sameVersion, err := Version(sameData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if version != sameVersion {
		t.Errorf("version not stable: %q != %q", version, sameVersion)
	}

	// A parent endpoint change produces a new generation.
	changedOpts := opts
	changedOpts.Host = "mariadb-primary.default.svc.cluster.local"
	changedData, err := BuildSecretData(conn, changedOpts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	changedVersion, err := Version(changedData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if version == changedVersion {
		t.Errorf("version did not change after host update: %q", version)
	}

	// A credential change produces a new generation.
	credOpts := opts
	credOpts.Password = "new-pass"
	credData, err := BuildSecretData(conn, credOpts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	credVersion, err := Version(credData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if version == credVersion {
		t.Errorf("version did not change after password update: %q", version)
	}
}
