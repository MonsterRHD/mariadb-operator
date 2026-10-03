// Package connection builds the connection data published by the Connection controller into a Secret.
package connection

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"text/template"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/hash"
	clientsql "github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
)

// BuildSecretData builds the whole set of Secret keys for a connection candidate: the DSN, the
// custom format when defined and the optional split fields. All keys are produced together so
// they are published atomically as a single generation.
func BuildSecretData(conn *mariadbv1alpha1.Connection, opts clientsql.Opts) (map[string][]byte, error) {
	dsn, err := clientsql.BuildDSN(opts)
	if err != nil {
		return nil, fmt.Errorf("error building DSN: %v", err)
	}

	data := map[string][]byte{
		conn.SecretKey(): []byte(dsn),
	}
	if formatString := conn.Spec.SecretTemplate.Format; formatString != nil {
		tmpl, err := template.New("").Parse(*formatString)
		if err != nil {
			return nil, fmt.Errorf("error parsing DSN template: %v", err)
		}
		builder := &strings.Builder{}
		if err := tmpl.Execute(builder, formatData(opts)); err != nil {
			return nil, fmt.Errorf("error executing DSN template: %v", err)
		}
		data[conn.SecretKey()] = []byte(builder.String())
	}
	if usernameKey := conn.Spec.SecretTemplate.UsernameKey; usernameKey != nil {
		data[*usernameKey] = []byte(opts.Username)
	}
	if passwordKey := conn.Spec.SecretTemplate.PasswordKey; passwordKey != nil {
		data[*passwordKey] = []byte(opts.Password)
	}
	if hostKey := conn.Spec.SecretTemplate.HostKey; hostKey != nil {
		data[*hostKey] = []byte(opts.Host)
	}
	if portKey := conn.Spec.SecretTemplate.PortKey; portKey != nil {
		data[*portKey] = []byte(strconv.Itoa(int(opts.Port)))
	}
	if databaseKey := conn.Spec.SecretTemplate.DatabaseKey; databaseKey != nil && opts.Database != "" {
		data[*databaseKey] = []byte(opts.Database)
	}
	return data, nil
}

func formatData(opts clientsql.Opts) map[string]string {
	return map[string]string{
		"Username": opts.Username,
		"Password": opts.Password,
		"Host":     opts.Host,
		"Port":     strconv.Itoa(int(opts.Port)),
		"Database": opts.Database,
		"Params": func() string {
			v := url.Values{}
			for key, value := range opts.Params {
				v.Add(key, value)
			}
			s := v.Encode()
			if s == "" {
				return s
			}
			return fmt.Sprintf("?%s", s)
		}(),
	}
}

// Version is the fingerprint of a connection generation: it changes whenever the referenced
// object, credentials, TLS material or templates that produce the Secret data change. It is
// persisted in the Connection status and used to detect stale candidates across restarts.
func Version(data map[string][]byte) (string, error) {
	return hash.HashJSON(data)
}
