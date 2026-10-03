package v1alpha1

import (
	"errors"

	"github.com/mariadb-operator/mariadb-operator/v26/pkg/metadata"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/statefulset"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	defaultConnSecretKey = "dsn"
)

// ConnectionRefs contains the resolved references of a Connection.
type ConnectionRefs struct {
	MariaDB         *MariaDB
	MaxScale        *MaxScale
	ExternalMariaDB *ExternalMariaDB
}

// Host returns the host address to connect to.
func (r *ConnectionRefs) Host(c *Connection) (*string, error) {
	if r.ExternalMariaDB != nil {
		return &r.ExternalMariaDB.Spec.Host, nil
	}
	objMeta, err := r.objectMeta()
	if err != nil {
		return nil, err
	}
	if c.Spec.ServiceName != nil {
		svcMeta := metav1.ObjectMeta{
			Name:      *c.Spec.ServiceName,
			Namespace: objMeta.Namespace,
		}
		return ptr.To(statefulset.ServiceFQDN(svcMeta)), nil
	}

	// Use the primary Service when HA is enabled to ensure writes go to the primary node
	// Only apply this for direct MariaDB connections, not for MaxScale connections.
	if r.MaxScale == nil && r.MariaDB != nil && r.MariaDB.IsHAEnabled() {
		primarySvcMeta := metav1.ObjectMeta{
			Name:      r.MariaDB.PrimaryServiceKey().Name,
			Namespace: objMeta.Namespace,
		}
		return ptr.To(statefulset.ServiceFQDN(primarySvcMeta)), nil
	}

	return ptr.To(statefulset.ServiceFQDN(*objMeta)), nil
}

// Port returns the port to connect to.
func (r *ConnectionRefs) Port() (*int32, error) {
	if r.ExternalMariaDB != nil {
		return &r.ExternalMariaDB.Spec.Port, nil
	}
	if r.MaxScale != nil {
		return r.MaxScale.DefaultPort()
	}
	if r.MariaDB != nil {
		return &r.MariaDB.Spec.Port, nil
	}
	return nil, errors.New("port not found")
}

func (r *ConnectionRefs) objectMeta() (*metav1.ObjectMeta, error) {
	if r.ExternalMariaDB != nil {
		return &r.ExternalMariaDB.ObjectMeta, nil
	}
	if r.MaxScale != nil {
		return &r.MaxScale.ObjectMeta, nil
	}
	if r.MariaDB != nil {
		return &r.MariaDB.ObjectMeta, nil
	}
	return nil, errors.New("references not found")
}

// ConnectionSpec defines the desired state of Connection
type ConnectionSpec struct {
	// ContainerTemplate defines templates to configure Container objects.
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	ConnectionTemplate `json:",inline"`
	// MariaDBRef is a reference to the MariaDB to connect to. Either MariaDBRef or MaxScaleRef must be provided.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	MariaDBRef *MariaDBRef `json:"mariaDbRef,omitempty"`
	// MaxScaleRef is a reference to the MaxScale to connect to. Either MariaDBRef or MaxScaleRef must be provided.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	MaxScaleRef *ObjectReference `json:"maxScaleRef,omitempty"`
	// Username to use for configuring the Connection.
	// +kubebuilder:validation:Required
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Username string `json:"username"`
	// PasswordSecretKeyRef is a reference to the password to use for configuring the Connection.
	// Either passwordSecretKeyRef or tlsClientCertSecretRef must be provided as client credentials.
	// If the referred Secret is labeled with "k8s.mariadb.com/watch", updates may be performed to the Secret in order to update the password.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	PasswordSecretKeyRef *SecretKeySelector `json:"passwordSecretKeyRef,omitempty"`
	// TLSClientCertSecretRef is a reference to a Kubernetes TLS Secret used as authentication when checking the connection health.
	// Either passwordSecretKeyRef or tlsClientCertSecretRef must be provided as client credentials.
	// If not provided, the client certificate provided by the referred MariaDB is used if TLS is enabled.
	// If the referred Secret is labeled with "k8s.mariadb.com/watch", updates may be performed to the Secret in order to update the client certificate.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	TLSClientCertSecretRef *LocalObjectReference `json:"tlsClientCertSecretRef,omitempty"`
	// Host to connect to. If not provided, it defaults to the MariaDB host or to the MaxScale host.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,xDescriptors={"urn:alm:descriptor:com.tectonic.ui:number","urn:alm:descriptor:com.tectonic.ui:advanced"}
	Host string `json:"host,omitempty"`
	// Database to use when configuring the Connection.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Database *string `json:"database,omitempty"`
}

// ConnectionStatus defines the observed state of Connection
type ConnectionStatus struct {
	// Conditions for the Connection object.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,xDescriptors={"urn:alm:descriptor:io.kubernetes.conditions"}
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// SecretVersion is the fingerprint of the connection data currently published to the Secret.
	// It identifies the generation of the referenced object, credentials and templates that the Secret was built from.
	// When a newer candidate is available, it is health-checked first and the Secret is only republished once it passes,
	// keeping this field in sync with the data the Secret holds.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status
	SecretVersion *string `json:"secretVersion,omitempty"`
}

func (c *ConnectionStatus) SetCondition(condition metav1.Condition) {
	if c.Conditions == nil {
		c.Conditions = make([]metav1.Condition, 0)
	}
	meta.SetStatusCondition(&c.Conditions, condition)
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=cmdb
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].message"
// +kubebuilder:printcolumn:name="Secret",type="string",JSONPath=".spec.secretName"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +operator-sdk:csv:customresourcedefinitions:resources={{Connection,v1alpha1},{Secret,v1}}

// Connection is the Schema for the connections API. It is used to configure connection strings for the applications connecting to MariaDB.
type Connection struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ConnectionSpec   `json:"spec,omitempty"`
	Status ConnectionStatus `json:"status,omitempty"`
}

func (c *Connection) IsReady() bool {
	return meta.IsStatusConditionTrue(c.Status.Conditions, ConditionTypeReady)
}

// SetDefaults defaults the fields that do not depend on the referenced object: the Secret name
// and the Secret template key. Host and port are intentionally not persisted, they are recomputed
// from the references on every reconciliation (see EffectiveHost and EffectivePort), so changes
// in the referenced MariaDB, MaxScale or ExternalMariaDB are always reflected.
func (c *Connection) SetDefaults() {
	if c.Spec.SecretName == nil {
		c.Spec.SecretName = &c.Name
	}
	if c.Spec.SecretTemplate == nil {
		c.Spec.SecretTemplate = &SecretTemplate{
			Key: &defaultConnSecretKey,
		}
	} else if c.Spec.SecretTemplate.Key == nil {
		c.Spec.SecretTemplate.Key = &defaultConnSecretKey
	}
}

// EffectiveHost returns the host the Connection must use: the explicitly configured host when
// present, or the host derived from the referenced object. Explicit values are never replaced
// by defaults.
func (c *Connection) EffectiveHost(refs *ConnectionRefs) (string, error) {
	if c.Spec.Host != "" {
		return c.Spec.Host, nil
	}
	host, err := refs.Host(c)
	if err != nil {
		return "", err
	}
	return *host, nil
}

// EffectivePort returns the port the Connection must use: the explicitly configured port when
// present, or the port derived from the referenced object.
func (c *Connection) EffectivePort(refs *ConnectionRefs) (int32, error) {
	if c.Spec.Port != 0 {
		return c.Spec.Port, nil
	}
	port, err := refs.Port()
	if err != nil {
		return 0, err
	}
	return *port, nil
}

// MigrateDerivedFields moves host and port values persisted into the spec by older operator
// versions into reference-derived values. It only clears values that match the endpoints the
// operator would derive itself (operator-owned Service FQDNs and the referenced port), keeping
// explicitly configured values untouched. The migration runs once, tracked by an annotation.
// It returns whether the Connection was modified.
func (c *Connection) MigrateDerivedFields(refs *ConnectionRefs) (bool, error) {
	if c.Annotations != nil {
		if _, ok := c.Annotations[metadata.ConnectionDerivedFieldsAnnotation]; ok {
			return false, nil
		}
	}
	defer func() {
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations[metadata.ConnectionDerivedFieldsAnnotation] = "true"
	}()

	derivedHosts, err := c.derivedHosts(refs)
	if err != nil {
		return false, err
	}
	if c.Spec.Host != "" {
		for _, host := range derivedHosts {
			if c.Spec.Host == host {
				c.Spec.Host = ""
				break
			}
		}
	}
	if c.Spec.Port != 0 {
		port, err := refs.Port()
		if err != nil {
			return false, err
		}
		if c.Spec.Port == *port {
			c.Spec.Port = 0
		}
	}
	return true, nil
}

// derivedHosts returns the Service FQDNs the operator derives a host from, including the naming
// variants older versions may have persisted into the spec.
func (c *Connection) derivedHosts(refs *ConnectionRefs) ([]string, error) {
	hosts := []string{}
	objMeta, err := refs.objectMeta()
	if err != nil {
		return nil, err
	}
	// Service named after the referenced object.
	hosts = append(hosts, statefulset.ServiceFQDN(*objMeta))
	// Current derivation: explicit serviceName or the HA primary Service.
	if host, err := refs.Host(c); err == nil && host != nil {
		hosts = append(hosts, *host)
	}
	if refs.MariaDB != nil {
		hosts = append(hosts,
			statefulset.ServiceFQDN(metav1.ObjectMeta{
				Name:      refs.MariaDB.PrimaryServiceKey().Name,
				Namespace: objMeta.Namespace,
			}),
			statefulset.ServiceFQDN(metav1.ObjectMeta{
				Name:      refs.MariaDB.SecondaryServiceKey().Name,
				Namespace: objMeta.Namespace,
			}),
		)
	}
	return hosts, nil
}

func (c *Connection) SecretName() string {
	if c.Spec.SecretName != nil {
		return *c.Spec.SecretName
	}
	return c.Name
}

func (c *Connection) SecretKey() string {
	if c.Spec.SecretTemplate.Key != nil {
		return *c.Spec.SecretTemplate.Key
	}
	return defaultConnSecretKey
}

//+kubebuilder:object:root=true

// ConnectionList contains a list of Connection
type ConnectionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Connection `json:"items"`
}

// ListItems gets a copy of the Items slice.
func (m *ConnectionList) ListItems() []client.Object {
	items := make([]client.Object, len(m.Items))
	for i, item := range m.Items {
		items[i] = item.DeepCopy()
	}
	return items
}
