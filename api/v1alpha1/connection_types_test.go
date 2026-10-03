package v1alpha1

import (
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/metadata"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/statefulset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

var _ = Describe("Connection types", func() {
	newConnection := func() *Connection {
		return &Connection{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "conn",
				Namespace: "test",
			},
			Spec: ConnectionSpec{
				Username: "test",
				MariaDBRef: &MariaDBRef{
					ObjectReference: ObjectReference{
						Name: "mariadb",
					},
				},
			},
		}
	}
	newMariaDB := func(ha bool, port int32) *MariaDB {
		mdb := &MariaDB{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mariadb",
				Namespace: "test",
			},
			Spec: MariaDBSpec{
				Port: port,
			},
		}
		if ha {
			mdb.Spec.Replication = &Replication{
				Enabled: true,
			}
		}
		return mdb
	}
	mariaDBFQDN := func(name string) string {
		return statefulset.ServiceFQDN(metav1.ObjectMeta{
			Name:      name,
			Namespace: "test",
		})
	}

	Describe("SetDefaults", func() {
		It("should default Secret name and template key without persisting host and port", func() {
			conn := newConnection()
			conn.SetDefaults()

			Expect(ptr.Deref(conn.Spec.SecretName, "")).To(Equal("conn"))
			Expect(conn.Spec.SecretTemplate).ToNot(BeNil())
			Expect(ptr.Deref(conn.Spec.SecretTemplate.Key, "")).To(Equal("dsn"))
			Expect(conn.Spec.Host).To(BeEmpty())
			Expect(conn.Spec.Port).To(BeZero())
		})

		It("should preserve explicit Secret name and template, defaulting a missing key", func() {
			conn := newConnection()
			conn.Spec.SecretName = ptr.To("custom-secret")
			conn.Spec.SecretTemplate = &SecretTemplate{
				Format: ptr.To("mysql://{{ .Host }}"),
			}
			conn.SetDefaults()

			Expect(ptr.Deref(conn.Spec.SecretName, "")).To(Equal("custom-secret"))
			Expect(ptr.Deref(conn.Spec.SecretTemplate.Key, "")).To(Equal("dsn"))
			Expect(ptr.Deref(conn.Spec.SecretTemplate.Format, "")).To(Equal("mysql://{{ .Host }}"))
		})
	})

	Describe("EffectiveHost", func() {
		DescribeTable(
			"should resolve explicit and derived hosts",
			func(refs *ConnectionRefs, specHost string, specServiceName *string, wantHost string) {
				conn := newConnection()
				conn.Spec.Host = specHost
				conn.Spec.ServiceName = specServiceName

				host, err := conn.EffectiveHost(refs)
				Expect(err).ToNot(HaveOccurred())
				Expect(host).To(Equal(wantHost))
			},
			Entry("MariaDB derived host",
				&ConnectionRefs{MariaDB: newMariaDB(false, 3306)},
				"", nil, mariaDBFQDN("mariadb"),
			),
			Entry("MariaDB HA derived host points to the primary Service",
				&ConnectionRefs{MariaDB: newMariaDB(true, 3306)},
				"", nil, mariaDBFQDN("mariadb-primary"),
			),
			Entry("explicit host is not replaced by a default",
				&ConnectionRefs{MariaDB: newMariaDB(false, 3306)},
				"db.example.com", nil, "db.example.com",
			),
			Entry("explicit serviceName is honored",
				&ConnectionRefs{MariaDB: newMariaDB(false, 3306)},
				"", ptr.To("mariadb-secondary"), mariaDBFQDN("mariadb-secondary"),
			),
			Entry("MaxScale derived host",
				&ConnectionRefs{MaxScale: &MaxScale{
					ObjectMeta: metav1.ObjectMeta{Name: "maxscale", Namespace: "test"},
				}},
				"", nil, mariaDBFQDN("maxscale"),
			),
			Entry("ExternalMariaDB derived host",
				&ConnectionRefs{ExternalMariaDB: &ExternalMariaDB{
					ObjectMeta: metav1.ObjectMeta{Name: "external", Namespace: "test"},
					Spec: ExternalMariaDBSpec{
						Host: "external.example.com",
						Port: 3306,
					},
				}},
				"", nil, "external.example.com",
			),
		)
	})

	Describe("EffectivePort", func() {
		DescribeTable(
			"should resolve explicit and derived ports",
			func(refs *ConnectionRefs, specPort, wantPort int32) {
				conn := newConnection()
				conn.Spec.Port = specPort

				port, err := conn.EffectivePort(refs)
				Expect(err).ToNot(HaveOccurred())
				Expect(port).To(Equal(wantPort))
			},
			Entry("MariaDB derived port", &ConnectionRefs{MariaDB: newMariaDB(false, 3306)}, int32(0), int32(3306)),
			Entry("MariaDB explicit port", &ConnectionRefs{MariaDB: newMariaDB(false, 3306)}, int32(3307), int32(3307)),
			Entry("ExternalMariaDB derived port", &ConnectionRefs{ExternalMariaDB: &ExternalMariaDB{
				Spec: ExternalMariaDBSpec{Host: "external.example.com", Port: 3307},
			}}, int32(0), int32(3307)),
			Entry("MaxScale derived port", &ConnectionRefs{MaxScale: &MaxScale{
				Spec: MaxScaleSpec{
					Services: []MaxScaleService{
						{
							Name:     "rw",
							Router:   ServiceRouterReadWriteSplit,
							Listener: MaxScaleListener{Port: 5000},
						},
					},
				},
			}}, int32(0), int32(5000)),
		)
	})

	Describe("MigrateDerivedFields", func() {
		It("should clear host and port persisted by older operator versions", func() {
			conn := newConnection()
			conn.Spec.Host = mariaDBFQDN("mariadb")
			conn.Spec.Port = 3306
			refs := &ConnectionRefs{MariaDB: newMariaDB(false, 3306)}

			changed, err := conn.MigrateDerivedFields(refs)
			Expect(err).ToNot(HaveOccurred())
			Expect(changed).To(BeTrue())
			Expect(conn.Spec.Host).To(BeEmpty())
			Expect(conn.Spec.Port).To(BeZero())
			Expect(conn.Annotations).To(HaveKey(metadata.ConnectionDerivedFieldsAnnotation))
		})

		It("should clear a legacy HA primary Service host", func() {
			conn := newConnection()
			conn.Spec.Host = mariaDBFQDN("mariadb-primary")
			conn.Spec.Port = 3306
			refs := &ConnectionRefs{MariaDB: newMariaDB(false, 3306)}

			changed, err := conn.MigrateDerivedFields(refs)
			Expect(err).ToNot(HaveOccurred())
			Expect(changed).To(BeTrue())
			Expect(conn.Spec.Host).To(BeEmpty())
		})

		It("should preserve explicitly configured host and port", func() {
			conn := newConnection()
			conn.Spec.Host = "db.example.com"
			conn.Spec.Port = 3307
			refs := &ConnectionRefs{MariaDB: newMariaDB(false, 3306)}

			changed, err := conn.MigrateDerivedFields(refs)
			Expect(err).ToNot(HaveOccurred())
			Expect(changed).To(BeTrue())
			Expect(conn.Spec.Host).To(Equal("db.example.com"))
			Expect(conn.Spec.Port).To(Equal(int32(3307)))
		})

		It("should run once", func() {
			conn := newConnection()
			conn.Spec.Host = mariaDBFQDN("mariadb")
			conn.Spec.Port = 3306
			refs := &ConnectionRefs{MariaDB: newMariaDB(false, 3306)}

			_, err := conn.MigrateDerivedFields(refs)
			Expect(err).ToNot(HaveOccurred())

			// Simulate the parent port changing after migration.
			refs.MariaDB.Spec.Port = 3307
			changed, err := conn.MigrateDerivedFields(refs)
			Expect(err).ToNot(HaveOccurred())
			Expect(changed).To(BeFalse())
			Expect(conn.Spec.Host).To(BeEmpty())
			Expect(conn.Spec.Port).To(BeZero())
		})
	})
})
