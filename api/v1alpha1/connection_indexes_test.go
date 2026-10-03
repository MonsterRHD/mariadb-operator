package v1alpha1

import (
	"context"
	"time"

	"github.com/mariadb-operator/mariadb-operator/v26/pkg/metadata"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("Connection index watches", Ordered, func() {
	var (
		mgrCancel context.CancelFunc
		requests  chan ctrl.Request
		cl        client.Client
	)

	BeforeAll(func() {
		scheme := clientgoscheme.Scheme
		Expect(AddToScheme(scheme)).To(Succeed())

		var err error
		cl, err = client.New(testEnv.Config, client.Options{Scheme: scheme})
		Expect(err).ToNot(HaveOccurred())

		mgr, err := ctrl.NewManager(testEnv.Config, ctrl.Options{
			Scheme:         scheme,
			LeaderElection: false,
			Metrics:        metricsserver.Options{BindAddress: "0"},
		})
		Expect(err).ToNot(HaveOccurred())

		builder := ctrl.NewControllerManagedBy(mgr).
			For(&Connection{}).
			Owns(&corev1.Secret{})
		Expect(IndexConnection(ctx, mgr, builder, mgr.GetClient())).To(Succeed())

		requests = make(chan ctrl.Request, 64)
		Expect(builder.Complete(reconcile.Func(func(_ context.Context, req ctrl.Request) (ctrl.Result, error) {
			requests <- req
			return ctrl.Result{}, nil
		}))).To(Succeed())

		var mgrCtx context.Context
		mgrCtx, mgrCancel = context.WithCancel(ctx)
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()

		Eventually(func() bool {
			return mgr.GetCache().WaitForCacheSync(mgrCtx)
		}, "10s", "100ms").Should(BeTrue())
	})

	AfterAll(func() {
		if mgrCancel != nil {
			mgrCancel()
		}
	})

	// drainRequests drains enqueued requests until no event arrives during the quiet period, so that
	// asynchronously delivered create events do not leak into subsequent assertions.
	drainRequests := func() {
		const quietFor = 100 * time.Millisecond
		for {
			select {
			case <-requests:
			case <-time.After(quietFor):
				return
			}
		}
	}

	It("re-enqueues the Connection when the referenced object spec changes", func() {
		emdbKey := types.NamespacedName{Name: "conn-watch-emdb", Namespace: testNamespace}
		connKey := types.NamespacedName{Name: "conn-watch-conn", Namespace: testNamespace}

		emdb := &ExternalMariaDB{
			ObjectMeta: metav1.ObjectMeta{
				Name:      emdbKey.Name,
				Namespace: emdbKey.Namespace,
			},
			Spec: ExternalMariaDBSpec{
				Host:     "external.example.com",
				Port:     3306,
				Username: ptr.To("app"),
			},
		}
		Expect(cl.Create(testCtx, emdb)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(cl.Delete(testCtx, emdb))).To(Succeed())
		})

		conn := &Connection{
			ObjectMeta: metav1.ObjectMeta{
				Name:      connKey.Name,
				Namespace: connKey.Namespace,
			},
			Spec: ConnectionSpec{
				Username: "app",
				PasswordSecretKeyRef: &SecretKeySelector{
					LocalObjectReference: LocalObjectReference{Name: "conn-watch-password"},
					Key:                  "password",
				},
				MariaDBRef: &MariaDBRef{
					ObjectReference: ObjectReference{Name: emdbKey.Name},
					Kind:            ExternalMariaDBKind,
				},
			},
		}
		Expect(cl.Create(testCtx, conn)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(cl.Delete(testCtx, conn))).To(Succeed())
		})

		drainRequests()

		By("Updating the referenced ExternalMariaDB port")
		Eventually(func(g Gomega) {
			var updated ExternalMariaDB
			g.Expect(cl.Get(testCtx, emdbKey, &updated)).To(Succeed())
			updated.Spec.Port = 3307
			g.Expect(cl.Update(testCtx, &updated)).To(Succeed())
		}, "10s", "100ms").Should(Succeed())

		Eventually(requests, "10s", "100ms").Should(Receive(Equal(ctrl.Request{NamespacedName: connKey})))
	})

	It("re-enqueues the Connection when a watch-labeled Secret changes and ignores unlabeled ones", func() {
		labeledSecretKey := types.NamespacedName{Name: "conn-watch-labeled-secret", Namespace: testNamespace}
		labeledConnKey := types.NamespacedName{Name: "conn-watch-labeled-conn", Namespace: testNamespace}

		labeledSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      labeledSecretKey.Name,
				Namespace: labeledSecretKey.Namespace,
				Labels:    map[string]string{metadata.WatchLabel: ""},
			},
			StringData: map[string]string{"password": "MariaDB11!"},
		}
		Expect(cl.Create(testCtx, labeledSecret)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(cl.Delete(testCtx, labeledSecret))).To(Succeed())
		})

		labeledConn := &Connection{
			ObjectMeta: metav1.ObjectMeta{
				Name:      labeledConnKey.Name,
				Namespace: labeledConnKey.Namespace,
			},
			Spec: ConnectionSpec{
				Username: "app",
				PasswordSecretKeyRef: &SecretKeySelector{
					LocalObjectReference: LocalObjectReference{Name: labeledSecretKey.Name},
					Key:                  "password",
				},
				MariaDBRef: &MariaDBRef{
					ObjectReference: ObjectReference{Name: "mariadb-webhook"},
				},
			},
		}
		Expect(cl.Create(testCtx, labeledConn)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(cl.Delete(testCtx, labeledConn))).To(Succeed())
		})
		drainRequests()

		By("Updating the watch-labeled Secret")
		Eventually(func(g Gomega) {
			var updated corev1.Secret
			g.Expect(cl.Get(testCtx, labeledSecretKey, &updated)).To(Succeed())
			updated.Data["password"] = []byte("MariaDB-updated11!")
			g.Expect(cl.Update(testCtx, &updated)).To(Succeed())
		}, "10s", "100ms").Should(Succeed())
		Eventually(requests, "10s", "100ms").Should(Receive(Equal(ctrl.Request{NamespacedName: labeledConnKey})))
		drainRequests()

		By("Updating an unlabeled Secret referenced by another Connection")
		unlabeledSecretKey := types.NamespacedName{Name: "conn-watch-unlabeled-secret", Namespace: testNamespace}
		unlabeledConnKey := types.NamespacedName{Name: "conn-watch-unlabeled-conn", Namespace: testNamespace}

		unlabeledSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      unlabeledSecretKey.Name,
				Namespace: unlabeledSecretKey.Namespace,
			},
			StringData: map[string]string{"password": "MariaDB11!"},
		}
		Expect(cl.Create(testCtx, unlabeledSecret)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(cl.Delete(testCtx, unlabeledSecret))).To(Succeed())
		})

		unlabeledConn := &Connection{
			ObjectMeta: metav1.ObjectMeta{
				Name:      unlabeledConnKey.Name,
				Namespace: unlabeledConnKey.Namespace,
			},
			Spec: ConnectionSpec{
				Username: "app",
				PasswordSecretKeyRef: &SecretKeySelector{
					LocalObjectReference: LocalObjectReference{Name: unlabeledSecretKey.Name},
					Key:                  "password",
				},
				MariaDBRef: &MariaDBRef{
					ObjectReference: ObjectReference{Name: "mariadb-webhook"},
				},
			},
		}
		Expect(cl.Create(testCtx, unlabeledConn)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(cl.Delete(testCtx, unlabeledConn))).To(Succeed())
		})
		drainRequests()

		var updated corev1.Secret
		Expect(cl.Get(testCtx, unlabeledSecretKey, &updated)).To(Succeed())
		updated.Data["password"] = []byte("MariaDB-updated11!")
		Expect(cl.Update(testCtx, &updated)).To(Succeed())

		Consistently(requests, time.Second, "100ms").ShouldNot(Receive())
	})
})
