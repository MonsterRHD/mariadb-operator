package graph

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func newTestMaxScale() *mariadbv1alpha1.MaxScale {
	return &mariadbv1alpha1.MaxScale{
		Spec: mariadbv1alpha1.MaxScaleSpec{
			Replicas: 2,
			Servers: []mariadbv1alpha1.MaxScaleServer{
				{
					Name:     "mdb-0",
					Address:  "mdb-0.mdb-internal.default.svc.cluster.local",
					Port:     3306,
					Protocol: "MariaDBBackend",
				},
				{
					Name:     "mdb-1",
					Address:  "mdb-1.mdb-internal.default.svc.cluster.local",
					Port:     3306,
					Protocol: "MariaDBBackend",
				},
			},
			Monitor: mariadbv1alpha1.MaxScaleMonitor{
				Name:     "mdb-monitor",
				Module:   mariadbv1alpha1.MonitorModuleMariadb,
				Interval: metav1.Duration{Duration: 2_000_000_000},
			},
			Services: []mariadbv1alpha1.MaxScaleService{
				{
					Name:   "rw-router",
					Router: mariadbv1alpha1.ServiceRouterReadWriteSplit,
					Listener: mariadbv1alpha1.MaxScaleListener{
						Name:     "rw-listener",
						Port:     3306,
						Protocol: "MariaDBProtocol",
					},
				},
			},
		},
	}
}

func newDesiredGraph() *Graph {
	return DesiredFromSpec(newTestMaxScale())
}

var _ = Describe("MaxScale configuration graph", func() {
	DescribeTable(
		"validation",
		func(mutate func(*Graph), expectedErr string) {
			g := newDesiredGraph()
			mutate(g)
			err := g.Validate()
			if expectedErr == "" {
				Expect(err).ToNot(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expectedErr))
			}
		},
		Entry("valid graph", func(*Graph) {}, ""),
		Entry("service references an undeclared filter", func(g *Graph) {
			g.Filters["real-filter"] = Filter{Name: "real-filter", Module: "regexfilter"}
			svc := g.Services["rw-router"]
			svc.Filters = []string{"missing-filter"}
			g.Services["rw-router"] = svc
		}, "references filter 'missing-filter'"),
		Entry("monitor references an undeclared server", func(g *Graph) {
			delete(g.Servers, "mdb-1")
		}, "references server 'mdb-1'"),
		Entry("duplicated listener port", func(g *Graph) {
			g.Services["rr-router"] = Service{
				Name:    "rr-router",
				Router:  mariadbv1alpha1.ServiceRouterReadConnRoute,
				Servers: []string{"mdb-0", "mdb-1"},
				Listener: Listener{
					Name:     "rr-listener",
					Port:     3306,
					Protocol: "MariaDBProtocol",
				},
			}
			g.Listeners["rr-listener"] = g.Services["rr-router"].Listener
		}, "share the same port 3306"),
		Entry("duplicated server address", func(g *Graph) {
			srv := g.Servers["mdb-1"]
			srv.Address = g.Servers["mdb-0"].Address
			g.Servers["mdb-1"] = srv
		}, "share the same address"),
		Entry("invalid monitor module", func(g *Graph) {
			mon := g.Monitors["mdb-monitor"]
			mon.Module = "bogusmon"
			g.Monitors["mdb-monitor"] = mon
		}, "monitor 'mdb-monitor' module invalid"),
	)

	It("hashes a stable whole-graph fingerprint unaffected by credential references", func() {
		g1 := newDesiredGraph()
		h1, err := g1.Hash()
		Expect(err).ToNot(HaveOccurred())

		mxs := newTestMaxScale()
		mxs.Spec.Auth.MonitorPasswordSecretKeyRef.Name = "changed-secret"
		mxs.Spec.Auth.ServerPasswordSecretKeyRef.Name = "other-secret"
		g2 := DesiredFromSpec(mxs)
		h2, err := g2.Hash()
		Expect(err).ToNot(HaveOccurred())
		Expect(h2).To(Equal(h1))

		h1Again, err := DesiredFromSpec(newTestMaxScale()).Hash()
		Expect(err).ToNot(HaveOccurred())
		Expect(h1Again).To(Equal(h1))
	})

	It("adopts desired objects present live and never adopts foreign objects, including port squatters", func() {
		desired := newDesiredGraph()

		// Rename the desired listener while a foreign holder keeps the same port.
		newListener := desired.Listeners["rw-listener"]
		newListener.Name = "blocking-listener"
		desired.Listeners["blocking-listener"] = newListener
		delete(desired.Listeners, "rw-listener")
		svc := desired.Services["rw-router"]
		delete(desired.Services, "rw-router")
		svc.Listener = newListener
		desired.Services["rw-router"] = svc

		live := LiveFromObjectIDs(desired.ObjectIDs())
		live.Servers.Add("foreign-server")
		live.Services.Add("foreign-service")
		live.Listeners.Add("foreign-listener")
		live.ListenerPorts[3306] = "port-squatter"
		live.Listeners.Add("port-squatter")

		adopted := Adopt(desired, live)
		Expect(adopted.Servers).To(ConsistOf("mdb-0", "mdb-1"))
		Expect(adopted.Services).To(ConsistOf("rw-router"))
		// The squatter is intentionally NOT adopted: the first post-upgrade change blocks instead
		// of deleting an object the CR has never owned.
		Expect(adopted.Listeners).To(ConsistOf("blocking-listener"))
		Expect(adopted.Monitor).To(ConsistOf("mdb-monitor"))
	})

	It("classifies creates, parameter patches and ledger-scoped deletes", func() {
		desired := newDesiredGraph()
		desired.Servers["mdb-2"] = Server{
			Name:     "mdb-2",
			Address:  "mdb-2.mdb-internal.default.svc.cluster.local",
			Port:     3306,
			Protocol: "MariaDBBackend",
		}
		delete(desired.Servers, "mdb-1")
		mon := desired.Monitors["mdb-monitor"]
		mon.Servers = []string{"mdb-0", "mdb-2"}
		desired.Monitors["mdb-monitor"] = mon

		managed := &mariadbv1alpha1.MaxScaleConfigObjectIDs{
			Servers:   []string{"mdb-0", "mdb-1"},
			Monitor:   []string{"mdb-monitor"},
			Services:  []string{"rw-router"},
			Listeners: []string{"rw-listener"},
		}
		live := LiveFromObjectIDs(managed)
		live.Servers.Add("foreign-server")
		live.MonitorServers["mdb-monitor"] = []string{"mdb-0", "mdb-1"}

		plan := BuildPlan(desired, managed, live)
		Expect(plan.Creates.Servers).To(ConsistOf("mdb-2"))
		Expect(plan.Patches.Servers).To(ContainElement("mdb-0"))
		Expect(plan.Deletes.Servers).To(ConsistOf("mdb-1"))
		Expect(plan.Deletes.Servers).ToNot(ContainElement("foreign-server"))
		Expect(plan.MonitorRelationsSwitch).To(BeTrue())
	})

	It("supersedes a stale plan by merging its prepared debris into the managed ledger", func() {
		managed := &mariadbv1alpha1.MaxScaleConfigObjectIDs{
			Servers: []string{"mdb-0"},
		}
		stalePrepared := &mariadbv1alpha1.MaxScaleConfigObjectIDs{
			Servers:   []string{"stale-server"},
			Listeners: []string{"stale-listener"},
			Monitor:   []string{"new-monitor"},
		}

		merged := Supersede(managed, stalePrepared)
		Expect(merged.Servers).To(ConsistOf("mdb-0", "stale-server"))
		Expect(merged.Listeners).To(ConsistOf("stale-listener"))
		Expect(merged.Monitor).To(ConsistOf("new-monitor"))

		desired := newDesiredGraph()
		live := LiveFromObjectIDs(merged)
		plan := BuildPlan(desired, merged, live)
		Expect(plan.Deletes.Servers).To(ConsistOf("stale-server"))
		Expect(plan.Deletes.Listeners).To(ConsistOf("stale-listener"))
		Expect(plan.Deletes.Monitor).To(ConsistOf("new-monitor"))
	})

	It("recomputes deletions when a newer generation re-declares a stale-plan deletion", func() {
		desired := newDesiredGraph() // still declares mdb-0 and mdb-1
		managed := &mariadbv1alpha1.MaxScaleConfigObjectIDs{
			Servers:   []string{"mdb-0", "mdb-1"},
			Monitor:   []string{"mdb-monitor"},
			Services:  []string{"rw-router"},
			Listeners: []string{"rw-listener"},
		}
		live := LiveFromObjectIDs(managed)
		plan := BuildPlan(desired, managed, live)
		Expect(plan.Deletes.Servers).To(BeEmpty())
	})

	It("classifies a monitor rename as create plus rename, not as a parameter patch", func() {
		desired := newDesiredGraph()
		mon := desired.Monitors["mdb-monitor"]
		delete(desired.Monitors, "mdb-monitor")
		mon.Name = "renamed-monitor"
		desired.Monitors["renamed-monitor"] = mon

		managed := &mariadbv1alpha1.MaxScaleConfigObjectIDs{
			Servers:   []string{"mdb-0", "mdb-1"},
			Monitor:   []string{"mdb-monitor"},
			Services:  []string{"rw-router"},
			Listeners: []string{"rw-listener"},
		}
		live := LiveFromObjectIDs(managed)
		plan := BuildPlan(desired, managed, live)

		Expect(plan.MonitorCreate).To(BeTrue())
		Expect(plan.MonitorRename).To(BeTrue())
		Expect(plan.Creates.Monitor).To(ConsistOf("renamed-monitor"))
		Expect(plan.Patches.Monitor).To(BeEmpty())
		Expect(plan.Deletes.Monitor).To(ConsistOf("mdb-monitor"))
	})

	It("defers a same-port listener rename to the switch stage", func() {
		desired := newDesiredGraph()
		svc := desired.Services["rw-router"]
		delete(desired.Services, "rw-router")
		delete(desired.Listeners, "rw-listener")
		svc.Name = "split-router"
		svc.Listener.Name = "split-listener"
		svc.Listener.Port = 3306
		desired.Services["split-router"] = svc
		desired.Listeners["split-listener"] = svc.Listener

		managed := &mariadbv1alpha1.MaxScaleConfigObjectIDs{
			Servers:   []string{"mdb-0", "mdb-1"},
			Monitor:   []string{"mdb-monitor"},
			Services:  []string{"rw-router"},
			Listeners: []string{"rw-listener"},
		}
		live := LiveFromObjectIDs(managed)
		live.ListenerPorts[3306] = "rw-listener"
		live.ServiceServers["rw-router"] = []string{"mdb-0", "mdb-1"}

		plan := BuildPlan(desired, managed, live)
		Expect(plan.ConflictingListeners).To(ConsistOf("split-listener"))
		Expect(plan.ConflictingListenerHolders).To(HaveKeyWithValue("split-listener", "rw-listener"))
		Expect(plan.ForeignPortHolders).To(BeEmpty())
		Expect(plan.Creates.Listeners).ToNot(ContainElement("split-listener"))
		Expect(plan.Deletes.Listeners).To(ConsistOf("rw-listener"))
	})

	It("blocks when a non-managed live listener holds the new listener port", func() {
		desired := newDesiredGraph()
		svc := desired.Services["rw-router"]
		delete(desired.Services, "rw-router")
		delete(desired.Listeners, "rw-listener")
		svc.Name = "split-router"
		svc.Listener.Name = "split-listener"
		desired.Services["split-router"] = svc
		desired.Listeners["split-listener"] = svc.Listener

		// The squatter is live but neither declared by the spec nor present in the ownership ledger.
		live := LiveFromObjectIDs(desired.ObjectIDs())
		live.Listeners.Add("rw-listener")
		live.ListenerPorts[3306] = "rw-listener"

		plan := BuildPlan(desired, &mariadbv1alpha1.MaxScaleConfigObjectIDs{}, live)
		Expect(plan.ConflictingListeners).To(BeEmpty())
		Expect(plan.ForeignPortHolders).To(HaveKeyWithValue("split-listener", "rw-listener"))
	})

	It("switches existing service relationships only when the live relationships differ", func() {
		desired := newDesiredGraph()
		managed := &mariadbv1alpha1.MaxScaleConfigObjectIDs{
			Servers:   []string{"mdb-0", "mdb-1"},
			Monitor:   []string{"mdb-monitor"},
			Services:  []string{"rw-router"},
			Listeners: []string{"rw-listener"},
		}
		live := LiveFromObjectIDs(managed)
		live.ServiceServers["rw-router"] = []string{"mdb-0", "mdb-1"}

		Expect(BuildPlan(desired, managed, live).ServiceRelationsSwitch).To(BeEmpty())

		live.ServiceServers["rw-router"] = []string{"mdb-0"}
		Expect(BuildPlan(desired, managed, live).ServiceRelationsSwitch).To(ConsistOf("rw-router"))
	})
})
