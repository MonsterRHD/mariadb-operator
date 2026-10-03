package converge

import (
	"context"
	"fmt"
	"time"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/graph"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func newTestDesiredGraph() *graph.Graph {
	return graph.DesiredFromSpec(&mariadbv1alpha1.MaxScale{
		Spec: mariadbv1alpha1.MaxScaleSpec{
			Replicas: 2,
			Servers: []mariadbv1alpha1.MaxScaleServer{
				{Name: "mdb-0", Address: "mdb-0.mdb-internal.default.svc.cluster.local", Port: 3306, Protocol: "MariaDBBackend"},
				{Name: "mdb-1", Address: "mdb-1.mdb-internal.default.svc.cluster.local", Port: 3306, Protocol: "MariaDBBackend"},
			},
			Monitor: mariadbv1alpha1.MaxScaleMonitor{
				Name:     "mdb-monitor",
				Module:   mariadbv1alpha1.MonitorModuleMariadb,
				Interval: metav1.Duration{Duration: 2 * time.Second},
			},
			Services: []mariadbv1alpha1.MaxScaleService{
				{
					Name:   "rw-router",
					Router: mariadbv1alpha1.ServiceRouterReadWriteSplit,
					Listener: mariadbv1alpha1.MaxScaleListener{
						Name: "rw-listener", Port: 3306, Protocol: "MariaDBProtocol",
					},
				},
			},
		},
	})
}

type fakePodAPI struct {
	version        int
	err            error
	matched        map[string]bool
	running        map[string]bool
	monitorServers map[string][]string
	serviceRels    map[string]serviceRel
}

type serviceRel struct {
	servers []string
	filters []string
}

func newFakePodAPI() *fakePodAPI {
	return &fakePodAPI{
		matched:        map[string]bool{},
		running:        map[string]bool{},
		monitorServers: map[string][]string{},
		serviceRels:    map[string]serviceRel{},
	}
}

func (f *fakePodAPI) key(kind, id string) string {
	return fmt.Sprintf("%s/%s", kind, id)
}

func (f *fakePodAPI) ConfigSyncVersion(context.Context) (int, error) { return f.version, f.err }
func (f *fakePodAPI) ServerMatches(_ context.Context, d graph.Server) (bool, error) {
	return f.matched[f.key("servers", d.Name)], f.err
}
func (f *fakePodAPI) FilterMatches(_ context.Context, d graph.Filter) (bool, error) {
	return f.matched[f.key("filters", d.Name)], f.err
}
func (f *fakePodAPI) MonitorMatches(_ context.Context, d graph.Monitor) (bool, error) {
	return f.matched[f.key("monitors", d.Name)], f.err
}
func (f *fakePodAPI) ServiceMatches(_ context.Context, d graph.Service) (bool, error) {
	return f.matched[f.key("services", d.Name)], f.err
}
func (f *fakePodAPI) ListenerMatches(_ context.Context, d graph.Listener) (bool, error) {
	return f.matched[f.key("listeners", d.Name)], f.err
}
func (f *fakePodAPI) ListenerRunning(_ context.Context, d graph.Listener) (bool, error) {
	return f.running[d.Name], f.err
}
func (f *fakePodAPI) MonitorServers(_ context.Context, name string) ([]string, error) {
	return f.monitorServers[name], f.err
}
func (f *fakePodAPI) ServiceRelationships(_ context.Context, name string) ([]string, []string, error) {
	rels := f.serviceRels[name]
	return rels.servers, rels.filters, f.err
}

var _ PodAPI = &fakePodAPI{}

func newChecker(ha bool) (*Checker, *graph.Graph) {
	desired := newTestDesiredGraph()
	checker := NewChecker(desired, ha, time.Second, func(context.Context) (int, error) {
		return 5, nil
	})
	return checker, desired
}

func markConverged(f *fakePodAPI, g *graph.Graph) {
	for _, id := range g.ObjectIDs().Servers {
		f.matched[f.key("servers", id)] = true
	}
	for _, id := range g.ObjectIDs().Monitor {
		f.matched[f.key("monitors", id)] = true
		f.monitorServers[id] = g.Monitors[id].Servers
	}
	for _, id := range g.ObjectIDs().Services {
		f.matched[f.key("services", id)] = true
		svc := g.Services[id]
		f.serviceRels[id] = serviceRel{servers: svc.Servers, filters: svc.Filters}
	}
	for _, id := range g.ObjectIDs().Listeners {
		f.matched[f.key("listeners", id)] = true
		f.running[id] = true
	}
}

func newTestObjs(g *graph.Graph) Objects {
	return Objects{
		Servers:          g.ObjectIDs().Servers,
		Monitors:         g.ObjectIDs().Monitor,
		Services:         g.ObjectIDs().Services,
		Listeners:        g.ObjectIDs().Listeners,
		MonitorRelations: g.ObjectIDs().Monitor,
		ServiceRelations: g.ObjectIDs().Services,
		RunningListeners: g.ObjectIDs().Listeners,
	}
}

var _ = Describe("MaxScale config convergence barrier", func() {
	It("waits when an HA Pod lags the database config sync version", func() {
		checker, desired := newChecker(true)
		pod0 := newFakePodAPI()
		pod0.version = 5
		pod1 := newFakePodAPI()
		pod1.version = 4
		converged, err := checker.Converged(context.Background(),
			map[string]PodAPI{"mxs-0": pod0, "mxs-1": pod1}, newTestObjs(desired))
		Expect(err).ToNot(HaveOccurred())
		Expect(converged).To(BeFalse())
	})

	It("returns errors when a Pod is unreachable", func() {
		checker, desired := newChecker(true)
		pod0 := newFakePodAPI()
		pod0.version = 5
		pod1 := newFakePodAPI()
		pod1.version = 5
		pod1.err = fmt.Errorf("connection refused")
		markConverged(pod0, desired)
		markConverged(pod1, desired)
		converged, err := checker.Converged(context.Background(),
			map[string]PodAPI{"mxs-0": pod0, "mxs-1": pod1}, newTestObjs(desired))
		Expect(err).To(HaveOccurred())
		Expect(converged).To(BeFalse())
	})

	It("waits when an expected object is missing on a Pod", func() {
		checker, desired := newChecker(false)
		pod := newFakePodAPI()
		pod.matched["servers/mdb-0"] = true
		pod.matched["monitors/mdb-monitor"] = true
		pod.matched["services/rw-router"] = true
		// rw-listener intentionally absent and unmatched.
		converged, err := checker.Converged(context.Background(),
			map[string]PodAPI{"mxs-0": pod}, newTestObjs(desired))
		Expect(err).ToNot(HaveOccurred())
		Expect(converged).To(BeFalse())
	})

	It("passes when every Pod converges, including entry-point state and relationships", func() {
		checker, desired := newChecker(true)
		pod0 := newFakePodAPI()
		pod0.version = 5
		pod1 := newFakePodAPI()
		pod1.version = 5
		markConverged(pod0, desired)
		markConverged(pod1, desired)
		converged, err := checker.Converged(context.Background(),
			map[string]PodAPI{"mxs-0": pod0, "mxs-1": pod1}, newTestObjs(desired))
		Expect(err).ToNot(HaveOccurred())
		Expect(converged).To(BeTrue())
	})
})
