package maxscaleconfigchange

import (
	"context"
	"fmt"
	"sync"
	"time"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/graph"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type fakeREST struct {
	mu sync.Mutex

	live  graph.Live
	calls []string

	matched map[string]bool
	running map[string]bool

	fail map[string]error
}

func newFakeREST(initial graph.Live) *fakeREST {
	return &fakeREST{
		live:    initial,
		matched: map[string]bool{},
		running: map[string]bool{},
		fail:    map[string]error{},
	}
}

func (f *fakeREST) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *fakeREST) callsList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	copied := make([]string, len(f.calls))
	copy(copied, f.calls)
	return copied
}

func (f *fakeREST) ObserveLive(context.Context) (graph.Live, error) {
	f.record("ObserveLive")
	return f.live, nil
}

func (f *fakeREST) ConfigSyncVersion(context.Context) (int, error) {
	return 5, nil
}

func (f *fakeREST) failFor(kind, id string) error {
	return f.fail[kind+":"+id]
}

func (f *fakeREST) CreateServer(_ context.Context, srv graph.Server) error {
	f.record("CreateServer:" + srv.Name)
	if err := f.failFor("servers", srv.Name); err != nil {
		return err
	}
	f.live.Servers.Add(srv.Name)
	f.matched["servers/"+srv.Name] = true
	return nil
}

func (f *fakeREST) PatchServer(_ context.Context, srv graph.Server) error {
	f.record("PatchServer:" + srv.Name)
	if err := f.failFor("servers", srv.Name); err != nil {
		return err
	}
	f.matched["servers/"+srv.Name] = true
	return nil
}

func (f *fakeREST) SetServerMaintenance(_ context.Context, srv graph.Server) error {
	f.record("SetServerMaintenance:" + srv.Name)
	return nil
}

func (f *fakeREST) DeleteServer(_ context.Context, name string) error {
	f.record("DeleteServer:" + name)
	if !f.live.Servers.Contains(name) {
		return nil
	}
	delete(f.live.Servers, name)
	return nil
}

func (f *fakeREST) CreateFilter(_ context.Context, filter graph.Filter) error {
	f.record("CreateFilter:" + filter.Name)
	f.live.Filters.Add(filter.Name)
	f.matched["filters/"+filter.Name] = true
	return nil
}

func (f *fakeREST) PatchFilter(_ context.Context, filter graph.Filter) error {
	f.record("PatchFilter:" + filter.Name)
	f.matched["filters/"+filter.Name] = true
	return nil
}

func (f *fakeREST) DeleteFilter(_ context.Context, name string) error {
	f.record("DeleteFilter:" + name)
	f.live.Filters.Remove(name)
	return nil
}

func (f *fakeREST) CreateMonitorStopped(_ context.Context, monitor graph.Monitor) error {
	f.record("CreateMonitorStopped:" + monitor.Name)
	f.live.Monitors.Add(monitor.Name)
	f.live.MonitorServers[monitor.Name] = append([]string(nil), monitor.Servers...)
	f.matched["monitors/"+monitor.Name] = true
	return nil
}

func (f *fakeREST) PatchMonitorParams(_ context.Context, monitor graph.Monitor) error {
	f.record("PatchMonitorParams:" + monitor.Name)
	f.matched["monitors/"+monitor.Name] = true
	return nil
}

func (f *fakeREST) PatchMonitorRelations(_ context.Context, monitor graph.Monitor) error {
	f.record("PatchMonitorRelations:" + monitor.Name)
	f.live.MonitorServers[monitor.Name] = append([]string(nil), monitor.Servers...)
	f.matched["monitors/"+monitor.Name] = true
	return nil
}

func (f *fakeREST) StartMonitor(_ context.Context, name string) error {
	f.record("StartMonitor:" + name)
	return nil
}

func (f *fakeREST) StopMonitor(_ context.Context, name string) error {
	f.record("StopMonitor:" + name)
	return nil
}

func (f *fakeREST) DeleteMonitor(_ context.Context, name string) error {
	f.record("DeleteMonitor:" + name)
	return nil
}

func (f *fakeREST) CreateService(_ context.Context, svc graph.Service) error {
	f.record("CreateService:" + svc.Name)
	f.live.Services.Add(svc.Name)
	f.live.ServiceServers[svc.Name] = append([]string(nil), svc.Servers...)
	f.live.ServiceFilters[svc.Name] = append([]string(nil), svc.Filters...)
	f.matched["services/"+svc.Name] = true
	return nil
}

func (f *fakeREST) PatchServiceParams(_ context.Context, svc graph.Service) error {
	f.record("PatchServiceParams:" + svc.Name)
	f.matched["services/"+svc.Name] = true
	return nil
}

func (f *fakeREST) PatchServiceRelations(_ context.Context, svc graph.Service) error {
	f.record("PatchServiceRelations:" + svc.Name)
	f.live.ServiceServers[svc.Name] = append([]string(nil), svc.Servers...)
	f.live.ServiceFilters[svc.Name] = append([]string(nil), svc.Filters...)
	f.matched["services/"+svc.Name] = true
	return nil
}

func (f *fakeREST) StartService(context.Context, string) error { return nil }
func (f *fakeREST) StopService(context.Context, string) error  { return nil }

func (f *fakeREST) DeleteService(_ context.Context, name string) error {
	f.record("DeleteService:" + name)
	return nil
}

func (f *fakeREST) CreateListener(_ context.Context, listener graph.Listener, serviceName string) error {
	f.record("CreateListener:" + listener.Name)
	f.live.Listeners.Add(listener.Name)
	f.live.ListenerPorts[listener.Port] = listener.Name
	f.matched["listeners/"+listener.Name] = true
	f.running[listener.Name] = !listener.Suspend
	return nil
}

func (f *fakeREST) PatchListener(_ context.Context, listener graph.Listener, _ string) error {
	f.record("PatchListener:" + listener.Name)
	f.matched["listeners/"+listener.Name] = true
	return nil
}

func (f *fakeREST) StartListener(_ context.Context, name string) error {
	f.record("StartListener:" + name)
	f.running[name] = true
	return nil
}

func (f *fakeREST) StopListener(_ context.Context, name string) error {
	f.record("StopListener:" + name)
	f.running[name] = false
	return nil
}

func (f *fakeREST) DeleteListener(_ context.Context, name string) error {
	f.record("DeleteListener:" + name)
	return nil
}

func (f *fakeREST) ServerMatches(_ context.Context, desired graph.Server) (bool, error) {
	return f.matched["servers/"+desired.Name], nil
}
func (f *fakeREST) FilterMatches(_ context.Context, desired graph.Filter) (bool, error) {
	return f.matched["filters/"+desired.Name], nil
}
func (f *fakeREST) MonitorMatches(_ context.Context, desired graph.Monitor) (bool, error) {
	return f.matched["monitors/"+desired.Name], nil
}
func (f *fakeREST) ServiceMatches(_ context.Context, desired graph.Service) (bool, error) {
	return f.matched["services/"+desired.Name], nil
}
func (f *fakeREST) ListenerMatches(_ context.Context, desired graph.Listener) (bool, error) {
	return f.matched["listeners/"+desired.Name], nil
}
func (f *fakeREST) ListenerRunning(_ context.Context, desired graph.Listener) (bool, error) {
	return f.running[desired.Name], nil
}
func (f *fakeREST) MonitorServers(_ context.Context, name string) ([]string, error) {
	return f.live.MonitorServers[name], nil
}
func (f *fakeREST) ServiceRelationships(_ context.Context, name string) ([]string, []string, error) {
	return f.live.ServiceServers[name], f.live.ServiceFilters[name], nil
}

var _ REST = &fakeREST{}

type fakeStatus struct {
	change *mariadbv1alpha1.MaxScaleConfigChangeStatus
	patchs int
}

func (s *fakeStatus) PatchConfigChange(_ context.Context,
	patcher func(*mariadbv1alpha1.MaxScaleConfigChangeStatus) error) error {
	s.patchs++
	if s.change == nil {
		s.change = &mariadbv1alpha1.MaxScaleConfigChangeStatus{}
	}
	return patcher(s.change)
}

type fakeEvents struct {
	events []string
}

func (e *fakeEvents) Eventf(_ runtime.Object, _ runtime.Object, eventtype, reason, _, note string, _ ...interface{}) {
	e.events = append(e.events, eventtype+":"+reason)
}

func testMaxScale(generation int64) *mariadbv1alpha1.MaxScale {
	return &mariadbv1alpha1.MaxScale{
		ObjectMeta: metav1.ObjectMeta{Name: "mxs", Namespace: "default", Generation: generation},
		Spec: mariadbv1alpha1.MaxScaleSpec{
			Replicas: 2,
			Servers: []mariadbv1alpha1.MaxScaleServer{
				{Name: "mdb-0", Address: "mdb-0", Port: 3306, Protocol: "MariaDBBackend"},
				{Name: "mdb-1", Address: "mdb-1", Port: 3306, Protocol: "MariaDBBackend"},
				{Name: "mdb-2", Address: "mdb-2", Port: 3306, Protocol: "MariaDBBackend"},
			},
			Filters: []mariadbv1alpha1.MaxScaleFilter{{Name: "my-filter", Module: "regexfilter"}},
			Monitor: mariadbv1alpha1.MaxScaleMonitor{
				Name: "mdb-monitor", Module: mariadbv1alpha1.MonitorModuleMariadb,
				Interval: metav1.Duration{Duration: 2 * time.Second},
			},
			Services: []mariadbv1alpha1.MaxScaleService{
				{
					Name: "rw-router-v2", Router: mariadbv1alpha1.ServiceRouterReadWriteSplit,
					Listener: mariadbv1alpha1.MaxScaleListener{Name: "rw-listener-v2", Port: 3306, Protocol: "MariaDBProtocol"},
					Filters:  []string{"my-filter"},
				},
			},
		},
	}
}

func oldLive() graph.Live {
	live := graph.LiveFromObjectIDs(&mariadbv1alpha1.MaxScaleConfigObjectIDs{
		Servers:   []string{"mdb-0", "mdb-1", "foreign-server"},
		Monitor:   []string{"mdb-monitor"},
		Services:  []string{"rw-router", "foreign-service"},
		Listeners: []string{"rw-listener", "foreign-listener"},
	})
	live.ListenerPorts[3306] = "rw-listener"
	live.MonitorServers["mdb-monitor"] = []string{"mdb-0", "mdb-1"}
	live.ServiceServers["rw-router"] = []string{"mdb-0", "mdb-1"}
	live.ServiceFilters["rw-router"] = nil
	return live
}

func oldManaged() *mariadbv1alpha1.MaxScaleConfigObjectIDs {
	return &mariadbv1alpha1.MaxScaleConfigObjectIDs{
		Servers:   []string{"mdb-0", "mdb-1"},
		Monitor:   []string{"mdb-monitor"},
		Services:  []string{"rw-router"},
		Listeners: []string{"rw-listener"},
	}
}

func testRunner(status Status, events EventRecorder) *Runner {
	return &Runner{
		Status: status,
		Events: events,
		Now:    func() metav1.Time { return metav1.Time{Time: time.Unix(1, 0)} },
		DatabaseVersion: func(context.Context, *mariadbv1alpha1.MaxScale) (int, error) {
			return 5, nil
		},
	}
}

type fakeRefs struct{ err error }

func (f fakeRefs) Validate(context.Context, *mariadbv1alpha1.MaxScale) error { return f.err }

var _ References = fakeRefs{}

var _ = Describe("Configuration change runner", func() {
	It("rejects unresolvable references without any REST mutation", func() {
		ctx := context.Background()
		mxs := testMaxScale(1)
		rest := newFakeREST(oldLive())
		status := &fakeStatus{}
		runner := testRunner(status, &fakeEvents{})
		runner.Refs = fakeRefs{err: fmt.Errorf("monitor password secret missing")}

		_, err := runner.Reconcile(ctx, mxs, rest, map[string]REST{"mxs-0": rest})
		Expect(err).To(HaveOccurred())
		Expect(status.change.Stage).To(Equal(mariadbv1alpha1.ConfigChangeStageValidating))
		for _, call := range rest.callsList() {
			Expect(call).ToNot(HavePrefix("Create"))
			Expect(call).ToNot(HavePrefix("Patch"))
			Expect(call).ToNot(HavePrefix("Delete"))
			Expect(call).ToNot(HavePrefix("Start"))
			Expect(call).ToNot(HavePrefix("Stop"))
			Expect(call).ToNot(HavePrefix("Set"))
		}
	})

	It("prepares alongside the old graph, switches only after convergence, and cleans managed leftovers", func() {
		ctx := context.Background()
		mxs := testMaxScale(2)
		live := oldLive()
		rest := newFakeREST(live)
		// Generation 1 was adopted and committed on the previous graph; generation 2 renames the entry point.
		initialChange := &mariadbv1alpha1.MaxScaleConfigChangeStatus{
			Generation:  1,
			Stage:       mariadbv1alpha1.ConfigChangeStageCompleted,
			DesiredHash: "generation-1-hash",
			Managed:     oldManaged(),
			LastApplied: oldManaged(),
		}
		status := &fakeStatus{change: initialChange}
		mxs.Status.ConfigChange = initialChange
		events := &fakeEvents{}
		runner := testRunner(status, events)

		result, err := runner.Reconcile(ctx, mxs, rest, map[string]REST{"mxs-0": rest, "mxs-1": rest})
		Expect(err).ToNot(HaveOccurred())
		Expect(result.IsZero()).To(BeTrue())
		Expect(status.change.Stage).To(Equal(mariadbv1alpha1.ConfigChangeStageCompleted))

		calls := rest.callsList()

		// No destructive operation happens during preparation.
		firstSwitch := indexCall(calls, "StopListener:rw-listener")
		Expect(firstSwitch).To(BeNumerically(">", 0))
		for _, call := range calls[:firstSwitch] {
			Expect(call).ToNot(HavePrefix("Delete"))
			Expect(call).ToNot(HavePrefix("Stop"))
		}
		// The conflicting new listener is not created while the old one binds the port.
		Expect(indexCall(calls, "CreateListener:rw-listener-v2")).To(BeNumerically(">", firstSwitch))
		Expect(indexCall(calls, "StopListener:rw-listener")).To(BeNumerically("<", indexCall(calls, "DeleteListener:rw-listener")))
		Expect(indexCall(calls, "DeleteListener:rw-listener")).To(BeNumerically("<", indexCall(calls, "CreateListener:rw-listener-v2")))
		Expect(indexCall(calls, "CreateListener:rw-listener-v2")).To(BeNumerically("<", indexCall(calls, "StartListener:rw-listener-v2")))
		// New filter, server and service are prepared before the switch.
		Expect(calls).To(ContainElement("CreateServer:mdb-2"))
		Expect(calls).To(ContainElement("CreateFilter:my-filter"))
		Expect(calls).To(ContainElement("CreateService:rw-router-v2"))
		// Monitor relationships only move at switch, after every prepared object exists.
		Expect(indexCall(calls, "PatchMonitorRelations:mdb-monitor")).To(BeNumerically(">",
			indexCall(calls, "CreateService:rw-router-v2")))
		Expect(indexCall(calls, "PatchMonitorRelations:mdb-monitor")).To(BeNumerically("<",
			indexCall(calls, "StopListener:rw-listener")))
		// Cleanup follows dependency order: listener before service.
		Expect(indexCall(calls, "DeleteListener:rw-listener")).To(BeNumerically("<", indexCall(calls, "DeleteService:rw-router")))
		// Foreign objects are never deleted.
		Expect(calls).ToNot(ContainElement("DeleteServer:foreign-server"))
		Expect(calls).ToNot(ContainElement("DeleteService:foreign-service"))
		Expect(calls).ToNot(ContainElement("DeleteListener:foreign-listener"))

		// Commit records the new graph and drops the old identity.
		Expect(status.change.LastApplied.Services).To(ConsistOf("rw-router-v2"))
		Expect(status.change.LastApplied.Listeners).To(ConsistOf("rw-listener-v2"))
		Expect(status.change.LastApplied.Servers).To(ConsistOf("mdb-0", "mdb-1", "mdb-2"))
		Expect(events.events).To(ContainElement("Normal:" + mariadbv1alpha1.ReasonMaxScaleConfigChangeCompleted))
	})

	It("blocks instead of deleting a foreign live listener that holds a declared port", func() {
		ctx := context.Background()
		mxs := testMaxScale(1)
		// Rename the entry point; a foreign (unmanaged) squatter holds port 3306 in MaxScale.
		svc := mxs.Spec.Services[0]
		mxs.Spec.Services = mxs.Spec.Services[1:]
		svc.Name = "rw-router-v3"
		svc.Listener.Name = "rw-listener-v3"
		mxs.Spec.Services = append(mxs.Spec.Services, svc)

		live := graph.LiveFromObjectIDs(&mariadbv1alpha1.MaxScaleConfigObjectIDs{
			Servers:  []string{"mdb-0", "mdb-1"},
			Monitor:  []string{"mdb-monitor"},
			Services: []string{"rw-router"},
		})
		live.Listeners.Add("foreign-squatter")
		live.ListenerPorts[3306] = "foreign-squatter"
		live.MonitorServers["mdb-monitor"] = []string{"mdb-0", "mdb-1"}
		rest := newFakeREST(live)
		status := &fakeStatus{}
		runner := testRunner(status, &fakeEvents{})

		_, err := runner.Reconcile(ctx, mxs, rest, map[string]REST{"mxs-0": rest})
		Expect(err).To(HaveOccurred())
		for _, call := range rest.callsList() {
			Expect(call).ToNot(HavePrefix("DeleteListener:foreign-squatter"))
			Expect(call).ToNot(HavePrefix("StopListener:foreign-squatter"))
		}
		Expect(status.change.Error.Kind).To(Equal("listeners"))
	})

	It("retains the old graph and records the stage when preparation fails", func() {
		ctx := context.Background()
		mxs := testMaxScale(2)
		rest := newFakeREST(oldLive())
		rest.fail["servers:mdb-2"] = fmt.Errorf("boom")
		initialChange := &mariadbv1alpha1.MaxScaleConfigChangeStatus{
			Generation: 1, Stage: mariadbv1alpha1.ConfigChangeStageCompleted,
			DesiredHash: "g1", Managed: oldManaged(), LastApplied: oldManaged(),
		}
		status := &fakeStatus{change: initialChange}
		mxs.Status.ConfigChange = initialChange
		runner := testRunner(status, &fakeEvents{})

		_, err := runner.Reconcile(ctx, mxs, rest, map[string]REST{"mxs-0": rest})
		Expect(err).To(HaveOccurred())
		Expect(status.change.Stage).To(Equal(mariadbv1alpha1.ConfigChangeStagePreparing))
		Expect(status.change.Error.Stage).To(Equal(mariadbv1alpha1.ConfigChangeStagePreparing))
		Expect(status.change.Error.Kind).To(Equal("servers"))
		Expect(status.change.Error.Object).To(Equal("mdb-2"))

		calls := rest.callsList()
		for _, call := range calls {
			Expect(call).ToNot(HavePrefix("Delete"))
			Expect(call).ToNot(HavePrefix("Stop"))
		}
	})

	It("resumes from checkpoints without recreating prepared objects after a restart", func() {
		ctx := context.Background()
		mxs := testMaxScale(1)
		rest := newFakeREST(oldLive())
		// Simulate objects prepared and checkpointed before the operator restarted.
		rest.live.Servers.Add("mdb-2")
		rest.matched["servers/mdb-2"] = true
		rest.live.Filters.Add("my-filter")
		rest.matched["filters/my-filter"] = true
		rest.live.Services.Add("rw-router-v2")
		rest.matched["services/rw-router-v2"] = true
		rest.live.ServiceServers["rw-router-v2"] = []string{"mdb-0", "mdb-1", "mdb-2"}
		rest.live.ServiceFilters["rw-router-v2"] = []string{"my-filter"}
		rest.live.MonitorServers["mdb-monitor"] = []string{"mdb-0", "mdb-1", "mdb-2"}
		// Mark existing objects converged as well so the barrier advances on this run.
		for _, id := range []string{"mdb-0", "mdb-1"} {
			rest.matched["servers/"+id] = true
		}
		rest.matched["monitors/mdb-monitor"] = true

		change := &mariadbv1alpha1.MaxScaleConfigChangeStatus{
			Generation:  1,
			Stage:       mariadbv1alpha1.ConfigChangeStagePreparing,
			DesiredHash: "resume",
			Managed:     oldManaged(),
			LastApplied: oldManaged(),
			Checkpoints: &mariadbv1alpha1.MaxScaleConfigChangeCheckpoints{
				Prepared: &mariadbv1alpha1.MaxScaleConfigObjectIDs{
					Servers:  []string{"mdb-2"},
					Filters:  []string{"my-filter"},
					Services: []string{"rw-router-v2"},
				},
			},
		}
		status := &fakeStatus{change: change}
		mxs.Status.ConfigChange = change
		runner := testRunner(status, &fakeEvents{})

		_, err := runner.Reconcile(ctx, mxs, rest, map[string]REST{"mxs-0": rest, "mxs-1": rest})
		Expect(err).ToNot(HaveOccurred())
		Expect(status.change.Stage).To(Equal(mariadbv1alpha1.ConfigChangeStageCompleted))
		Expect(rest.callsList()).ToNot(ContainElement("CreateServer:mdb-2"))
		Expect(rest.callsList()).ToNot(ContainElement("CreateFilter:my-filter"))
		Expect(rest.callsList()).ToNot(ContainElement("CreateService:rw-router-v2"))
	})

	It("discards stale-generation plans and folds prepared debris into the new ledger", func() {
		ctx := context.Background()
		mxs := testMaxScale(2)
		// Generation 2 drops mdb-1; generation 1 had prepared a temporary server that is now debris.
		mxs.Spec.Servers = mxs.Spec.Servers[:2]
		mxs.Spec.Servers[1].Name = "mdb-2"
		mxs.Spec.Servers[1].Address = "mdb-2"
		rest := newFakeREST(oldLive())
		rest.live.Servers.Add("temp-server")
		rest.live.Servers.Add("mdb-2")
		change := &mariadbv1alpha1.MaxScaleConfigChangeStatus{
			Generation:  1,
			Stage:       mariadbv1alpha1.ConfigChangeStagePreparing,
			DesiredHash: "g1",
			Managed:     oldManaged(),
			LastApplied: oldManaged(),
			Checkpoints: &mariadbv1alpha1.MaxScaleConfigChangeCheckpoints{
				Prepared: &mariadbv1alpha1.MaxScaleConfigObjectIDs{Servers: []string{"temp-server"}},
			},
		}
		status := &fakeStatus{change: change}
		mxs.Status.ConfigChange = change
		runner := testRunner(status, &fakeEvents{})

		_, err := runner.Reconcile(ctx, mxs, rest, map[string]REST{"mxs-0": rest})
		Expect(err).ToNot(HaveOccurred())

		calls := rest.callsList()
		Expect(calls).To(ContainElement("DeleteServer:mdb-1"))
		Expect(calls).To(ContainElement("DeleteServer:temp-server"))
		Expect(calls).ToNot(ContainElement("DeleteServer:foreign-server"))
		Expect(status.change.Generation).To(BeEquivalentTo(2))
		Expect(status.change.LastApplied.Servers).To(ConsistOf("mdb-0", "mdb-2"))
	})
})

func indexCall(calls []string, call string) int {
	for i, c := range calls {
		if c == call {
			return i
		}
	}
	return -1
}
