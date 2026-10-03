package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	mxsclient "github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/client"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/graph"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/refresolver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// configAPI is the REST boundary used by the generation-scoped configuration transaction.
// Every implementation targets a single MaxScale Pod: configuration writes are expected to be
// issued against the healthy Pod and propagated via config sync, while state operations
// (start/stop) are executed on every Pod because config sync does not propagate runtime state.
type configAPI interface {
	observeLive(ctx context.Context) (graph.Live, error)
	configSyncVersion(ctx context.Context) (int, error)

	createServer(ctx context.Context, srv graph.Server) error
	patchServer(ctx context.Context, srv graph.Server) error
	setServerMaintenance(ctx context.Context, srv graph.Server) error
	deleteServer(ctx context.Context, name string) error

	createFilter(ctx context.Context, filter graph.Filter) error
	patchFilter(ctx context.Context, filter graph.Filter) error
	deleteFilter(ctx context.Context, name string) error

	createMonitorStopped(ctx context.Context, monitor graph.Monitor) error
	patchMonitorParams(ctx context.Context, monitor graph.Monitor) error
	patchMonitorRelations(ctx context.Context, monitor graph.Monitor) error
	startMonitor(ctx context.Context, name string) error
	stopMonitor(ctx context.Context, name string) error
	deleteMonitor(ctx context.Context, name string) error

	createService(ctx context.Context, svc graph.Service) error
	patchServiceParams(ctx context.Context, svc graph.Service) error
	patchServiceRelations(ctx context.Context, svc graph.Service) error
	startService(ctx context.Context, name string) error
	stopService(ctx context.Context, name string) error
	deleteService(ctx context.Context, name string) error

	createListener(ctx context.Context, listener graph.Listener, serviceName string) error
	patchListener(ctx context.Context, listener graph.Listener, serviceName string) error
	startListener(ctx context.Context, name string) error
	stopListener(ctx context.Context, name string) error
	deleteListener(ctx context.Context, name string) error

	// matches reports whether the live object converges to the desired node:
	// existence must be checked by the caller, which also collects relationships and state.
	serverMatches(ctx context.Context, desired graph.Server) (bool, error)
	filterMatches(ctx context.Context, desired graph.Filter) (bool, error)
	monitorMatches(ctx context.Context, desired graph.Monitor) (bool, error)
	serviceMatches(ctx context.Context, desired graph.Service) (bool, error)
	listenerMatches(ctx context.Context, desired graph.Listener) (bool, error)
	// listenerRunning reports whether the listener runtime state matches the desired suspend intent.
	listenerRunning(ctx context.Context, desired graph.Listener) (bool, error)
	monitorServers(ctx context.Context, name string) ([]string, error)
	serviceRelationships(ctx context.Context, name string) (servers, filters []string, err error)
}

type mxsConfigAPI struct {
	mxs    *mariadbv1alpha1.MaxScale
	client *mxsclient.Client
	api    *maxScaleAPI
}

func newMxsConfigAPI(mxs *mariadbv1alpha1.MaxScale, client *mxsclient.Client,
	refResolver *refresolver.RefResolver) *mxsConfigAPI {
	return &mxsConfigAPI{
		mxs:    mxs,
		client: client,
		api:    newMaxScaleAPI(mxs, client, refResolver),
	}
}

func (a *mxsConfigAPI) observeLive(ctx context.Context) (graph.Live, error) {
	live := graph.NewLive()

	serverIdx, err := a.client.Server.ListIndex(ctx)
	if err != nil {
		return live, fmt.Errorf("error listing servers: %v", err)
	}
	for id := range serverIdx {
		live.Servers.Add(id)
	}

	filterIdx, err := a.client.Filter.ListIndex(ctx)
	if err != nil {
		return live, fmt.Errorf("error listing filters: %v", err)
	}
	for id := range filterIdx {
		live.Filters.Add(id)
	}

	monitorIdx, err := a.client.Monitor.ListIndex(ctx)
	if err != nil {
		return live, fmt.Errorf("error listing monitors: %v", err)
	}
	for id, mon := range monitorIdx {
		live.Monitors.Add(id)
		live.MonitorServers[id] = relationshipIDs(mon.Relationships, mxsclient.ObjectTypeServers)
	}

	serviceIdx, err := a.client.Service.ListIndex(ctx)
	if err != nil {
		return live, fmt.Errorf("error listing services: %v", err)
	}
	for id, svc := range serviceIdx {
		live.Services.Add(id)
		live.ServiceServers[id] = relationshipIDs(svc.Relationships, mxsclient.ObjectTypeServers)
		live.ServiceFilters[id] = filterRelationshipIDs(svc.Relationships)
	}

	listenerIdx, err := a.client.Listener.ListIndex(ctx)
	if err != nil {
		return live, fmt.Errorf("error listing listeners: %v", err)
	}
	for id, listener := range listenerIdx {
		live.Listeners.Add(id)
		if listener.Attributes.Parameters.Port > 0 {
			live.ListenerPorts[listener.Attributes.Parameters.Port] = id
		}
	}

	return live, nil
}

func (a *mxsConfigAPI) configSyncVersion(ctx context.Context) (int, error) {
	status, err := a.client.MaxScale.Get(ctx)
	if err != nil {
		return 0, err
	}
	if status.Attributes.ConfigSync == nil {
		return 0, nil
	}
	return status.Attributes.ConfigSync.Version, nil
}

// Servers

func (a *mxsConfigAPI) createServer(ctx context.Context, srv graph.Server) error {
	spec := serverToSpec(srv)
	if err := a.api.createServer(ctx, &spec); err != nil {
		if mxsclient.IsConflict(err) {
			return a.api.patchServer(ctx, &spec)
		}
		return err
	}
	return nil
}

func (a *mxsConfigAPI) patchServer(ctx context.Context, srv graph.Server) error {
	spec := serverToSpec(srv)
	return a.api.patchServer(ctx, &spec)
}

func (a *mxsConfigAPI) setServerMaintenance(ctx context.Context, srv graph.Server) error {
	spec := serverToSpec(srv)
	return a.api.updateServerState(ctx, &spec)
}

func (a *mxsConfigAPI) deleteServer(ctx context.Context, name string) error {
	if err := a.api.deleteServer(ctx, name); err != nil && !mxsclient.IsNotFound(err) {
		return err
	}
	return nil
}

// Filters

func (a *mxsConfigAPI) createFilter(ctx context.Context, filter graph.Filter) error {
	spec := filterToSpec(filter)
	if err := a.api.createFilter(ctx, &spec); err != nil {
		if mxsclient.IsConflict(err) {
			return a.api.patchFilter(ctx, &spec)
		}
		return err
	}
	return nil
}

func (a *mxsConfigAPI) patchFilter(ctx context.Context, filter graph.Filter) error {
	spec := filterToSpec(filter)
	return a.api.patchFilter(ctx, &spec)
}

func (a *mxsConfigAPI) deleteFilter(ctx context.Context, name string) error {
	if err := a.api.deleteFilter(ctx, name); err != nil && !mxsclient.IsNotFound(err) {
		return err
	}
	return nil
}

// Monitors

func (a *mxsConfigAPI) createMonitorStopped(ctx context.Context, monitor graph.Monitor) error {
	spec := monitorToSpec(monitor)
	attrs, err := a.api.monitorAttributes(ctx)
	if err != nil {
		return err
	}
	// A monitor created concurrently between planning and execution must not receive the new
	// relationships during preparation; the switch stage patches them before the monitor starts.
	if err := a.client.Monitor.Create(ctx, spec.Name, attrs); err != nil {
		if !mxsclient.IsConflict(err) {
			return err
		}
		if err := a.client.Monitor.Patch(ctx, spec.Name, attrs); err != nil {
			return err
		}
	}
	return a.client.Monitor.Stop(ctx, spec.Name)
}

func (a *mxsConfigAPI) patchMonitorParams(ctx context.Context, monitor graph.Monitor) error {
	spec := monitorToSpec(monitor)
	attrs, err := a.api.monitorAttributes(ctx)
	if err != nil {
		return err
	}
	// Relationships are intentionally omitted so live topology stays untouched until the switch stage.
	return a.client.Monitor.Patch(ctx, spec.Name, attrs)
}

func (a *mxsConfigAPI) patchMonitorRelations(ctx context.Context, monitor graph.Monitor) error {
	rels := monitorRelationships(monitor)
	spec := monitorToSpec(monitor)
	attrs, err := a.api.monitorAttributes(ctx)
	if err != nil {
		return err
	}
	return a.client.Monitor.Patch(ctx, spec.Name, attrs, mxsclient.WithRelationships(rels))
}

func (a *mxsConfigAPI) startMonitor(ctx context.Context, name string) error {
	return a.client.Monitor.Start(ctx, name)
}

func (a *mxsConfigAPI) stopMonitor(ctx context.Context, name string) error {
	return a.client.Monitor.Stop(ctx, name)
}

func (a *mxsConfigAPI) deleteMonitor(ctx context.Context, name string) error {
	if err := a.client.Monitor.Delete(ctx, name, mxsclient.WithForceQuery()); err != nil &&
		!mxsclient.IsNotFound(err) {
		return err
	}
	return nil
}

// Services

func (a *mxsConfigAPI) createService(ctx context.Context, svc graph.Service) error {
	spec := serviceToSpec(svc)
	rels := serviceRelationships(svc)
	if err := a.api.createService(ctx, &spec, rels); err != nil {
		if mxsclient.IsConflict(err) {
			return a.api.patchService(ctx, &spec, rels)
		}
		return err
	}
	return nil
}

func (a *mxsConfigAPI) patchServiceParams(ctx context.Context, svc graph.Service) error {
	spec := serviceToSpec(svc)
	// Relationships are intentionally omitted so routing keeps using the previous servers until switch.
	attrs, err := a.api.serviceAttributes(ctx, &spec)
	if err != nil {
		return err
	}
	return a.client.Service.Patch(ctx, spec.Name, attrs)
}

func (a *mxsConfigAPI) patchServiceRelations(ctx context.Context, svc graph.Service) error {
	spec := serviceToSpec(svc)
	return a.api.patchService(ctx, &spec, serviceRelationships(svc))
}

func (a *mxsConfigAPI) startService(ctx context.Context, name string) error {
	return a.client.Service.Start(ctx, name)
}

func (a *mxsConfigAPI) stopService(ctx context.Context, name string) error {
	return a.client.Service.Stop(ctx, name)
}

func (a *mxsConfigAPI) deleteService(ctx context.Context, name string) error {
	if err := a.api.deleteService(ctx, name); err != nil && !mxsclient.IsNotFound(err) {
		return err
	}
	return nil
}

// Listeners

func (a *mxsConfigAPI) createListener(ctx context.Context, listener graph.Listener, serviceName string) error {
	spec := listenerToSpec(listener)
	rels := a.api.serviceRelationships(serviceName)
	if err := a.api.createListener(ctx, &spec, rels); err != nil {
		if mxsclient.IsConflict(err) {
			return a.api.patchListener(ctx, &spec, rels)
		}
		return err
	}
	return nil
}

func (a *mxsConfigAPI) patchListener(ctx context.Context, listener graph.Listener, serviceName string) error {
	spec := listenerToSpec(listener)
	return a.api.patchListener(ctx, &spec, a.api.serviceRelationships(serviceName))
}

func (a *mxsConfigAPI) startListener(ctx context.Context, name string) error {
	return a.client.Listener.Start(ctx, name)
}

func (a *mxsConfigAPI) stopListener(ctx context.Context, name string) error {
	return a.client.Listener.Stop(ctx, name)
}

func (a *mxsConfigAPI) deleteListener(ctx context.Context, name string) error {
	if err := a.api.deleteListener(ctx, name); err != nil && !mxsclient.IsNotFound(err) {
		return err
	}
	return nil
}

func (a *mxsConfigAPI) monitorServers(ctx context.Context, name string) ([]string, error) {
	mon, err := a.client.Monitor.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return relationshipIDs(mon.Relationships, mxsclient.ObjectTypeServers), nil
}

func (a *mxsConfigAPI) serviceRelationships(ctx context.Context, name string) ([]string, []string, error) {
	svc, err := a.client.Service.Get(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	return relationshipIDs(svc.Relationships, mxsclient.ObjectTypeServers),
		filterRelationshipIDs(svc.Relationships), nil
}

// Convergence comparison

func rawAttributes(data map[string]any) map[string]any {
	attrs, _ := data["attributes"].(map[string]any)
	return nonNilMap(attrs)
}

func rawParameters(attrs map[string]any) map[string]any {
	params, _ := attrs["parameters"].(map[string]any)
	return nonNilMap(params)
}

func (a *mxsConfigAPI) serverMatches(ctx context.Context, desired graph.Server) (bool, error) {
	data, err := a.client.Server.GetRawData(ctx, desired.Name)
	if err != nil {
		return false, err
	}
	attrs := rawAttributes(data)
	expect := map[string]any{
		"address":  desired.Address,
		"port":     desired.Port,
		"protocol": desired.Protocol,
	}
	if desired.TLS.Enabled {
		expect["ssl"] = true
		expect["ssl_version"] = "TLSv13"
		expect["ssl_verify_peer_certificate"] = desired.TLS.VerifyPeerCertificate
		expect["ssl_verify_peer_host"] = desired.TLS.VerifyPeerHost
	}
	return rawMapContains(attrs, expect) && rawParamsContain(rawParameters(attrs), desired.Params), nil
}

func (a *mxsConfigAPI) filterMatches(ctx context.Context, desired graph.Filter) (bool, error) {
	data, err := a.client.Filter.GetRawData(ctx, desired.Name)
	if err != nil {
		return false, err
	}
	attrs := rawAttributes(data)
	return rawEqual(attrs["module"], desired.Module) && rawParamsContain(rawParameters(attrs), desired.Params), nil
}

func (a *mxsConfigAPI) monitorMatches(ctx context.Context, desired graph.Monitor) (bool, error) {
	data, err := a.client.Monitor.GetRawData(ctx, desired.Name)
	if err != nil {
		return false, err
	}
	attrs := rawAttributes(data)
	expect := map[string]any{
		"module": string(desired.Module),
	}
	if desired.IntervalMillis > 0 {
		expect["monitor_interval"] = desired.IntervalMillis
	}
	if desired.CooperativeMonitoring != nil {
		expect["cooperative_monitoring_locks"] = string(*desired.CooperativeMonitoring)
	}
	return rawMapContains(attrs, expect) && rawParamsContain(rawParameters(attrs), desired.Params), nil
}

func (a *mxsConfigAPI) serviceMatches(ctx context.Context, desired graph.Service) (bool, error) {
	data, err := a.client.Service.GetRawData(ctx, desired.Name)
	if err != nil {
		return false, err
	}
	attrs := rawAttributes(data)
	return rawEqual(attrs["router"], string(desired.Router)) &&
		rawParamsContain(rawParameters(attrs), desired.Params), nil
}

func (a *mxsConfigAPI) listenerMatches(ctx context.Context, desired graph.Listener) (bool, error) {
	data, err := a.client.Listener.GetRawData(ctx, desired.Name)
	if err != nil {
		return false, err
	}
	attrs := rawAttributes(data)
	expect := map[string]any{
		"port":     desired.Port,
		"protocol": desired.Protocol,
	}
	if desired.TLS {
		expect["ssl"] = true
		expect["ssl_version"] = "TLSv13"
		expect["ssl_verify_peer_certificate"] = a.mxs.ShouldVerifyPeerCertificate()
		expect["ssl_verify_peer_host"] = a.mxs.ShouldVerifyPeerHost()
	}
	return rawMapContains(attrs, expect) && rawParamsContain(rawParameters(attrs), desired.Params), nil
}

func (a *mxsConfigAPI) listenerRunning(ctx context.Context, desired graph.Listener) (bool, error) {
	data, err := a.client.Listener.GetRawData(ctx, desired.Name)
	if err != nil {
		return false, err
	}
	state, _ := rawAttributes(data)["state"].(string)
	return stateMatchesSuspend(state, desired.Suspend), nil
}

// graph -> spec conversions

func serverToSpec(srv graph.Server) mariadbv1alpha1.MaxScaleServer {
	return mariadbv1alpha1.MaxScaleServer{
		Name:        srv.Name,
		Address:     srv.Address,
		Port:        srv.Port,
		Protocol:    srv.Protocol,
		Maintenance: srv.Maintenance,
		Params:      srv.Params,
	}
}

func filterToSpec(filter graph.Filter) mariadbv1alpha1.MaxScaleFilter {
	return mariadbv1alpha1.MaxScaleFilter{
		Name:   filter.Name,
		Module: filter.Module,
		Params: filter.Params,
	}
}

func monitorToSpec(monitor graph.Monitor) mariadbv1alpha1.MaxScaleMonitor {
	spec := mariadbv1alpha1.MaxScaleMonitor{
		Name:    monitor.Name,
		Module:  monitor.Module,
		Suspend: monitor.Suspend,
		Params:  monitor.Params,
	}
	if monitor.IntervalMillis > 0 {
		spec.Interval = metav1.Duration{Duration: time.Duration(monitor.IntervalMillis) * time.Millisecond}
	}
	if monitor.CooperativeMonitoring != nil {
		spec.CooperativeMonitoring = monitor.CooperativeMonitoring
	}
	return spec
}

func serviceToSpec(svc graph.Service) mariadbv1alpha1.MaxScaleService {
	return mariadbv1alpha1.MaxScaleService{
		Name:     svc.Name,
		Router:   svc.Router,
		Suspend:  svc.Suspend,
		Params:   svc.Params,
		Filters:  svc.Filters,
		Listener: listenerToSpec(svc.Listener),
	}
}

func listenerToSpec(listener graph.Listener) mariadbv1alpha1.MaxScaleListener {
	return mariadbv1alpha1.MaxScaleListener{
		Name:     listener.Name,
		Port:     listener.Port,
		Protocol: listener.Protocol,
		Suspend:  listener.Suspend,
		Params:   listener.Params,
	}
}

// relationships

func monitorRelationships(monitor graph.Monitor) *mxsclient.Relationships {
	ids := graph.SortedCopy(monitor.Servers)
	return mxsclient.NewRelationshipsBuilder().WithServers(ids...).Build()
}

func serviceRelationships(svc graph.Service) *mxsclient.Relationships {
	servers := graph.SortedCopy(svc.Servers)
	rels := mxsclient.NewRelationshipsBuilder().WithServers(servers...).WithFilters(svc.Filters...).Build()
	return rels
}

func relationshipIDs(rels *mxsclient.Relationships, objType mxsclient.ObjectType) []string {
	if rels == nil {
		return nil
	}
	var data []mxsclient.RelationshipItem
	switch objType {
	case mxsclient.ObjectTypeServers:
		if rels.Servers != nil {
			data = rels.Servers.Data
		}
	case mxsclient.ObjectTypeServices:
		if rels.Services != nil {
			data = rels.Services.Data
		}
	}
	ids := make([]string, 0, len(data))
	for _, item := range data {
		ids = append(ids, item.ID)
	}
	sort.Strings(ids)
	return ids
}

func filterRelationshipIDs(rels *mxsclient.Relationships) []string {
	if rels == nil || rels.Filters == nil {
		return nil
	}
	ids := make([]string, 0, len(rels.Filters.Data))
	for _, item := range rels.Filters.Data {
		ids = append(ids, item.ID)
	}
	sort.Strings(ids)
	return ids
}

// raw comparison helpers

func rawMapContains(actual map[string]any, expected map[string]any) bool {
	for key, expectedValue := range expected {
		if !rawEqual(actual[key], expectedValue) {
			return false
		}
	}
	return true
}

func rawParamsContain(actual map[string]any, expected map[string]string) bool {
	for key, value := range expected {
		expectedRaw, err := json.Marshal(mxsclient.Param(value))
		if err != nil {
			return false
		}
		actualRaw, err := json.Marshal(actual[key])
		if err != nil || string(actualRaw) != string(expectedRaw) {
			return false
		}
	}
	return true
}

func rawEqual(actual any, expected any) bool {
	actualRaw, err := json.Marshal(actual)
	if err != nil {
		return false
	}
	expectedRaw, err := json.Marshal(expected)
	if err != nil {
		return false
	}
	return string(actualRaw) == string(expectedRaw)
}

func nonNilMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// stateMatchesSuspend tolerates MaxScale runtime representations of started and stopped states.
func stateMatchesSuspend(state string, suspended bool) bool {
	if suspended {
		return state == "Stopped"
	}
	return state != "Stopped"
}
