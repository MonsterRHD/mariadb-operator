package controller

import (
	"context"

	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/graph"
)

func (a *mxsConfigAPI) ObserveLive(ctx context.Context) (graph.Live, error) {
	return a.observeLive(ctx)
}

func (a *mxsConfigAPI) ConfigSyncVersion(ctx context.Context) (int, error) {
	return a.configSyncVersion(ctx)
}

func (a *mxsConfigAPI) CreateServer(ctx context.Context, value graph.Server) error {
	return a.createServer(ctx, value)
}

func (a *mxsConfigAPI) PatchServer(ctx context.Context, value graph.Server) error {
	return a.patchServer(ctx, value)
}

func (a *mxsConfigAPI) SetServerMaintenance(ctx context.Context, value graph.Server) error {
	return a.setServerMaintenance(ctx, value)
}

func (a *mxsConfigAPI) DeleteServer(ctx context.Context, name string) error {
	return a.deleteServer(ctx, name)
}

func (a *mxsConfigAPI) CreateFilter(ctx context.Context, value graph.Filter) error {
	return a.createFilter(ctx, value)
}

func (a *mxsConfigAPI) PatchFilter(ctx context.Context, value graph.Filter) error {
	return a.patchFilter(ctx, value)
}

func (a *mxsConfigAPI) DeleteFilter(ctx context.Context, name string) error {
	return a.deleteFilter(ctx, name)
}

func (a *mxsConfigAPI) CreateMonitorStopped(ctx context.Context, value graph.Monitor) error {
	return a.createMonitorStopped(ctx, value)
}

func (a *mxsConfigAPI) PatchMonitorParams(ctx context.Context, value graph.Monitor) error {
	return a.patchMonitorParams(ctx, value)
}

func (a *mxsConfigAPI) PatchMonitorRelations(ctx context.Context, value graph.Monitor) error {
	return a.patchMonitorRelations(ctx, value)
}

func (a *mxsConfigAPI) StartMonitor(ctx context.Context, name string) error {
	return a.startMonitor(ctx, name)
}

func (a *mxsConfigAPI) StopMonitor(ctx context.Context, name string) error {
	return a.stopMonitor(ctx, name)
}

func (a *mxsConfigAPI) DeleteMonitor(ctx context.Context, name string) error {
	return a.deleteMonitor(ctx, name)
}

func (a *mxsConfigAPI) CreateService(ctx context.Context, value graph.Service) error {
	return a.createService(ctx, value)
}

func (a *mxsConfigAPI) PatchServiceParams(ctx context.Context, value graph.Service) error {
	return a.patchServiceParams(ctx, value)
}

func (a *mxsConfigAPI) PatchServiceRelations(ctx context.Context, value graph.Service) error {
	return a.patchServiceRelations(ctx, value)
}

func (a *mxsConfigAPI) StartService(ctx context.Context, name string) error {
	return a.startService(ctx, name)
}

func (a *mxsConfigAPI) StopService(ctx context.Context, name string) error {
	return a.stopService(ctx, name)
}

func (a *mxsConfigAPI) DeleteService(ctx context.Context, name string) error {
	return a.deleteService(ctx, name)
}

func (a *mxsConfigAPI) CreateListener(ctx context.Context, value graph.Listener, serviceName string) error {
	return a.createListener(ctx, value, serviceName)
}

func (a *mxsConfigAPI) PatchListener(ctx context.Context, value graph.Listener, serviceName string) error {
	return a.patchListener(ctx, value, serviceName)
}

func (a *mxsConfigAPI) StartListener(ctx context.Context, name string) error {
	return a.startListener(ctx, name)
}

func (a *mxsConfigAPI) StopListener(ctx context.Context, name string) error {
	return a.stopListener(ctx, name)
}

func (a *mxsConfigAPI) DeleteListener(ctx context.Context, name string) error {
	return a.deleteListener(ctx, name)
}

func (a *mxsConfigAPI) ServerMatches(ctx context.Context, value graph.Server) (bool, error) {
	return a.serverMatches(ctx, value)
}

func (a *mxsConfigAPI) FilterMatches(ctx context.Context, value graph.Filter) (bool, error) {
	return a.filterMatches(ctx, value)
}

func (a *mxsConfigAPI) MonitorMatches(ctx context.Context, value graph.Monitor) (bool, error) {
	return a.monitorMatches(ctx, value)
}

func (a *mxsConfigAPI) ServiceMatches(ctx context.Context, value graph.Service) (bool, error) {
	return a.serviceMatches(ctx, value)
}

func (a *mxsConfigAPI) ListenerMatches(ctx context.Context, value graph.Listener) (bool, error) {
	return a.listenerMatches(ctx, value)
}

func (a *mxsConfigAPI) ListenerRunning(ctx context.Context, value graph.Listener) (bool, error) {
	return a.listenerRunning(ctx, value)
}

func (a *mxsConfigAPI) MonitorServers(ctx context.Context, name string) ([]string, error) {
	return a.monitorServers(ctx, name)
}

func (a *mxsConfigAPI) ServiceRelationships(ctx context.Context, name string) ([]string, []string, error) {
	return a.serviceRelationships(ctx, name)
}
