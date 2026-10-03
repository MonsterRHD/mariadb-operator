// Package maxscaleconfigchange runs a generation-scoped, recoverable transaction that moves
// MaxScale servers, filters, monitor, services and listeners from one dependency graph to the next:
// validate, prepare without disrupting the old graph, converge on every replica, switch entry
// points, converge again, and only then delete objects the MaxScale CR owns.
package maxscaleconfigchange

import (
	"context"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/converge"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/graph"
)

// REST is the per-Pod MaxScale REST boundary. Configuration mutations are expected to be issued
// against one Pod and propagated via config sync; runtime state operations must hit every Pod.
type REST interface {
	converge.PodAPI

	ObserveLive(ctx context.Context) (graph.Live, error)

	CreateServer(ctx context.Context, srv graph.Server) error
	PatchServer(ctx context.Context, srv graph.Server) error
	SetServerMaintenance(ctx context.Context, srv graph.Server) error
	DeleteServer(ctx context.Context, name string) error

	CreateFilter(ctx context.Context, filter graph.Filter) error
	PatchFilter(ctx context.Context, filter graph.Filter) error
	DeleteFilter(ctx context.Context, name string) error

	CreateMonitorStopped(ctx context.Context, monitor graph.Monitor) error
	PatchMonitorParams(ctx context.Context, monitor graph.Monitor) error
	PatchMonitorRelations(ctx context.Context, monitor graph.Monitor) error
	StartMonitor(ctx context.Context, name string) error
	StopMonitor(ctx context.Context, name string) error
	DeleteMonitor(ctx context.Context, name string) error

	CreateService(ctx context.Context, svc graph.Service) error
	PatchServiceParams(ctx context.Context, svc graph.Service) error
	PatchServiceRelations(ctx context.Context, svc graph.Service) error
	StartService(ctx context.Context, name string) error
	StopService(ctx context.Context, name string) error
	DeleteService(ctx context.Context, name string) error

	CreateListener(ctx context.Context, listener graph.Listener, serviceName string) error
	PatchListener(ctx context.Context, listener graph.Listener, serviceName string) error
	StartListener(ctx context.Context, name string) error
	StopListener(ctx context.Context, name string) error
	DeleteListener(ctx context.Context, name string) error
}

// Status persists the recoverable change in the MaxScale CR status.
type Status interface {
	PatchConfigChange(ctx context.Context,
		patcher func(*mariadbv1alpha1.MaxScaleConfigChangeStatus) error) error
}

// References validates the dynamic references the desired graph needs (passwords, TLS bundles).
type References interface {
	Validate(ctx context.Context, mxs *mariadbv1alpha1.MaxScale) error
}
