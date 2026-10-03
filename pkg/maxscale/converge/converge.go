// Package converge implements the multi-replica barrier used by generation-scoped MaxScale
// configuration changes: configuration objects are written once and propagated via config sync,
// so every replica must expose the same graph before entry points move.
package converge

import (
	"context"
	"fmt"
	"time"

	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/graph"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Objects lists the live objects every MaxScale Pod must expose before a stage advances.
type Objects struct {
	Servers          []string
	Filters          []string
	Monitors         []string
	Services         []string
	Listeners        []string
	MonitorRelations []string
	ServiceRelations []string
	RunningListeners []string
}

// PodAPI is the per-Pod read boundary required by the barrier.
type PodAPI interface {
	ConfigSyncVersion(ctx context.Context) (int, error)

	ServerMatches(ctx context.Context, desired graph.Server) (bool, error)
	FilterMatches(ctx context.Context, desired graph.Filter) (bool, error)
	MonitorMatches(ctx context.Context, desired graph.Monitor) (bool, error)
	ServiceMatches(ctx context.Context, desired graph.Service) (bool, error)
	ListenerMatches(ctx context.Context, desired graph.Listener) (bool, error)
	ListenerRunning(ctx context.Context, desired graph.Listener) (bool, error)
	MonitorServers(ctx context.Context, name string) ([]string, error)
	ServiceRelationships(ctx context.Context, name string) (servers, filters []string, err error)
}

// Checker verifies that every Pod converges to the expected graph.
type Checker struct {
	desired         *graph.Graph
	ha              bool
	syncInterval    time.Duration
	databaseVersion func(ctx context.Context) (int, error)
}

// NewChecker builds the barrier. In HA the backend config-sync version gates progress;
// single replicas skip version checks and converge against the local Pod only.
func NewChecker(desired *graph.Graph, ha bool, syncInterval time.Duration,
	databaseVersion func(ctx context.Context) (int, error)) *Checker {
	return &Checker{
		desired:         desired,
		ha:              ha,
		syncInterval:    syncInterval,
		databaseVersion: databaseVersion,
	}
}

// Converged reports whether every Pod observes the expected objects. A missing object or version
// lag is a retriable "not converged yet" answer; transport errors are returned to the caller.
func (c *Checker) Converged(ctx context.Context, pods map[string]PodAPI, objs Objects) (bool, error) {
	dbVersion := 0
	if c.ha {
		var err error
		dbVersion, err = c.databaseVersion(ctx)
		if err != nil {
			return false, err
		}
	}

	for podName, api := range pods {
		if c.ha {
			version, err := api.ConfigSyncVersion(ctx)
			if err != nil {
				return false, fmt.Errorf("error getting config sync version in Pod '%s': %v", podName, err)
			}
			if version != dbVersion {
				log.FromContext(ctx).V(1).Info("Config sync lag detected",
					"pod", podName, "pod-version", version, "database-version", dbVersion)
				return false, nil
			}
		}
		ok, err := c.podConverged(ctx, api, objs)
		if err != nil {
			return false, fmt.Errorf("error checking convergence in Pod '%s': %v", podName, err)
		}
		if !ok {
			log.FromContext(ctx).V(1).Info("Objects not yet converged in Pod", "pod", podName)
			return false, nil
		}
	}
	return true, nil
}
