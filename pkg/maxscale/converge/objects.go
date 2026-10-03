package converge

import (
	"context"
	"fmt"

	mxsclient "github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/client"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/graph"
)

func (c *Checker) podConverged(ctx context.Context, api PodAPI, objs Objects) (bool, error) {
	if ok, err := allMatch(ctx, objs.Servers, nodeOf(c.desired.Servers), api.ServerMatches, "server"); !ok || err != nil {
		return false, err
	}
	if ok, err := allMatch(ctx, objs.Filters, nodeOf(c.desired.Filters), api.FilterMatches, "filter"); !ok || err != nil {
		return false, err
	}
	if ok, err := allMatch(ctx, objs.Monitors, nodeOf(c.desired.Monitors), api.MonitorMatches, "monitor"); !ok || err != nil {
		return false, err
	}
	if ok, err := allMatch(ctx, objs.Services, nodeOf(c.desired.Services), api.ServiceMatches, "service"); !ok || err != nil {
		return false, err
	}
	if ok, err := allMatch(ctx, objs.Listeners, nodeOf(c.desired.Listeners), api.ListenerMatches, "listener"); !ok || err != nil {
		return false, err
	}

	if ok, err := c.monitorRelationsConverged(ctx, api, objs.MonitorRelations); !ok || err != nil {
		return false, err
	}
	if ok, err := c.serviceRelationsConverged(ctx, api, objs.ServiceRelations); !ok || err != nil {
		return false, err
	}
	return allMatch(ctx, objs.RunningListeners, nodeOf(c.desired.Listeners), api.ListenerRunning, "listener")
}

// nodeOf returns a value lookup for use with allMatch, keeping the call sites compact.
func nodeOf[T any](m map[string]T) func(string) T {
	return func(key string) T {
		return m[key]
	}
}

// allMatch reports whether every desired node matches its live counterpart. A not-found
// counterpart is a retriable "not converged" answer rather than a transport error.
func allMatch[T any](ctx context.Context, ids []string, desired func(string) T,
	match func(context.Context, T) (bool, error), kind string) (bool, error) {
	for _, id := range ids {
		matches, err := match(ctx, desired(id))
		if err != nil {
			return false, errExistingOrObject(kind, id, err)
		}
		if !matches {
			return false, nil
		}
	}
	return true, nil
}

// errExistingOrObject turns a missing object into a "not converged yet" answer and
// propagates any transport error with object context.
func errExistingOrObject(kind, id string, err error) error {
	if mxsclient.IsNotFound(err) {
		return nil
	}
	return errObject(kind, id, err)
}

func errObject(kind, id string, err error) error {
	return fmt.Errorf("error matching %s '%s': %v", kind, id, err)
}

func (c *Checker) monitorRelationsConverged(ctx context.Context, api PodAPI, ids []string) (bool, error) {
	for _, id := range ids {
		servers, err := api.MonitorServers(ctx, id)
		if err != nil {
			return false, errExistingOrObject("monitor", id, err)
		}
		if !graph.SameStrings(servers, c.desired.Monitors[id].Servers) {
			return false, nil
		}
	}
	return true, nil
}

func (c *Checker) serviceRelationsConverged(ctx context.Context, api PodAPI, ids []string) (bool, error) {
	for _, id := range ids {
		servers, filters, err := api.ServiceRelationships(ctx, id)
		if err != nil {
			return false, errExistingOrObject("service", id, err)
		}
		desired := c.desired.Services[id]
		if !graph.SameStrings(servers, desired.Servers) || !graph.SameStrings(filters, desired.Filters) {
			return false, nil
		}
	}
	return true, nil
}
