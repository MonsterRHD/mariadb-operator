package graph

import (
	"errors"
	"fmt"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
)

// Validate checks graph closure: identities are unique and complete and every
// relationship target exists. Dynamic references (Secrets) are resolved by the caller.
func (g *Graph) Validate() error {
	var errs []error
	errs = append(errs, g.validateServers()...)
	errs = append(errs, g.validateFilters()...)
	errs = append(errs, g.validateMonitor()...)
	errs = append(errs, g.validateServices()...)
	return errors.Join(errs...)
}

func (g *Graph) validateServers() []error {
	var errs []error
	addresses := make(map[string]string)
	for _, id := range g.serverIDs() {
		srv := g.Servers[id]
		if srv.Address == "" {
			errs = append(errs, fmt.Errorf("server '%s' address must be set", id))
		} else if other, ok := addresses[srv.Address]; ok {
			errs = append(errs, fmt.Errorf("servers '%s' and '%s' share the same address", other, id))
		} else {
			addresses[srv.Address] = id
		}
		if srv.Protocol == "" {
			errs = append(errs, fmt.Errorf("server '%s' protocol must be set", id))
		}
	}
	return errs
}

func (g *Graph) validateFilters() []error {
	var errs []error
	for _, id := range g.filterIDs() {
		if g.Filters[id].Module == "" {
			errs = append(errs, fmt.Errorf("filter '%s' module must be set", id))
		}
	}
	return errs
}

func (g *Graph) validateMonitor() []error {
	var errs []error
	for _, id := range g.monitorIDs() {
		mon := g.Monitors[id]
		if mon.Name == "" {
			errs = append(errs, errors.New("monitor name must be set"))
		}
		if err := mon.Module.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("monitor '%s' module invalid: %v", id, err))
		}
		for _, srvID := range mon.Servers {
			if _, ok := g.Servers[srvID]; !ok {
				errs = append(errs, fmt.Errorf("monitor '%s' references server '%s' which is not declared", id, srvID))
			}
		}
	}
	return errs
}

func (g *Graph) validateServices() []error {
	var errs []error
	listenerNames := make(map[string]string)
	listenerPorts := make(map[int32]string)
	for _, id := range g.serviceIDs() {
		svc := g.Services[id]
		if svc.Router != mariadbv1alpha1.ServiceRouterReadWriteSplit &&
			svc.Router != mariadbv1alpha1.ServiceRouterReadConnRoute {
			errs = append(errs, fmt.Errorf("service '%s' router invalid: '%s'", id, svc.Router))
		}
		errs = append(errs, g.validateServiceRelations(id, svc)...)
		errs = append(errs, g.validateListener(id, svc.Listener, listenerNames, listenerPorts)...)
	}
	return errs
}

func (g *Graph) validateServiceRelations(id string, svc Service) []error {
	var errs []error
	for _, serverID := range svc.Servers {
		if _, ok := g.Servers[serverID]; !ok {
			errs = append(errs, fmt.Errorf("service '%s' references server '%s' which is not declared", id, serverID))
		}
	}
	for _, filterID := range svc.Filters {
		if _, ok := g.Filters[filterID]; !ok {
			errs = append(errs, fmt.Errorf("service '%s' references filter '%s' which is not declared", id, filterID))
		}
	}
	return errs
}

func (g *Graph) validateListener(serviceID string, listener Listener,
	names map[string]string, ports map[int32]string) []error {
	var errs []error
	if listener.Name == "" {
		errs = append(errs, fmt.Errorf("service '%s' listener name must be set", serviceID))
	} else if other, ok := names[listener.Name]; ok {
		errs = append(errs, fmt.Errorf("listener name '%s' is shared by services '%s' and '%s'", listener.Name, other, serviceID))
	} else {
		names[listener.Name] = serviceID
	}
	if listener.Port <= 0 {
		errs = append(errs, fmt.Errorf("listener '%s' port must be set", listener.Name))
	} else if other, ok := ports[listener.Port]; ok {
		errs = append(errs, fmt.Errorf("listeners '%s' and '%s' share the same port %d", other, listener.Name, listener.Port))
	} else {
		ports[listener.Port] = listener.Name
	}
	if listener.Protocol == "" {
		errs = append(errs, fmt.Errorf("listener '%s' protocol must be set", listener.Name))
	}
	return errs
}
