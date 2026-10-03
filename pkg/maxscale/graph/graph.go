package graph

import (
	"sort"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
)

// ServerTLS holds the spec-derived TLS flags that are part of a server REST configuration.
type ServerTLS struct {
	Enabled               bool
	VerifyPeerCertificate bool
	VerifyPeerHost        bool
	ReplicationSSLEnabled bool
}

// Server is a desired MaxScale server node.
type Server struct {
	Name        string
	Address     string
	Port        int32
	Protocol    string
	Maintenance bool
	TLS         ServerTLS
	Params      map[string]string
}

// Filter is a desired MaxScale filter node.
type Filter struct {
	Name   string
	Module string
	Params map[string]string
}

// Monitor is the desired MaxScale monitor node and its server relationships.
type Monitor struct {
	Name                  string
	Module                mariadbv1alpha1.MonitorModule
	IntervalMillis        int64
	CooperativeMonitoring *mariadbv1alpha1.CooperativeMonitoring
	Suspend               bool
	Servers               []string
	Params                map[string]string
}

// Listener is the desired entry point attached to a service.
type Listener struct {
	Name     string
	Port     int32
	Protocol string
	Suspend  bool
	TLS      bool
	Params   map[string]string
}

// Service is a desired MaxScale routing service node.
type Service struct {
	Name     string
	Router   mariadbv1alpha1.ServiceRouter
	Suspend  bool
	Servers  []string
	Filters  []string
	Listener Listener
	Params   map[string]string
}

// Graph is the spec-derived dependency graph managed via the MaxScale REST API.
// It deliberately excludes credentials: monitor and service passwords are not part of the graph.
type Graph struct {
	Servers   map[string]Server
	Filters   map[string]Filter
	Monitors  map[string]Monitor
	Services  map[string]Service
	Listeners map[string]Listener
}

// DesiredFromSpec builds the desired dependency graph from a defaulted MaxScale spec.
func DesiredFromSpec(mxs *mariadbv1alpha1.MaxScale) *Graph {
	serverIDs := mxs.ServerIDs()
	g := &Graph{
		Servers:   make(map[string]Server, len(mxs.Spec.Servers)),
		Filters:   make(map[string]Filter, len(mxs.Spec.Filters)),
		Monitors:  make(map[string]Monitor, 1),
		Services:  make(map[string]Service, len(mxs.Spec.Services)),
		Listeners: make(map[string]Listener, len(mxs.Spec.Services)),
	}

	serverTLS := ServerTLS{
		Enabled:               mxs.IsTLSEnabled(),
		VerifyPeerCertificate: mxs.ShouldVerifyPeerCertificate(),
		VerifyPeerHost:        mxs.ShouldVerifyPeerHost(),
		ReplicationSSLEnabled: mxs.IsReplicationSSLEnabled(),
	}
	for _, srv := range mxs.Spec.Servers {
		g.Servers[srv.Name] = Server{
			Name:        srv.Name,
			Address:     srv.Address,
			Port:        srv.Port,
			Protocol:    srv.Protocol,
			Maintenance: srv.Maintenance,
			TLS:         serverTLS,
			Params:      srv.Params,
		}
	}

	for _, filter := range mxs.Spec.Filters {
		g.Filters[filter.Name] = Filter{
			Name:   filter.Name,
			Module: filter.Module,
			Params: filter.Params,
		}
	}

	mon := mxs.Spec.Monitor
	g.Monitors[mon.Name] = Monitor{
		Name:                  mon.Name,
		Module:                mon.Module,
		IntervalMillis:        mon.Interval.Milliseconds(),
		CooperativeMonitoring: mon.CooperativeMonitoring,
		Suspend:               mon.Suspend,
		Servers:               sortedCopy(serverIDs),
		Params:                mon.Params,
	}

	for _, svc := range mxs.Spec.Services {
		filters := make([]string, len(svc.Filters))
		copy(filters, svc.Filters)
		service := Service{
			Name:    svc.Name,
			Router:  svc.Router,
			Suspend: svc.Suspend,
			Servers: sortedCopy(serverIDs),
			Filters: filters,
			Listener: Listener{
				Name:     svc.Listener.Name,
				Port:     svc.Listener.Port,
				Protocol: svc.Listener.Protocol,
				Suspend:  svc.Listener.Suspend,
				TLS:      mxs.IsTLSEnabled(),
				Params:   svc.Listener.Params,
			},
			Params: svc.Params,
		}
		g.Services[svc.Name] = service
		g.Listeners[svc.Listener.Name] = service.Listener
	}

	return g
}

// ObjectIDs returns the sorted ID set of the graph by kind.
func (g *Graph) ObjectIDs() *mariadbv1alpha1.MaxScaleConfigObjectIDs {
	return idsFromSets(
		newIDSet(g.serverIDs()...),
		newIDSet(g.filterIDs()...),
		newIDSet(g.monitorIDs()...),
		newIDSet(g.serviceIDs()...),
		newIDSet(g.listenerIDs()...),
	)
}

func (g *Graph) serverIDs() []string {
	ids := make([]string, 0, len(g.Servers))
	for id := range g.Servers {
		ids = append(ids, id)
	}
	return sortedStrings(ids)
}

func (g *Graph) filterIDs() []string {
	ids := make([]string, 0, len(g.Filters))
	for id := range g.Filters {
		ids = append(ids, id)
	}
	return sortedStrings(ids)
}

func (g *Graph) monitorIDs() []string {
	ids := make([]string, 0, len(g.Monitors))
	for id := range g.Monitors {
		ids = append(ids, id)
	}
	return sortedStrings(ids)
}

func (g *Graph) serviceIDs() []string {
	ids := make([]string, 0, len(g.Services))
	for id := range g.Services {
		ids = append(ids, id)
	}
	return sortedStrings(ids)
}

func (g *Graph) listenerIDs() []string {
	ids := make([]string, 0, len(g.Listeners))
	for id := range g.Listeners {
		ids = append(ids, id)
	}
	return sortedStrings(ids)
}

func sortedCopy(ids []string) []string {
	copied := make([]string, len(ids))
	copy(copied, ids)
	return sortedStrings(copied)
}

// SortedCopy returns a sorted copy without modifying the caller's slice.
func SortedCopy(ids []string) []string {
	return sortedCopy(ids)
}

func sortedStrings(ids []string) []string {
	sort.Strings(ids)
	return ids
}
