package graph

import (
	"fmt"

	"github.com/mariadb-operator/mariadb-operator/v26/pkg/hash"
)

// Hash returns the canonical fingerprint of the whole desired graph.
// It excludes credentials, which are never part of the graph.
func (g *Graph) Hash() (string, error) {
	h, err := hash.HashJSON(g.canonical())
	if err != nil {
		return "", fmt.Errorf("error hashing graph: %v", err)
	}
	return h, nil
}

// ServerFingerprint returns the fingerprint of a server configuration, excluding its identity.
func (g *Graph) ServerFingerprint(id string) (string, bool, error) {
	srv, ok := g.Servers[id]
	if !ok {
		return "", false, nil
	}
	srv.Name = ""
	h, err := hash.HashJSON(srv)
	if err != nil {
		return "", true, fmt.Errorf("error hashing server '%s': %v", id, err)
	}
	return h, true, nil
}

// FilterFingerprint returns the fingerprint of a filter configuration, excluding its identity.
func (g *Graph) FilterFingerprint(id string) (string, bool, error) {
	filter, ok := g.Filters[id]
	if !ok {
		return "", false, nil
	}
	filter.Name = ""
	h, err := hash.HashJSON(filter)
	if err != nil {
		return "", true, fmt.Errorf("error hashing filter '%s': %v", id, err)
	}
	return h, true, nil
}

// MonitorFingerprint returns the fingerprint of the monitor configuration, excluding its identity.
func (g *Graph) MonitorFingerprint(id string) (string, bool, error) {
	mon, ok := g.Monitors[id]
	if !ok {
		return "", false, nil
	}
	mon.Name = ""
	h, err := hash.HashJSON(mon)
	if err != nil {
		return "", true, fmt.Errorf("error hashing monitor '%s': %v", id, err)
	}
	return h, true, nil
}

// ServiceFingerprint returns the fingerprint of a service configuration, excluding its identity.
func (g *Graph) ServiceFingerprint(id string) (string, bool, error) {
	svc, ok := g.Services[id]
	if !ok {
		return "", false, nil
	}
	svc.Name = ""
	svc.Listener.Name = ""
	h, err := hash.HashJSON(svc)
	if err != nil {
		return "", true, fmt.Errorf("error hashing service '%s': %v", id, err)
	}
	return h, true, nil
}

// ListenerFingerprint returns the fingerprint of a listener configuration, excluding its identity.
func (g *Graph) ListenerFingerprint(id string) (string, bool, error) {
	listener, ok := g.Listeners[id]
	if !ok {
		return "", false, nil
	}
	listener.Name = ""
	h, err := hash.HashJSON(listener)
	if err != nil {
		return "", true, fmt.Errorf("error hashing listener '%s': %v", id, err)
	}
	return h, true, nil
}

type canonicalGraph struct {
	Servers   []Server  `json:"servers"`
	Filters   []Filter  `json:"filters"`
	Monitors  []Monitor `json:"monitors"`
	Services  []Service `json:"services"`
	Listeners []Listener `json:"listeners"`
}

// canonical returns a slice-based, sorted representation of the graph so the
// fingerprint does not depend on Go map iteration order.
func (g *Graph) canonical() canonicalGraph {
	servers := make([]Server, 0, len(g.Servers))
	for _, id := range g.serverIDs() {
		servers = append(servers, g.Servers[id])
	}
	filters := make([]Filter, 0, len(g.Filters))
	for _, id := range g.filterIDs() {
		filters = append(filters, g.Filters[id])
	}
	monitors := make([]Monitor, 0, len(g.Monitors))
	for _, id := range g.monitorIDs() {
		monitors = append(monitors, g.Monitors[id])
	}
	services := make([]Service, 0, len(g.Services))
	for _, id := range g.serviceIDs() {
		services = append(services, g.Services[id])
	}
	listeners := make([]Listener, 0, len(g.Listeners))
	for _, id := range g.listenerIDs() {
		listeners = append(listeners, g.Listeners[id])
	}
	return canonicalGraph{
		Servers:   servers,
		Filters:   filters,
		Monitors:  monitors,
		Services:  services,
		Listeners: listeners,
	}
}
