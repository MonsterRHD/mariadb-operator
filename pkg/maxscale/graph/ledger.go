package graph

import (
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
)

// Ledger is the set of MaxScale REST objects currently owned by a MaxScale CR.
type Ledger struct {
	servers   IDSet
	filters   IDSet
	monitors  IDSet
	services  IDSet
	listeners IDSet
}

func ledgerFromIDs(ids *mariadbv1alpha1.MaxScaleConfigObjectIDs) Ledger {
	if ids == nil {
		return Ledger{
			servers:   IDSet{},
			filters:   IDSet{},
			monitors:  IDSet{},
			services:  IDSet{},
			listeners: IDSet{},
		}
	}
	return Ledger{
		servers:   setsFromIDs(ids.Servers),
		filters:   setsFromIDs(ids.Filters),
		monitors:  setsFromIDs(ids.Monitor),
		services:  setsFromIDs(ids.Services),
		listeners: setsFromIDs(ids.Listeners),
	}
}

func (l Ledger) objectIDs() *mariadbv1alpha1.MaxScaleConfigObjectIDs {
	return idsFromSets(l.servers, l.filters, l.monitors, l.services, l.listeners)
}

func (l Ledger) Union(other Ledger) Ledger {
	return Ledger{
		servers:   l.servers.Union(other.servers),
		filters:   l.filters.Union(other.filters),
		monitors:  l.monitors.Union(other.monitors),
		services:  l.services.Union(other.services),
		listeners: l.listeners.Union(other.listeners),
	}
}

// Live is the observed MaxScale REST presence and relationships used to plan a change.
// Presence maps are always populated; relationship maps are only needed for operation classification.
type Live struct {
	Servers   IDSet
	Filters   IDSet
	Monitors  IDSet
	Services  IDSet
	Listeners IDSet
	// ListenerPorts maps a bound port to the live listener holding it.
	ListenerPorts  map[int32]string
	MonitorServers map[string][]string
	ServiceServers map[string][]string
	ServiceFilters map[string][]string
}

// NewLive returns an empty Live observation.
func NewLive() Live {
	return Live{
		Servers:        IDSet{},
		Filters:        IDSet{},
		Monitors:       IDSet{},
		Services:       IDSet{},
		Listeners:      IDSet{},
		ListenerPorts:  map[int32]string{},
		MonitorServers: map[string][]string{},
		ServiceServers: map[string][]string{},
		ServiceFilters: map[string][]string{},
	}
}

// LiveFromObjectIDs populates presence from an ID set; relationships stay empty.
func LiveFromObjectIDs(ids *mariadbv1alpha1.MaxScaleConfigObjectIDs) Live {
	live := NewLive()
	if ids == nil {
		return live
	}
	live.Servers = setsFromIDs(ids.Servers)
	live.Filters = setsFromIDs(ids.Filters)
	live.Monitors = setsFromIDs(ids.Monitor)
	live.Services = setsFromIDs(ids.Services)
	live.Listeners = setsFromIDs(ids.Listeners)
	return live
}

// Adopt returns the ownership ledger for the first reconciliation after upgrade:
// the desired objects that are also present live. Foreign live objects are never adopted.
// A live listener squatting a desired port is not adopted either: displacing an object the CR
// has never owned is unsafe, so that change blocks until the squatter is removed manually.
func Adopt(desired *Graph, live Live) *mariadbv1alpha1.MaxScaleConfigObjectIDs {
	return idsFromSets(
		setsFromIDs(desired.serverIDs()).Intersect(live.Servers),
		setsFromIDs(desired.filterIDs()).Intersect(live.Filters),
		setsFromIDs(desired.monitorIDs()).Intersect(live.Monitors),
		setsFromIDs(desired.serviceIDs()).Intersect(live.Services),
		setsFromIDs(desired.listenerIDs()).Intersect(live.Listeners),
	)
}

// Supersede discards a stale in-flight change and returns an updated managed ledger that
// also owns the debris the stale change prepared, so the newer change can clean it up.
func Supersede(managed, stalePrepared *mariadbv1alpha1.MaxScaleConfigObjectIDs) *mariadbv1alpha1.MaxScaleConfigObjectIDs {
	return ledgerFromIDs(managed).Union(ledgerFromIDs(stalePrepared)).objectIDs()
}
