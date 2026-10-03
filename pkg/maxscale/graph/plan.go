package graph

import (
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
)

// Plan classifies the REST operations required to move the live graph to the desired graph.
// Deletions are derived exclusively from the managed ledger; foreign live objects are never touched.
type Plan struct {
	// Creates are desired objects absent from live.
	Creates *mariadbv1alpha1.MaxScaleConfigObjectIDs
	// Patches are desired objects already present live (parameter-patch candidates).
	Patches *mariadbv1alpha1.MaxScaleConfigObjectIDs
	// Deletes are managed objects absent from the desired graph.
	Deletes *mariadbv1alpha1.MaxScaleConfigObjectIDs

	// MonitorCreate indicates that the desired monitor is absent from live and must be created stopped.
	MonitorCreate bool
	// MonitorRename indicates the desired monitor replaces a different managed monitor still present live.
	MonitorRename bool
	// MonitorRelationsSwitch indicates the same-name monitor server relationships change at the switch stage.
	MonitorRelationsSwitch bool

	// ConflictingListeners are desired listeners whose port is held by a live, non-desired listener
	// that the ledger owns: they are created at the switch stage, right after the holder is removed.
	ConflictingListeners []string
	// ConflictingListenerHolders maps a conflicting desired listener ID to the live listener
	// holding its port. Only ledger-owned holders are listed here.
	ConflictingListenerHolders map[string]string
	// ForeignPortHolders maps desired listener IDs to live holders that are not in the ownership
	// ledger. They block the change and must not be deleted by the operator.
	ForeignPortHolders map[string]string
	// ServiceRelationsSwitch are existing services whose servers/filters relationships change at the switch stage.
	ServiceRelationsSwitch []string
}

// BuildPlan classifies operations from the desired graph, the managed ownership ledger and the live observation.
func BuildPlan(desired *Graph, managed *mariadbv1alpha1.MaxScaleConfigObjectIDs, live Live) *Plan {
	desiredServers := setsFromIDs(desired.serverIDs())
	desiredFilters := setsFromIDs(desired.filterIDs())
	desiredMonitors := setsFromIDs(desired.monitorIDs())
	desiredServices := setsFromIDs(desired.serviceIDs())
	desiredListeners := setsFromIDs(desired.listenerIDs())

	ledger := ledgerFromIDs(managed)

	allHolders := listenerPortHolders(desired, live)
	conflictList := conflictingListeners(desired, live, ledger)
	foreignHolders := foreignPortHolders(desired, live, ledger)
	blockedListeners := setsFromIDs(keysOf(allHolders))
	creates := idsFromSets(
		desiredServers.Minus(live.Servers),
		desiredFilters.Minus(live.Filters),
		desiredMonitors.Minus(live.Monitors),
		desiredServices.Minus(live.Services),
		desiredListeners.Minus(live.Listeners).Minus(blockedListeners),
	)
	patches := idsFromSets(
		desiredServers.Intersect(live.Servers),
		desiredFilters.Intersect(live.Filters),
		desiredMonitors.Intersect(live.Monitors),
		desiredServices.Intersect(live.Services),
		desiredListeners.Intersect(live.Listeners),
	)
	deletes := idsFromSets(
		ledger.servers.Minus(desiredServers),
		ledger.filters.Minus(desiredFilters),
		ledger.monitors.Minus(desiredMonitors),
		ledger.services.Minus(desiredServices),
		ledger.listeners.Minus(desiredListeners),
	)

	monitorID := ""
	if ids := desired.monitorIDs(); len(ids) > 0 {
		monitorID = ids[0]
	}
	monitorCreate := monitorID != "" && !live.Monitors.Contains(monitorID)
	monitorRename := monitorCreate && setsFromIDs(deletes.Monitor).ContainsOtherThan(monitorID)

	plan := &Plan{
		Creates:                    creates,
		Patches:                    patches,
		Deletes:                    deletes,
		MonitorCreate:              monitorCreate,
		MonitorRename:              monitorRename,
		MonitorRelationsSwitch:     monitorID != "" && monitorRelationsChanged(desired, monitorID, live),
		ConflictingListeners:       conflictList,
		ConflictingListenerHolders: conflictHolders(allHolders, conflictList),
		ForeignPortHolders:         foreignHolders,
		ServiceRelationsSwitch:     serviceRelationsChanged(desired, live),
	}
	return plan
}

func monitorRelationsChanged(desired *Graph, monitorID string, live Live) bool {
	mon, ok := desired.Monitors[monitorID]
	if !ok {
		return false
	}
	liveServers, ok := live.MonitorServers[monitorID]
	if !ok {
		return false
	}
	return !sameStringSet(mon.Servers, liveServers)
}

func serviceRelationsChanged(desired *Graph, live Live) []string {
	changed := IDSet{}
	for _, id := range desired.serviceIDs() {
		svc := desired.Services[id]
		liveServers, ok := live.ServiceServers[id]
		if ok && !sameStringSet(svc.Servers, liveServers) {
			changed.Add(id)
			continue
		}
		liveFilters, ok := live.ServiceFilters[id]
		if ok && !sameStringSet(svc.Filters, liveFilters) {
			changed.Add(id)
		}
	}
	return changed.Sorted()
}

// conflictingListeners returns desired listeners that cannot bind during preparation because a
// live listener owned by the MaxScale CR still holds their port and is being removed.
func conflictingListeners(desired *Graph, live Live, ledger Ledger) []string {
	holders := listenerPortHolders(desired, live)
	conflicting := make([]string, 0, len(holders))
	for id, holder := range holders {
		if ledger.listeners.Contains(holder) {
			conflicting = append(conflicting, id)
		}
	}
	return SortedCopy(conflicting)
}

// foreignPortHolders maps desired listeners to live port holders that are not in the ownership
// ledger and therefore must not be removed by the operator.
func foreignPortHolders(desired *Graph, live Live, ledger Ledger) map[string]string {
	holders := listenerPortHolders(desired, live)
	foreign := map[string]string{}
	for id, holder := range holders {
		if !ledger.listeners.Contains(holder) {
			foreign[id] = holder
		}
	}
	return foreign
}

// listenerPortHolders maps every conflicting desired listener to the live listener holding its port.
func listenerPortHolders(desired *Graph, live Live) map[string]string {
	holders := map[string]string{}
	for _, id := range desired.listenerIDs() {
		listener := desired.Listeners[id]
		holder, ok := live.ListenerPorts[listener.Port]
		if !ok || holder == id {
			continue
		}
		// The holder survives in the desired graph, so the collision is a spec validation problem.
		if live.Listeners.Contains(holder) && setsFromIDs(desired.listenerIDs()).Contains(holder) {
			continue
		}
		holders[id] = holder
	}
	return holders
}

// conflictHolders restricts the holder map to the conflicting desired listener IDs.
func conflictHolders(holders map[string]string, conflictList []string) map[string]string {
	wanted := setsFromIDs(conflictList)
	filtered := map[string]string{}
	for id, holder := range holders {
		if wanted.Contains(id) {
			filtered[id] = holder
		}
	}
	return filtered
}

func sameStringSet(a, b []string) bool {
	return setsFromIDs(a).Equal(setsFromIDs(b))
}

func keysOf(m map[string]string) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return SortedCopy(ids)
}
