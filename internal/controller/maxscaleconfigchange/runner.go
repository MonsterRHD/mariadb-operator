package maxscaleconfigchange

import (
	"context"
	"fmt"
	"sort"
	"time"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/converge"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/maxscale/graph"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Runner executes generation-scoped configuration changes against injectable REST, status
// and event boundaries so the change is recoverable and unit-testable without a live cluster.
type Runner struct {
	Status          Status
	Events          EventRecorder
	Refs            References
	Now             func() metav1.Time
	DatabaseVersion func(ctx context.Context, mxs *mariadbv1alpha1.MaxScale) (int, error)
}

// EventRecorder is the subset of k8s.io/client-go/tools/events.EventRecorder used by the runner.
type EventRecorder interface {
	Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{})
}

func (rn *Runner) now() metav1.Time {
	if rn.Now != nil {
		return rn.Now()
	}
	return metav1.Now()
}

// Reconcile drives the transaction. It returns ctrl.Result{RequeueAfter: syncInterval} while the
// graph is converging across replicas, and zero Result once the generation has been committed.
func (rn *Runner) Reconcile(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	writer REST, pods map[string]REST) (ctrl.Result, error) {
	desired := graph.DesiredFromSpec(mxs)
	desiredHash, err := desired.Hash()
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error hashing desired configuration graph: %v", err)
	}

	// Validation precedes every REST mutation: no configuration write happens with an invalid graph.
	// It is recorded on the settled change afterwards, so an adoption ledger (never an empty one)
	// governs what a later switch is allowed to displace.
	var validationErr error
	if rn.Refs != nil {
		if err := rn.Refs.Validate(ctx, mxs); err != nil {
			validationErr = fmt.Errorf("error validating dynamic references: %w", err)
		}
	}
	if validationErr == nil {
		if err := desired.Validate(); err != nil {
			validationErr = err
		}
	}

	change, err := rn.settle(ctx, mxs, desired, desiredHash, writer)
	if err != nil {
		return ctrl.Result{}, err
	}
	if validationErr != nil {
		// A nil change means the current live graph was adopted and committed; still surface the
		// invalid desired graph without fabricating an ownership ledger.
		if change == nil {
			return ctrl.Result{}, fmt.Errorf("error validating desired configuration graph: %v", validationErr)
		}
		return rn.failValidation(ctx, mxs, validationErr)
	}
	if change == nil {
		return ctrl.Result{}, nil
	}
	plan, err := rn.resolvePlan(ctx, writer, desired, change)
	if err != nil {
		return ctrl.Result{}, err
	}
	if blocked, err := blockedByForeignListener(desired, plan); err != nil {
		return ctrl.Result{}, rn.failStage(ctx, mxs, change, "listeners", blocked, err)
	}

	for {
		result, err, done := rn.runStage(ctx, mxs, desired, desiredHash, plan, change, writer, pods)
		if done {
			return result, err
		}
	}
}

func (rn *Runner) runStage(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, desiredHash string, plan *graph.Plan,
	change *mariadbv1alpha1.MaxScaleConfigChangeStatus, writer REST,
	pods map[string]REST) (ctrl.Result, error, bool) {
	switch change.Stage {
	case mariadbv1alpha1.ConfigChangeStageValidating:
		rn.event(mxs, corev1.EventTypeNormal, mariadbv1alpha1.ReasonMaxScaleConfigChangePreparing,
			"Preparing configuration for generation %d without disrupting the previous graph", mxs.Generation)
		if err := rn.transition(ctx, mxs, change, mariadbv1alpha1.ConfigChangeStagePreparing); err != nil {
			return ctrl.Result{}, err, true
		}

	case mariadbv1alpha1.ConfigChangeStagePreparing:
		if result, err := rn.prepare(ctx, mxs, desired, plan, change, writer); !result.IsZero() || err != nil {
			return result, err, true
		}
		result, err := rn.awaitConvergence(ctx, mxs, desired, pods, converge.Objects{
			Servers:  unionSorted(plan.Creates.Servers, plan.Patches.Servers),
			Filters:  unionSorted(plan.Creates.Filters, plan.Patches.Filters),
			Monitors: unionSorted(plan.Creates.Monitor, plan.Patches.Monitor),
			Services: unionSorted(plan.Creates.Services, plan.Patches.Services),
			Listeners: unionSorted(
				excludeSorted(plan.Creates.Listeners, plan.ConflictingListeners),
				plan.Patches.Listeners),
		})
		if !result.IsZero() || err != nil {
			return result, err, true
		}
		rn.event(mxs, corev1.EventTypeNormal, mariadbv1alpha1.ReasonMaxScaleConfigChangeSwitching,
			"Switching entry points for generation %d", mxs.Generation)
		if err := rn.transition(ctx, mxs, change, mariadbv1alpha1.ConfigChangeStageSwitching); err != nil {
			return ctrl.Result{}, err, true
		}

	case mariadbv1alpha1.ConfigChangeStageSwitching:
		if err := rn.switchChange(ctx, mxs, desired, plan, change, writer, pods); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "", "", err), true
		}
		result, err := rn.awaitConvergence(ctx, mxs, desired, pods, converge.Objects{
			Servers:          desired.ObjectIDs().Servers,
			Filters:          desired.ObjectIDs().Filters,
			Monitors:         desired.ObjectIDs().Monitor,
			Services:         desired.ObjectIDs().Services,
			Listeners:        desired.ObjectIDs().Listeners,
			MonitorRelations: desired.ObjectIDs().Monitor,
			ServiceRelations: desired.ObjectIDs().Services,
			RunningListeners: runningListeners(desired),
		})
		if !result.IsZero() || err != nil {
			return result, err, true
		}
		rn.event(mxs, corev1.EventTypeNormal, mariadbv1alpha1.ReasonMaxScaleConfigChangeCleaningUp,
			"Cleaning previous-generation objects for generation %d", mxs.Generation)
		if err := rn.transition(ctx, mxs, change, mariadbv1alpha1.ConfigChangeStageCleaningUp); err != nil {
			return ctrl.Result{}, err, true
		}

	case mariadbv1alpha1.ConfigChangeStageCleaningUp:
		if err := rn.cleanup(ctx, mxs, plan, change, writer, pods); err != nil {
			return ctrl.Result{}, err, true
		}
		return ctrl.Result{}, rn.commit(ctx, mxs, desired, desiredHash), true

	default:
		log.FromContext(ctx).Info("Discarding unknown configuration change stage, restarting", "stage", change.Stage)
		if err := rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
			c.Stage = mariadbv1alpha1.ConfigChangeStageValidating
			return nil
		}); err != nil {
			return ctrl.Result{}, err, true
		}
	}
	return ctrl.Result{}, nil, false
}

// plan is resolved lazily after the Validating stage so REST reads remain a pure validation boundary.

func (rn *Runner) settle(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, desiredHash string, writer REST) (*mariadbv1alpha1.MaxScaleConfigChangeStatus, error) {
	existing := mxs.Status.ConfigChange

	if existing != nil && existing.Generation != mxs.Generation {
		var prepared *mariadbv1alpha1.MaxScaleConfigObjectIDs
		if existing.Checkpoints != nil {
			prepared = existing.Checkpoints.Prepared
		}
		managed := graph.Supersede(existing.Managed, prepared)
		log.FromContext(ctx).Info("Discarding stale configuration change from an older generation",
			"stale-generation", existing.Generation, "new-generation", mxs.Generation)
		newChange := &mariadbv1alpha1.MaxScaleConfigChangeStatus{
			Generation:  mxs.Generation,
			Stage:       mariadbv1alpha1.ConfigChangeStageValidating,
			DesiredHash: desiredHash,
			LastApplied: existing.LastApplied,
			Managed:     managed,
		}
		if err := rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
			*c = *newChange
			return nil
		}); err != nil {
			return nil, err
		}
		return newChange, nil
	}

	if existing != nil && existing.CompleteFor(mxs.Generation) {
		if existing.DesiredHash == desiredHash {
			return nil, nil
		}
		existing = nil // same generation committed, start a new change conservatively
	}

	if existing == nil {
		change := &mariadbv1alpha1.MaxScaleConfigChangeStatus{
			Generation:  mxs.Generation,
			Stage:       mariadbv1alpha1.ConfigChangeStageValidating,
			DesiredHash: desiredHash,
		}
		live, err := writer.ObserveLive(ctx)
		if err != nil {
			return nil, fmt.Errorf("error observing live configuration for adoption: %v", err)
		}
		adopted := graph.Adopt(desired, live)
		change.Managed = adopted
		change.LastApplied = adopted
		if sameIDs(adopted, desired.ObjectIDs()) {
			change.Stage = mariadbv1alpha1.ConfigChangeStageCompleted
			rn.event(mxs, corev1.EventTypeNormal, mariadbv1alpha1.ReasonMaxScaleConfigChangeCompleted,
				"Adopted and committed existing configuration for generation %d", mxs.Generation)
		}
		if err := rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
			*c = *change
			return nil
		}); err != nil {
			return nil, err
		}
		if change.Stage == mariadbv1alpha1.ConfigChangeStageCompleted {
			return nil, nil
		}
		return change, nil
	}

	return existing, nil
}

// prepare only creates and parameter-patches objects. It never deletes nor stops
// previous listeners, services or monitors.
func (rn *Runner) prepare(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, plan *graph.Plan, change *mariadbv1alpha1.MaxScaleConfigChangeStatus,
	writer REST) (ctrl.Result, error) {
	prepared := ensurePrepared(change)

	if result, err := rn.prepareServers(ctx, mxs, desired, plan, change, writer, prepared); !result.IsZero() || err != nil {
		return result, err
	}
	if result, err := rn.prepareFilters(ctx, mxs, desired, plan, change, writer, prepared); !result.IsZero() || err != nil {
		return result, err
	}
	if result, err := rn.prepareMonitor(ctx, mxs, desired, plan, change, writer, prepared); !result.IsZero() || err != nil {
		return result, err
	}
	if result, err := rn.prepareServices(ctx, mxs, desired, plan, change, writer, prepared); !result.IsZero() || err != nil {
		return result, err
	}
	return ctrl.Result{}, rn.prepareListeners(ctx, mxs, desired, plan, change, writer, prepared)
}

func (rn *Runner) prepareServers(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, plan *graph.Plan, change *mariadbv1alpha1.MaxScaleConfigChangeStatus,
	writer REST, prepared *mariadbv1alpha1.MaxScaleConfigObjectIDs) (ctrl.Result, error) {
	for _, id := range plan.Patches.Servers {
		if contains(prepared.Servers, id) {
			continue
		}
		if err := writer.PatchServer(ctx, desired.Servers[id]); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "servers", id, err)
		}
		prepared.Servers = append(prepared.Servers, id)
	}
	for _, id := range plan.Creates.Servers {
		if contains(prepared.Servers, id) {
			continue
		}
		srv := desired.Servers[id]
		if err := writer.CreateServer(ctx, srv); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "servers", id, err)
		}
		if err := writer.SetServerMaintenance(ctx, srv); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "servers", id, err)
		}
		prepared.Servers = append(prepared.Servers, id)
	}
	return ctrl.Result{}, rn.savePrepared(ctx, mxs, change, prepared)
}

func (rn *Runner) prepareFilters(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, plan *graph.Plan, change *mariadbv1alpha1.MaxScaleConfigChangeStatus,
	writer REST, prepared *mariadbv1alpha1.MaxScaleConfigObjectIDs) (ctrl.Result, error) {
	for _, id := range plan.Patches.Filters {
		if contains(prepared.Filters, id) {
			continue
		}
		if err := writer.PatchFilter(ctx, desired.Filters[id]); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "filters", id, err)
		}
		prepared.Filters = append(prepared.Filters, id)
	}
	for _, id := range plan.Creates.Filters {
		if contains(prepared.Filters, id) {
			continue
		}
		if err := writer.CreateFilter(ctx, desired.Filters[id]); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "filters", id, err)
		}
		prepared.Filters = append(prepared.Filters, id)
	}
	return ctrl.Result{}, rn.savePrepared(ctx, mxs, change, prepared)
}

func (rn *Runner) prepareMonitor(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, plan *graph.Plan, change *mariadbv1alpha1.MaxScaleConfigChangeStatus,
	writer REST, prepared *mariadbv1alpha1.MaxScaleConfigObjectIDs) (ctrl.Result, error) {
	if plan.MonitorCreate && !change.Checkpoints.MonitorCreated {
		monitorID := plan.Creates.Monitor[0]
		if err := writer.CreateMonitorStopped(ctx, desired.Monitors[monitorID]); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "monitors", monitorID, err)
		}
		prepared.Monitor = append(prepared.Monitor, monitorID)
		change.Checkpoints.MonitorCreated = true
	}
	for _, id := range plan.Patches.Monitor {
		if contains(prepared.Monitor, id) {
			continue
		}
		if err := writer.PatchMonitorParams(ctx, desired.Monitors[id]); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "monitors", id, err)
		}
		prepared.Monitor = append(prepared.Monitor, id)
	}
	return ctrl.Result{}, rn.savePrepared(ctx, mxs, change, prepared)
}

func (rn *Runner) prepareServices(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, plan *graph.Plan, change *mariadbv1alpha1.MaxScaleConfigChangeStatus,
	writer REST, prepared *mariadbv1alpha1.MaxScaleConfigObjectIDs) (ctrl.Result, error) {
	for _, id := range plan.Creates.Services {
		if contains(prepared.Services, id) {
			continue
		}
		if err := writer.CreateService(ctx, desired.Services[id]); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "services", id, err)
		}
		prepared.Services = append(prepared.Services, id)
	}
	for _, id := range plan.Patches.Services {
		if contains(prepared.Services, id) {
			continue
		}
		if err := writer.PatchServiceParams(ctx, desired.Services[id]); err != nil {
			return ctrl.Result{}, rn.failStage(ctx, mxs, change, "services", id, err)
		}
		prepared.Services = append(prepared.Services, id)
	}
	return ctrl.Result{}, rn.savePrepared(ctx, mxs, change, prepared)
}

func (rn *Runner) prepareListeners(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, plan *graph.Plan, change *mariadbv1alpha1.MaxScaleConfigChangeStatus,
	writer REST, prepared *mariadbv1alpha1.MaxScaleConfigObjectIDs) error {
	for _, id := range plan.Patches.Listeners {
		if contains(prepared.Listeners, id) {
			continue
		}
		listener := desired.Listeners[id]
		serviceName, err := mxs.ServiceForListener(id)
		if err != nil {
			return rn.failStage(ctx, mxs, change, "listeners", id, err)
		}
		if err := writer.PatchListener(ctx, listener, serviceName); err != nil {
			return rn.failStage(ctx, mxs, change, "listeners", id, err)
		}
		prepared.Listeners = append(prepared.Listeners, id)
	}
	for _, id := range plan.Creates.Listeners {
		if contains(plan.ConflictingListeners, id) || contains(prepared.Listeners, id) {
			continue
		}
		if err := writer.CreateListener(ctx, desired.Listeners[id], listenerServiceFor(desired, id)); err != nil {
			return rn.failStage(ctx, mxs, change, "listeners", id, err)
		}
		prepared.Listeners = append(prepared.Listeners, id)
	}
	return rn.savePrepared(ctx, mxs, change, prepared)
}

// switchChange moves topology relationships and entry points. Runtime state is applied on every
// Pod because config sync does not propagate start/stop state.
func (rn *Runner) switchChange(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, plan *graph.Plan, change *mariadbv1alpha1.MaxScaleConfigChangeStatus,
	writer REST, pods map[string]REST) error {
	if !change.Checkpoints.MonitorRelationsSwitched {
		if err := rn.switchMonitor(ctx, desired, plan, writer, pods); err != nil {
			return err
		}
		change.Checkpoints.MonitorRelationsSwitched = true
	}

	for _, id := range plan.ServiceRelationsSwitch {
		if contains(change.Checkpoints.SwitchedServices, id) {
			continue
		}
		if err := writer.PatchServiceRelations(ctx, desired.Services[id]); err != nil {
			return fmt.Errorf("error switching service relationships '%s': %v", id, err)
		}
		change.Checkpoints.SwitchedServices = append(change.Checkpoints.SwitchedServices, id)
	}

	for _, id := range plan.ConflictingListeners {
		if !contains(change.Checkpoints.SwitchedListeners, id) {
			if err := rn.switchConflictingListener(ctx, desired, plan, id, writer, pods); err != nil {
				return err
			}
			change.Checkpoints.SwitchedListeners = append(change.Checkpoints.SwitchedListeners, id)
		}
	}

	if err := rn.alignNewListenerStates(ctx, desired, plan, pods); err != nil {
		return err
	}
	if err := rn.alignServerMaintenance(ctx, desired, pods); err != nil {
		return err
	}
	if err := rn.alignMonitorState(ctx, desired, plan, pods); err != nil {
		return err
	}

	return rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
		c.Checkpoints = change.Checkpoints
		return nil
	})
}

func (rn *Runner) switchMonitor(ctx context.Context, desired *graph.Graph, plan *graph.Plan,
	writer REST, pods map[string]REST) error {
	if plan.MonitorRename {
		newMonitor := plan.Creates.Monitor[0]
		oldMonitor := plan.Deletes.Monitor[0]
		if err := forEachPod(pods, func(_ string, api REST) error {
			return api.StopMonitor(ctx, oldMonitor)
		}); err != nil {
			return fmt.Errorf("error stopping old monitor '%s': %v", oldMonitor, err)
		}
		if err := writer.PatchMonitorRelations(ctx, desired.Monitors[newMonitor]); err != nil {
			return fmt.Errorf("error switching monitor relationships to '%s': %v", newMonitor, err)
		}
		if !desired.Monitors[newMonitor].Suspend {
			if err := forEachPod(pods, func(_ string, api REST) error {
				return api.StartMonitor(ctx, newMonitor)
			}); err != nil {
				return fmt.Errorf("error starting new monitor '%s': %v", newMonitor, err)
			}
		}
		return nil
	}
	if plan.MonitorRelationsSwitch {
		monitorID := plan.Patches.Monitor[0]
		if err := writer.PatchMonitorRelations(ctx, desired.Monitors[monitorID]); err != nil {
			return fmt.Errorf("error switching monitor relationships: %v", err)
		}
	}
	return nil
}

func (rn *Runner) switchConflictingListener(ctx context.Context, desired *graph.Graph, plan *graph.Plan,
	id string, writer REST, pods map[string]REST) error {
	newListener := desired.Listeners[id]
	oldListener := plan.ConflictingListenerHolders[id]
	if oldListener == "" || oldListener == id {
		return fmt.Errorf("unable to locate old listener holding port %d for '%s'", newListener.Port, id)
	}
	if err := forEachPod(pods, func(_ string, api REST) error {
		return api.StopListener(ctx, oldListener)
	}); err != nil {
		return fmt.Errorf("error stopping old listener '%s': %v", oldListener, err)
	}
	if err := writer.DeleteListener(ctx, oldListener); err != nil {
		return fmt.Errorf("error deleting old listener '%s': %v", oldListener, err)
	}
	if err := writer.CreateListener(ctx, newListener, listenerServiceFor(desired, id)); err != nil {
		return fmt.Errorf("error creating switched listener '%s': %v", id, err)
	}
	return forEachPod(pods, func(_ string, api REST) error {
		if newListener.Suspend {
			return api.StopListener(ctx, id)
		}
		return api.StartListener(ctx, id)
	})
}

func (rn *Runner) alignNewListenerStates(ctx context.Context, desired *graph.Graph, plan *graph.Plan,
	pods map[string]REST) error {
	for _, id := range plan.Creates.Listeners {
		if contains(plan.ConflictingListeners, id) {
			continue
		}
		suspend := desired.Listeners[id].Suspend
		if err := forEachPod(pods, func(_ string, api REST) error {
			if suspend {
				return api.StopListener(ctx, id)
			}
			return api.StartListener(ctx, id)
		}); err != nil {
			return fmt.Errorf("error reconciling new listener state '%s': %v", id, err)
		}
	}
	return nil
}

func (rn *Runner) alignServerMaintenance(ctx context.Context, desired *graph.Graph, pods map[string]REST) error {
	for _, id := range desired.ObjectIDs().Servers {
		srv := desired.Servers[id]
		if err := forEachPod(pods, func(_ string, api REST) error {
			return api.SetServerMaintenance(ctx, srv)
		}); err != nil {
			return fmt.Errorf("error reconciling maintenance state for server '%s': %v", id, err)
		}
	}
	return nil
}

func (rn *Runner) alignMonitorState(ctx context.Context, desired *graph.Graph, plan *graph.Plan,
	pods map[string]REST) error {
	if plan.MonitorRename || len(plan.Patches.Monitor) == 0 {
		return nil
	}
	monitorID := plan.Patches.Monitor[0]
	suspend := desired.Monitors[monitorID].Suspend
	return forEachPod(pods, func(_ string, api REST) error {
		if suspend {
			return api.StopMonitor(ctx, monitorID)
		}
		return api.StartMonitor(ctx, monitorID)
	})
}

// cleanup deletes only objects in the managed ledger that the desired graph no longer declares,
// following the dependency order listeners -> services -> monitor -> filters -> servers.
func (rn *Runner) cleanup(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	plan *graph.Plan, change *mariadbv1alpha1.MaxScaleConfigChangeStatus,
	writer REST, pods map[string]REST) error {
	cleaned := change.Checkpoints.Cleaned
	if cleaned == nil {
		cleaned = &mariadbv1alpha1.MaxScaleConfigObjectIDs{}
	}

	if err := rn.deleteManagedKind(ctx, mxs, change, cleaned, "listeners", plan.Deletes.Listeners,
		writer.DeleteListener); err != nil {
		return err
	}
	if err := rn.deleteManagedKind(ctx, mxs, change, cleaned, "services", plan.Deletes.Services,
		writer.DeleteService); err != nil {
		return err
	}
	if err := rn.deleteMonitors(ctx, mxs, plan, change, cleaned, writer, pods); err != nil {
		return err
	}
	if err := rn.deleteManagedKind(ctx, mxs, change, cleaned, "filters", plan.Deletes.Filters,
		writer.DeleteFilter); err != nil {
		return err
	}
	return rn.deleteManagedKind(ctx, mxs, change, cleaned, "servers", plan.Deletes.Servers,
		writer.DeleteServer)
}

func (rn *Runner) deleteManagedKind(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	change *mariadbv1alpha1.MaxScaleConfigChangeStatus, cleaned *mariadbv1alpha1.MaxScaleConfigObjectIDs,
	kind string, ids []string, deleteFn func(context.Context, string) error) error {
	cleanedIDs := cleanedIDsFor(kind, cleaned)
	for _, id := range ids {
		if contains(cleanedIDs, id) {
			continue
		}
		if err := deleteFn(ctx, id); err != nil {
			return rn.failStage(ctx, mxs, change, kind, id, err)
		}
		markCleaned(kind, id, cleaned)
		if err := rn.saveCleaned(ctx, mxs, change, cleaned); err != nil {
			return err
		}
	}
	return nil
}

func (rn *Runner) deleteMonitors(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	plan *graph.Plan, change *mariadbv1alpha1.MaxScaleConfigChangeStatus,
	cleaned *mariadbv1alpha1.MaxScaleConfigObjectIDs, writer REST, pods map[string]REST) error {
	for _, id := range plan.Deletes.Monitor {
		if contains(cleaned.Monitor, id) {
			continue
		}
		if err := forEachPod(pods, func(_ string, api REST) error {
			return api.StopMonitor(ctx, id)
		}); err != nil {
			return fmt.Errorf("error stopping old monitor '%s': %v", id, err)
		}
		if err := writer.DeleteMonitor(ctx, id); err != nil {
			return rn.failStage(ctx, mxs, change, "monitors", id, err)
		}
		cleaned.Monitor = append(cleaned.Monitor, id)
		if err := rn.saveCleaned(ctx, mxs, change, cleaned); err != nil {
			return err
		}
	}
	return nil
}

func cleanedIDsFor(kind string, cleaned *mariadbv1alpha1.MaxScaleConfigObjectIDs) []string {
	switch kind {
	case "listeners":
		return cleaned.Listeners
	case "services":
		return cleaned.Services
	case "monitors":
		return cleaned.Monitor
	case "filters":
		return cleaned.Filters
	case "servers":
		return cleaned.Servers
	default:
		return nil
	}
}

func markCleaned(kind, id string, cleaned *mariadbv1alpha1.MaxScaleConfigObjectIDs) {
	switch kind {
	case "listeners":
		cleaned.Listeners = append(cleaned.Listeners, id)
	case "services":
		cleaned.Services = append(cleaned.Services, id)
	case "monitors":
		cleaned.Monitor = append(cleaned.Monitor, id)
	case "filters":
		cleaned.Filters = append(cleaned.Filters, id)
	case "servers":
		cleaned.Servers = append(cleaned.Servers, id)
	}
}

func (rn *Runner) awaitConvergence(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, pods map[string]REST, objs converge.Objects) (ctrl.Result, error) {
	syncInterval := 5 * time.Second
	if mxs.IsHAEnabled() && mxs.Spec.Config.Sync != nil && mxs.Spec.Config.Sync.Interval.Duration > 0 {
		syncInterval = mxs.Spec.Config.Sync.Interval.Duration
	}
	var dbVersion func(context.Context) (int, error)
	if rn.DatabaseVersion != nil {
		dbVersion = func(ctx context.Context) (int, error) {
			return rn.DatabaseVersion(ctx, mxs)
		}
	}
	checker := converge.NewChecker(desired, mxs.IsHAEnabled(), syncInterval, dbVersion)
	converged, err := checker.Converged(ctx, podBarrier(pods), objs)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error waiting for configuration convergence: %v", err)
	}
	if !converged {
		return ctrl.Result{RequeueAfter: syncInterval}, nil
	}
	return ctrl.Result{}, nil
}

func (rn *Runner) commit(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	desired *graph.Graph, desiredHash string) error {
	if err := rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
		ids := desired.ObjectIDs()
		c.Stage = mariadbv1alpha1.ConfigChangeStageCompleted
		c.DesiredHash = desiredHash
		c.LastApplied = ids
		c.Managed = ids
		c.Checkpoints = nil
		c.Error = nil
		return nil
	}); err != nil {
		return err
	}
	rn.event(mxs, corev1.EventTypeNormal, mariadbv1alpha1.ReasonMaxScaleConfigChangeCompleted,
		"Configuration for generation %d committed on every MaxScale replica", mxs.Generation)
	return nil
}

func (rn *Runner) failValidation(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	validationErr error) (ctrl.Result, error) {
	if err := rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
		c.Stage = mariadbv1alpha1.ConfigChangeStageValidating
		c.Error = &mariadbv1alpha1.MaxScaleConfigChangeError{
			Stage:   mariadbv1alpha1.ConfigChangeStageValidating,
			Message: validationErr.Error(),
			Time:    ptr.To(rn.now()),
		}
		return nil
	}); err != nil {
		return ctrl.Result{}, err
	}
	rn.event(mxs, corev1.EventTypeWarning, mariadbv1alpha1.ReasonMaxScaleConfigChangeInvalid,
		"Configuration validation failed: %v", validationErr)
	return ctrl.Result{}, fmt.Errorf("error validating desired configuration graph: %v", validationErr)
}

func (rn *Runner) failStage(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	change *mariadbv1alpha1.MaxScaleConfigChangeStatus, kind, object string, cause error) error {
	if err := rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
		c.Error = &mariadbv1alpha1.MaxScaleConfigChangeError{
			Stage:   change.Stage,
			Kind:    kind,
			Object:  object,
			Message: cause.Error(),
			Time:    ptr.To(rn.now()),
		}
		return nil
	}); err != nil {
		return err
	}
	rn.event(mxs, corev1.EventTypeWarning, mariadbv1alpha1.ReasonMaxScaleConfigChangeFailed,
		"Configuration change failed at %s on %s '%s': %v", change.Stage, kind, object, cause)
	return cause
}

func (rn *Runner) transition(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	change *mariadbv1alpha1.MaxScaleConfigChangeStatus, stage mariadbv1alpha1.MaxScaleConfigChangeStage) error {
	change.Stage = stage
	return rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
		c.Stage = stage
		c.Error = nil
		return nil
	})
}

func (rn *Runner) savePrepared(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	change *mariadbv1alpha1.MaxScaleConfigChangeStatus, prepared *mariadbv1alpha1.MaxScaleConfigObjectIDs) error {
	if change.Checkpoints == nil {
		change.Checkpoints = &mariadbv1alpha1.MaxScaleConfigChangeCheckpoints{}
	}
	return rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
		if c.Checkpoints == nil {
			c.Checkpoints = &mariadbv1alpha1.MaxScaleConfigChangeCheckpoints{}
		}
		c.Checkpoints.Prepared = prepared
		return nil
	})
}

func (rn *Runner) saveCleaned(ctx context.Context, mxs *mariadbv1alpha1.MaxScale,
	change *mariadbv1alpha1.MaxScaleConfigChangeStatus, cleaned *mariadbv1alpha1.MaxScaleConfigObjectIDs) error {
	if change.Checkpoints == nil {
		change.Checkpoints = &mariadbv1alpha1.MaxScaleConfigChangeCheckpoints{}
	}
	return rn.Status.PatchConfigChange(ctx, func(c *mariadbv1alpha1.MaxScaleConfigChangeStatus) error {
		if c.Checkpoints == nil {
			c.Checkpoints = &mariadbv1alpha1.MaxScaleConfigChangeCheckpoints{}
		}
		c.Checkpoints.Cleaned = cleaned
		return nil
	})
}

func (rn *Runner) event(mxs *mariadbv1alpha1.MaxScale, eventtype, reason, note string, args ...any) {
	if rn.Events == nil {
		return
	}
	rn.Events.Eventf(mxs, nil, eventtype, reason, reason, note, args...)
}

func ensurePrepared(change *mariadbv1alpha1.MaxScaleConfigChangeStatus) *mariadbv1alpha1.MaxScaleConfigObjectIDs {
	if change.Checkpoints != nil && change.Checkpoints.Prepared != nil {
		return change.Checkpoints.Prepared
	}
	return &mariadbv1alpha1.MaxScaleConfigObjectIDs{}
}

// ReconcilePlan resolves the plan lazily, after validation and before the first mutation.
func (rn *Runner) resolvePlan(ctx context.Context, writer REST, desired *graph.Graph,
	change *mariadbv1alpha1.MaxScaleConfigChangeStatus) (*graph.Plan, error) {
	live, err := writer.ObserveLive(ctx)
	if err != nil {
		return nil, fmt.Errorf("error observing live configuration: %v", err)
	}
	return graph.BuildPlan(desired, change.Managed, live), nil
}

func forEachPod(pods map[string]REST, fn func(podName string, api REST) error) error {
	names := make([]string, 0, len(pods))
	for podName := range pods {
		names = append(names, podName)
	}
	sort.Strings(names)
	for _, podName := range names {
		if err := fn(podName, pods[podName]); err != nil {
			return err
		}
	}
	return nil
}

// blockedByForeignListener refuses a change when a live listener outside the ownership ledger
// holds a port the desired graph needs: removing it would delete an object this CR does not own.
func blockedByForeignListener(desired *graph.Graph, plan *graph.Plan) (string, error) {
	if len(plan.ForeignPortHolders) == 0 {
		return "", nil
	}
	ids := make([]string, 0, len(plan.ForeignPortHolders))
	for id := range plan.ForeignPortHolders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	id := ids[0]
	holder := plan.ForeignPortHolders[id]
	return id, fmt.Errorf("listener port %d for '%s' is held by live listener '%s' that is not managed by this MaxScale CR",
		desired.Listeners[id].Port, id, holder)
}

func runningListeners(desired *graph.Graph) []string {
	ids := make([]string, 0, len(desired.Listeners))
	for _, id := range desired.ObjectIDs().Listeners {
		if !desired.Listeners[id].Suspend {
			ids = append(ids, id)
		}
	}
	return ids
}

func listenerServiceFor(desired *graph.Graph, listenerID string) string {
	for _, svc := range desired.Services {
		if svc.Listener.Name == listenerID {
			return svc.Name
		}
	}
	return ""
}

func contains(items []string, item string) bool {
	for _, i := range items {
		if i == item {
			return true
		}
	}
	return false
}

func excludeSorted(items, excluded []string) []string {
	result := make([]string, 0, len(items))
	for _, item := range items {
		if !contains(excluded, item) {
			result = append(result, item)
		}
	}
	return result
}

func unionSorted(a, b []string) []string {
	set := map[string]struct{}{}
	for _, id := range a {
		set[id] = struct{}{}
	}
	for _, id := range b {
		set[id] = struct{}{}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func sameIDs(a, b *mariadbv1alpha1.MaxScaleConfigObjectIDs) bool {
	return graph.SameStrings(a.Servers, b.Servers) &&
		graph.SameStrings(a.Filters, b.Filters) &&
		graph.SameStrings(a.Monitor, b.Monitor) &&
		graph.SameStrings(a.Services, b.Services) &&
		graph.SameStrings(a.Listeners, b.Listeners)
}

func podBarrier(pods map[string]REST) map[string]converge.PodAPI {
	barrier := make(map[string]converge.PodAPI, len(pods))
	for pod, api := range pods {
		barrier[pod] = api
	}
	return barrier
}
