package allocwatcher

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/endocrimes/nomad-vector/internal/system"
	"github.com/endocrimes/nomad-vector/internal/worker"
	"github.com/hashicorp/nomad/api"
	log "github.com/sirupsen/logrus"
)

const (
	nomadAllocClientStatusPending  = "pending"
	nomadAllocClientStatusRunning  = "running"
	nomadAllocClientStatusComplete = "complete"
	nomadAllocClientStatusFailed   = "failed"
	nomadAllocClientStatusLost     = "lost"
	nomadAllocClientStatusUnknown  = "unknown"
)

type AllocChangeSet struct {
	AddedOrUpdated []string
	Removed        []string

	NewState map[string]*api.Allocation
}

// Watcher manages watching for changes to allocations on a given Nomad node
// and will pass changed allocations to a provided channel.
type Watcher struct {
	client   *api.Client
	interval time.Duration
	allocCh  chan<- *AllocChangeSet
	nodeID   string

	trackedAllocs map[string]*api.Allocation
}

func (w *Watcher) HealthChecks() (name string, check system.HealthFunc) {
	return "allocwatcher", func(ctx context.Context) error {
		_, _, err := w.client.Allocations().List((&api.QueryOptions{
			AllowStale: true,
			PerPage:    1,
		}).WithContext(ctx))

		return err
	}
}

func (w *Watcher) fetchAllocations(ctx context.Context) ([]*api.Allocation, error) {
	q := (&api.QueryOptions{
		// AllowStale reads to minimize load on the leader - we might be slow to add
		// allocs to the tracker in some cases because of this, but the tradeoff is
		// somewhat worth it.
		AllowStale: true,

		// Fetch allocs from all namespaces
		Namespace: "*",

		// Filter by the current node
		Filter: fmt.Sprintf("NodeID==%q", w.nodeID),
	}).WithContext(ctx)

	stubs, _, err := w.client.Allocations().List(q)
	if err != nil {
		return nil, err
	}

	var result []*api.Allocation
	for _, stub := range stubs {
		logger := log.WithFields(log.Fields{
			"alloc_id": stub.ID,
			"job_id":   stub.JobID,
		})

		if stub.NodeID != w.nodeID {
			logger.WithField("node_id", stub.NodeID).Info("bug: found allocation with differing nodeid")
		}

		// Skip over jobs that are complete or failed, as they'll be safe to cleanup
		// downstream
		if stub.ClientStatus == nomadAllocClientStatusComplete ||
			stub.ClientStatus == nomadAllocClientStatusFailed {
			continue
		}

		nq := new(api.QueryOptions)
		*nq = *q
		nq.Namespace = stub.Namespace

		info, _, err := w.client.Allocations().Info(stub.ID, q)
		if err != nil {
			logger.WithError(err).Info("failed to fetch alloc info")
			continue
		}

		result = append(result, info)
	}

	return result, nil
}

func calculateChanges(existingState map[string]*api.Allocation, liveAllocs []*api.Allocation) (addedOrUpdated []string, removed []string, newState map[string]*api.Allocation) {
	newState = make(map[string]*api.Allocation, len(liveAllocs))
	for _, alloc := range liveAllocs {
		newState[alloc.ID] = alloc

		existing, ok := existingState[alloc.ID]
		// If we don't have the alloc already, or we do and the satus is changed,
		// it's new or updated.
		if !ok || (ok && existing.ClientStatus != alloc.ClientStatus) {
			log.WithField("alloc_id", alloc.ID).Info("added or updated allocation")
			addedOrUpdated = append(addedOrUpdated, alloc.ID)
			continue
		}
	}

	for oldAllocID := range existingState {
		_, ok := newState[oldAllocID]
		if !ok {
			log.WithField("alloc_id", oldAllocID).Info("removed allocation")
			removed = append(removed, oldAllocID)
		}
	}

	return addedOrUpdated, removed, newState
}

func (w *Watcher) RunOnce(ctx context.Context) error {
	allocs, err := w.fetchAllocations(ctx)
	if err != nil {
		return err
	}

	var addedOrUpdated, removed []string
	addedOrUpdated, removed, w.trackedAllocs = calculateChanges(w.trackedAllocs, allocs)

	if len(addedOrUpdated) == 0 && len(removed) == 0 {
		log.Info("No changes found for current node, backing off")
		return worker.ErrBackoff
	}

	diff := &AllocChangeSet{
		AddedOrUpdated: addedOrUpdated,
		Removed:        removed,
		NewState:       w.trackedAllocs,
	}

	w.allocCh <- diff

	return nil
}

func (w *Watcher) Run(ctx context.Context) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = w.interval
	bo.MaxInterval = w.interval * 2
	bo.Reset()

	cfg := worker.Config{
		Name: "allocwatcher",
		// Lets give a little leniency for underprovisioned Nomad servers and
		// extremely busy nodes
		MaxWorkTime:        10 * time.Second,
		NoWorkBackOff:      bo,
		MinDuration:        w.interval,
		BackoffOnAllErrors: true,
		WorkFunc:           w.RunOnce,
	}
	worker.Run(ctx, cfg)
	return nil
}

func LoadIntoSystem(
	sys *system.System,
	interval time.Duration,
	allocCh chan<- *AllocChangeSet,
	nomadClient *api.Client,
) (*Watcher, error) {
	agentInfo, err := nomadClient.Agent().Self()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch agent info: %w", err)
	}

	nodeID := agentInfo.Stats["client"]["node_id"]
	if nodeID == "" {
		return nil, fmt.Errorf("invalid node id")
	}

	w := &Watcher{
		client:        nomadClient,
		allocCh:       allocCh,
		interval:      interval,
		nodeID:        nodeID,
		trackedAllocs: make(map[string]*api.Allocation),
	}

	sys.AddService(w.Run)
	sys.AddHealthCheck(w)

	return w, nil
}
