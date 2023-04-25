package system

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/endocrimes/nomad-vector/internal/termination"
	log "github.com/sirupsen/logrus"
)

type HealthFunc func(ctx context.Context) error
type CleanupFunc func(ctx context.Context) error

type HealthCheckable interface {
	HealthChecks() (name string, check HealthFunc)
}

type cleanupEntry struct {
	name string
	fn   CleanupFunc
}

// System encapsulates the "business" work that a service needs to perform.
// This is done in the form of a set of long-running services (an API server,
// event loops, etc), and coordinates their health checks and termination work.
type System struct {
	services     []func(context.Context) error
	healthChecks []HealthCheckable
	cleanups     []cleanupEntry
}

// New create a new, empty system.
func New() *System {
	return &System{}
}

// terminationTestHook is defined purely for internal testing.
var terminationTestHook = termination.Handle

// Run runs any services added to the system. The provided context should be
// configured with an observability provider.
//
// Run is blocking and will only return when all it's services have finished.
// The error returned will be the first error returned from any of the services.
// The terminationDelay passed in is the amount of time to wait between receiving a
// signal and cancelling the system context
func (r *System) Run(ctx context.Context, terminationDelay time.Duration) error {
	var err error
	log.Info("starting system")
	defer func(err *error) {
		if err != nil && *err != nil {
			log.WithError(*err).Error("error running system")
		}
	}(&err)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return terminationTestHook(ctx, terminationDelay)
	})

	for _, f := range r.services {
		// Capture the fn, so we don't overwrite it when starting in parallel.
		f := f
		g.Go(func() error {
			return f(ctx)
		})
	}

	return g.Wait()
}

func (r *System) AddService(s func(ctx context.Context) error) {
	r.services = append(r.services, s)
}

func (r *System) AddHealthCheck(h HealthCheckable) {
	r.healthChecks = append(r.healthChecks, h)
}

func (r *System) AddCleanup(name string, c CleanupFunc) {
	r.cleanups = append(r.cleanups, cleanupEntry{name: name, fn: c})
}

func (r *System) HealthChecks() []HealthCheckable {
	return r.healthChecks
}

func (r *System) Cleanup(ctx context.Context) {
	log.Info("cleaning up system")

	inReverseOrder(r.cleanups, func(e cleanupEntry) {
		err := e.fn(ctx)
		if err != nil {
			log.WithField("component_name", e.name).WithError(err).Error("error cleaning up component")
		} else {
			log.WithField("component_name", e.name).Info("cleaning up component")
		}
	})
}

func inReverseOrder[L ~[]E, E any](list L, eachFunc func(v E)) {
	for idx := len(list) - 1; idx >= 0; idx-- {
		eachFunc(list[idx])
	}
}
