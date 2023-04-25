package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"

	log "github.com/sirupsen/logrus"
)

var (
	ErrBackoff = errors.New("backoff")
)

type Config struct {
	// Name is the name of the worker used for logging.
	Name string

	// NoWorkBackoff is the backoff strategy to use if the WorkFunc indicates a backoff should happen
	NoWorkBackOff backoff.BackOff

	// MaxWorkTime is the duration after which the context passed to the worker will
	// timeout.
	MaxWorkTime time.Duration

	// MinDuration is the minimum duration each work loop should take.
	// This can be used to throttle a busy worker by setting the minimum period
	// between invocations.
	MinDuration time.Duration

	// WorkFunc should return ErrBackoff if it wants to back off.
	WorkFunc func(ctx context.Context) error

	// If backoff is desired for any returned error
	BackoffOnAllErrors bool

	// Override hook for testing
	waitFn func(ctx context.Context, delay time.Duration)
}

// Run a worker, which calls WorkFunc in a loop.
// Run exits when the context is cancelled.
func Run(ctx context.Context, cfg Config) {
	cfg = setDefaults(cfg)
	cfg.NoWorkBackOff.Reset()

	for ctx.Err() == nil {
		start := time.Now()

		backoff := doWork(cfg)

		// No backoff required, just make sure we wait the minimum period.
		if backoff < 0 {
			cfg.NoWorkBackOff.Reset()
			workDuration := time.Since(start)

			// If the work took longer than the minimum we can continue the loop
			if workDuration > cfg.MinDuration {
				continue
			}

			// Otherwise backoff until we meet the minimum duration
			backoff = cfg.MinDuration - workDuration
		}

		// Wait for the backoff period. (waitFn must terminate on ctx cancellation).
		cfg.waitFn(ctx, backoff)
	}
}

func setDefaults(cfg Config) Config {
	if cfg.waitFn == nil {
		cfg.waitFn = wait
	}

	if cfg.NoWorkBackOff == nil {
		cfg.NoWorkBackOff = defaultBackOff()
	}

	return cfg
}

func wait(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func defaultBackOff() backoff.BackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     time.Millisecond * 50,
		RandomizationFactor: 0.75,
		Multiplier:          2,
		MaxInterval:         time.Second * 10,
		MaxElapsedTime:      0,
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return b
}

func doWork(cfg Config) (backoff time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.MaxWorkTime)
	defer cancel()

	logger := log.WithFields(log.Fields{
		"worker": cfg.Name,
	})
	var err error
	defer func(err *error) {
		if err != nil && *err != nil {
			logger.WithError(*err).Error("error during worker loop")
		}
	}(&err)

	defer func() {
		// Handle panics so that loop worker behaves like net/http.ServeHTTP
		if r := recover(); r != nil {
			err = fmt.Errorf("panic handled: %+v", r)
		}

		// If an error occurred then calculate the correct backoff
		switch {
		case errors.Is(err, ErrBackoff):
			backoff = cfg.NoWorkBackOff.NextBackOff()
			err = nil
		case cfg.BackoffOnAllErrors && err != nil:
			backoff = cfg.NoWorkBackOff.NextBackOff()
		default:
			// Otherwise no explicit backoff - run the loop as normal.
			backoff = -1
		}

		if backoff > 0 {
			logger.WithFields(log.Fields{"backoff_ms": backoff.Milliseconds()}).Info("backing off")
		}
	}()

	err = cfg.WorkFunc(ctx)
	return backoff
}
