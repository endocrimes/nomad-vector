package termination

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

// ErrTerminated is used to indicate that the Handle func received a shutdown signal.
var ErrTerminated = errors.New("terminated")

// Handle is function that will return ErrTerminated when a signal is received.
// If the context is cancelled it will return nil.
// It is intended to be used with a x/sync/errgroup.WithContext.
func Handle(ctx context.Context, delay time.Duration) error {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-quit:
		log.WithFields(log.Fields{"signal": s, "delay": delay}).Info("received shutdown signal")
		time.Sleep(delay)
		return ErrTerminated
	case <-ctx.Done():
		log.Info("shutdown context cancelled")
		return nil
	}
}
