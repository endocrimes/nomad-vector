package system

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/endocrimes/nomad-vector/internal/termination"
	"github.com/stretchr/testify/require"
)

func TestSystem_Run(t *testing.T) {
	// Wait until everything has been exercised before terminating
	terminationWait := &sync.WaitGroup{}
	terminationTestHook = func(ctx context.Context, delay time.Duration) error {
		terminationWait.Wait()
		return termination.ErrTerminated
	}

	sys := New()

	terminationWait.Add(1)
	sys.AddService(func(ctx context.Context) (err error) {
		terminationWait.Done()
		<-ctx.Done()
		return nil
	})

	sys.AddHealthCheck(newMockHealthChecker())

	var cleanupsCalled []string
	sys.AddCleanup("1", func(ctx context.Context) (err error) {
		cleanupsCalled = append(cleanupsCalled, "1")
		return nil
	})
	sys.AddCleanup("2", func(ctx context.Context) (err error) {
		cleanupsCalled = append(cleanupsCalled, "2")
		return nil
	})

	ctx := context.Background()
	err := sys.Run(ctx, 0)
	require.True(t, errors.Is(err, termination.ErrTerminated))

	sys.Cleanup(ctx)
	require.Equal(t, []string{"2", "1"}, cleanupsCalled)
}

type mockHealthChecker struct {
}

func newMockHealthChecker() *mockHealthChecker {
	return &mockHealthChecker{}
}

func (m *mockHealthChecker) HealthChecks() (name string, check HealthFunc) {
	return "name", nil
}
