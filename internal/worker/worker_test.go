package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRun_SleepsOnError(t *testing.T) {
	t.Parallel()
	tt := []struct {
		name              string
		waitAnyError      bool
		errToReturn       error
		expectedWaitCalls int
	}{
		{
			name:              "when BackoffOnAllErrors is true and a random error is returned",
			waitAnyError:      true,
			errToReturn:       errors.New("oops"),
			expectedWaitCalls: 10,
		},
		{
			name:              "when BackoffOnAllErrors is true and backoff is returned",
			waitAnyError:      true,
			errToReturn:       ErrBackoff,
			expectedWaitCalls: 10,
		},
		{
			name:              "when BackoffOnAllErrors is false and a random error is returned",
			waitAnyError:      false,
			errToReturn:       errors.New("oops"),
			expectedWaitCalls: 0,
		},
		{
			name:              "when BackoffOnAllErrors is false and backoff is returned",
			waitAnyError:      false,
			errToReturn:       ErrBackoff,
			expectedWaitCalls: 10,
		},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			counter := 0
			expected := 10
			f := func(ctx context.Context) error {
				counter++
				if counter == expected {
					cancel()
				}
				return tc.errToReturn
			}

			waitCalls := 0
			waitFn := func(_ context.Context, delay time.Duration) {
				waitCalls++
			}

			backOff := new(fakeBackOff)
			Run(ctx, Config{
				Name:               t.Name(),
				BackoffOnAllErrors: tc.waitAnyError,

				NoWorkBackOff: backOff,
				WorkFunc:      f,
				waitFn:        waitFn,
			})

			require.Equal(t, tc.expectedWaitCalls, backOff.nextCallCount)
			require.Equal(t, tc.expectedWaitCalls, waitCalls)
		})
	}
}

func TestRun_SleepsAfterNoWorkCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	counter := 0
	expected := 10
	f := func(ctx context.Context) error {
		counter++
		if counter == expected {
			cancel()
		}
		return ErrBackoff
	}

	waitCalls := 0
	waitFn := func(_ context.Context, delay time.Duration) {
		waitCalls++
	}

	backOff := new(fakeBackOff)
	Run(ctx, Config{
		Name:          "sleep-after-no-work",
		NoWorkBackOff: backOff,
		WorkFunc:      f,
		waitFn:        waitFn,
	})

	require.Equal(t, expected, backOff.nextCallCount)
	require.Equal(t, expected, waitCalls)
	require.Equal(t, 1, backOff.resetCallCount, "reset should only be to initialize")
}

func TestRun_DoesNotSleepAfterWorkCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	counter := 0
	expected := 3
	f := func(ctx context.Context) error {
		counter++
		if counter == expected {
			cancel()
		}
		return nil
	}

	waitFn := func(_ context.Context, delay time.Duration) {
		panic("wait should never be called")
	}

	backOff := new(fakeBackOff)
	Run(ctx, Config{
		Name:          "does-not-sleep-after-work-cycle",
		NoWorkBackOff: backOff,
		WorkFunc:      f,
		waitFn:        waitFn,
	})

	require.Equal(t, 0, backOff.nextCallCount)
	require.Equal(t, expected+1, backOff.resetCallCount)
}

func TestRun_MinWorkTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	counter := 0
	expected := 3

	f := func(ctx context.Context) error {
		counter++
		if counter == expected {
			cancel()
		}
		// Tight loop of cheap work with a large backlog
		return nil
	}

	waitCallCount := 0
	backOff := new(fakeBackOff)
	Run(ctx, Config{
		Name:          "does-not-tight-loop",
		NoWorkBackOff: backOff,
		MinDuration:   time.Millisecond * 10,
		WorkFunc:      f,
		waitFn: func(ctx context.Context, delay time.Duration) {
			waitCallCount++
		},
	})

	// Confirm normal backoff never called
	require.Equal(t, 0, backOff.nextCallCount)
	// Reset is called once to initialize the backOff
	require.Equal(t, expected+1, backOff.resetCallCount)
	// Check that the wait was called expected times since the MinWorkTime should
	// have meant we called the wait func.
	require.Equal(t, expected, waitCallCount)
}

func Test_doWork_WorkFuncPanics_IsRecovered(t *testing.T) {
	f := func(ctx context.Context) error {
		panic("Oops")
	}

	cfg := Config{
		Name:     "work-func-panics",
		WorkFunc: f,
	}

	// We don't backoff on a panic, so should get -1
	require.True(t, doWork(cfg) < 0)
}

type fakeBackOff struct {
	nextBackOff    time.Duration
	nextCallCount  int
	resetCallCount int
}

func (b *fakeBackOff) NextBackOff() time.Duration {
	b.nextCallCount++
	return b.nextBackOff
}

func (b *fakeBackOff) Reset() {
	b.resetCallCount++
}
