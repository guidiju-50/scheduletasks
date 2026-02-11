package scheduler_test

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"scheduletasks/scheduler"
)

// stubStore implements ScheduleStore for the example (e.g. replace with Db.Schedule in production).
type stubStore struct {
	mu     sync.Mutex
	tasks  []scheduler.ScheduleTask
	called int
}

func (s *stubStore) ReadEnabled(ctx context.Context) ([]scheduler.ScheduleTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.called++
	// First call: one task due in 1 second. Later calls: same or empty.
	out := make([]scheduler.ScheduleTask, len(s.tasks))
	copy(out, s.tasks)
	return out, nil
}

func (s *stubStore) UpdateNextRun(ctx context.Context, id int, nextRun, lastRun time.Time) error {
	log.Printf("stubStore: UpdateNextRun id=%d nextRun=%v lastRun=%v", id, nextRun, lastRun)
	return nil
}

// exampleExecutor implements TaskExecutor for the example.
type exampleExecutor struct{}

func (e *exampleExecutor) Execute(ctx context.Context, t scheduler.ScheduleTask) error {
	log.Printf("exampleExecutor: executed task id=%d", t.ID)
	return nil
}

// TestExampleRuns verifies the example compiles and the scheduler can be constructed and run briefly.
func TestExampleRuns(t *testing.T) {
	store := &stubStore{
		tasks: []scheduler.ScheduleTask{{
			ID:               1,
			StartAt:          time.Now(),
			NextRunAt:        time.Now().Add(1 * time.Second),
			LastRun:          nil,
			IntervalMinutes:  1,
			Enabled:          true,
		}},
	}
	exec := &exampleExecutor{}
	s := scheduler.NewScheduler(store, exec)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(100 * time.Millisecond)
	s.NotifyScheduleChanged()
	cancel()
	time.Sleep(150 * time.Millisecond)
}

// ExampleNewScheduler demonstrates the public API: NewScheduler(store, exec), Run(ctx), NotifyScheduleChanged().
func ExampleNewScheduler() {
	store := &stubStore{
		tasks: []scheduler.ScheduleTask{{
			ID:               1,
			StartAt:          time.Now(),
			NextRunAt:        time.Now().Add(1 * time.Second),
			LastRun:          nil,
			IntervalMinutes:  1,
			Enabled:          true,
		}},
	}
	exec := &exampleExecutor{}
	s := scheduler.NewScheduler(store, exec)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)

	time.Sleep(3 * time.Second)
	s.NotifyScheduleChanged()
	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(200 * time.Millisecond)
	// Output is non-deterministic; this example only shows usage.
}
