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
	mu                sync.Mutex
	tasks             []scheduler.ScheduleTask
	called            int
	disableCalls      []int
	updateNextRunCalls []int
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
	s.mu.Lock()
	s.updateNextRunCalls = append(s.updateNextRunCalls, id)
	s.mu.Unlock()
	log.Printf("stubStore: UpdateNextRun id=%d nextRun=%v lastRun=%v", id, nextRun, lastRun)
	return nil
}

func (s *stubStore) Disable(ctx context.Context, id int, lastRun time.Time) error {
	s.mu.Lock()
	s.disableCalls = append(s.disableCalls, id)
	s.mu.Unlock()
	log.Printf("stubStore: Disable id=%d lastRun=%v", id, lastRun)
	return nil
}

// mutableStubStore is a store that, when Disable(id) is called, sets that task's Enabled to false
// so ReadEnabled stops returning it (simulates production DB). Used to assert no duplicate execution.
type mutableStubStore struct {
	mu                sync.Mutex
	tasks             []scheduler.ScheduleTask
	disableCalls      []int
	updateNextRunCalls []int
}

func (m *mutableStubStore) ReadEnabled(ctx context.Context) ([]scheduler.ScheduleTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []scheduler.ScheduleTask
	for _, t := range m.tasks {
		if t.Enabled {
			out = append(out, t)
		}
	}
	return out, nil
}

func (m *mutableStubStore) UpdateNextRun(ctx context.Context, id int, nextRun, lastRun time.Time) error {
	m.mu.Lock()
	m.updateNextRunCalls = append(m.updateNextRunCalls, id)
	m.mu.Unlock()
	return nil
}

func (m *mutableStubStore) Disable(ctx context.Context, id int, lastRun time.Time) error {
	m.mu.Lock()
	m.disableCalls = append(m.disableCalls, id)
	for i := range m.tasks {
		if m.tasks[i].ID == id {
			m.tasks[i].Enabled = false
			break
		}
	}
	m.mu.Unlock()
	return nil
}

// countingExecutor counts Execute calls per task ID for tests.
type countingExecutor struct {
	mu    sync.Mutex
	calls map[int]int
}

func (e *countingExecutor) Execute(ctx context.Context, t scheduler.ScheduleTask) error {
	e.mu.Lock()
	e.calls[t.ID]++
	e.mu.Unlock()
	return nil
}

// failingExecutor fails the first N executions for a given ID, then succeeds.
type failingExecutor struct {
	mu      sync.Mutex
	failFor map[int]int // id -> count of failures left
}

func (e *failingExecutor) Execute(ctx context.Context, t scheduler.ScheduleTask) error {
	e.mu.Lock()
	n := e.failFor[t.ID]
	if n > 0 {
		e.failFor[t.ID] = n - 1
		e.mu.Unlock()
		return context.DeadlineExceeded
	}
	e.mu.Unlock()
	return nil
}

// exampleExecutor implements TaskExecutor for the example.
type exampleExecutor struct{}

func (e *exampleExecutor) Execute(ctx context.Context, t scheduler.ScheduleTask) error {
	log.Printf("exampleExecutor: executed task id=%d", t.ID)
	return nil
}

// TestOneShotCallsDisable verifies that a task with IntervalMinutes <= 0 triggers Disable and not UpdateNextRun.
func TestOneShotCallsDisable(t *testing.T) {
	store := &stubStore{
		tasks: []scheduler.ScheduleTask{{
			ID:               10,
			StartAt:          time.Now().Add(-time.Hour),
			NextRunAt:        time.Now().Add(-time.Second),
			LastRun:          nil,
			IntervalMinutes:  0,
			Enabled:          true,
		}},
	}
	exec := &exampleExecutor{}
	s := scheduler.NewScheduler(store, exec)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(2 * time.Second)
	cancel()
	time.Sleep(300 * time.Millisecond)
	store.mu.Lock()
	disableCalls := append([]int(nil), store.disableCalls...)
	updateCalls := append([]int(nil), store.updateNextRunCalls...)
	store.mu.Unlock()
	if len(disableCalls) == 0 {
		t.Error("expected Disable to be called for one-shot task")
	}
	for _, id := range disableCalls {
		if id != 10 {
			t.Errorf("Disable called with id=%d, want 10", id)
		}
	}
	for _, id := range updateCalls {
		if id == 10 {
			t.Error("UpdateNextRun should not be called for one-shot task id=10")
		}
	}
}

// TestRecurrentCallsUpdateNextRun verifies that a task with IntervalMinutes > 0 triggers UpdateNextRun and not Disable.
func TestRecurrentCallsUpdateNextRun(t *testing.T) {
	store := &stubStore{
		tasks: []scheduler.ScheduleTask{{
			ID:               20,
			StartAt:          time.Now().Add(-time.Hour),
			NextRunAt:        time.Now().Add(-time.Second),
			LastRun:          nil,
			IntervalMinutes:  5,
			Enabled:          true,
		}},
	}
	exec := &exampleExecutor{}
	s := scheduler.NewScheduler(store, exec)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(2 * time.Second)
	cancel()
	time.Sleep(300 * time.Millisecond)
	store.mu.Lock()
	disableCalls := append([]int(nil), store.disableCalls...)
	updateCalls := append([]int(nil), store.updateNextRunCalls...)
	store.mu.Unlock()
	if len(updateCalls) == 0 {
		t.Error("expected UpdateNextRun to be called for recurrent task")
	}
	for _, id := range updateCalls {
		if id != 20 {
			t.Errorf("UpdateNextRun called with id=%d, want 20", id)
		}
	}
	for _, id := range disableCalls {
		if id == 20 {
			t.Error("Disable should not be called for recurrent task id=20")
		}
	}
}

// TestOneShotExecutesOnceWithResync ensures a one-shot task runs exactly once even when
// Run() resyncs multiple times (task stays "due" until Disable is applied in the store).
func TestOneShotExecutesOnceWithResync(t *testing.T) {
	store := &mutableStubStore{
		tasks: []scheduler.ScheduleTask{{
			ID:               10,
			StartAt:          time.Now().Add(-time.Hour),
			NextRunAt:        time.Now().Add(-time.Second),
			LastRun:          nil,
			IntervalMinutes:  0,
			Enabled:          true,
		}},
	}
	exec := &countingExecutor{calls: make(map[int]int)}
	s := scheduler.NewScheduler(store, exec,
		scheduler.WithWorkers(2),
		scheduler.WithResyncInterval(50*time.Millisecond),
	)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(200 * time.Millisecond)

	exec.mu.Lock()
	count := exec.calls[10]
	exec.mu.Unlock()
	if count != 1 {
		t.Errorf("one-shot task Execute count = %d, want 1", count)
	}
	store.mu.Lock()
	disableCount := len(store.disableCalls)
	store.mu.Unlock()
	if disableCount != 1 {
		t.Errorf("Disable call count = %d, want 1", disableCount)
	}
}

// TestOneShotDisableCalledOnce asserts Disable is invoked exactly once for a one-shot task.
func TestOneShotDisableCalledOnce(t *testing.T) {
	store := &mutableStubStore{
		tasks: []scheduler.ScheduleTask{{
			ID:               11,
			StartAt:          time.Now().Add(-time.Hour),
			NextRunAt:        time.Now(),
			LastRun:          nil,
			IntervalMinutes:  0,
			Enabled:          true,
		}},
	}
	exec := &exampleExecutor{}
	s := scheduler.NewScheduler(store, exec, scheduler.WithResyncInterval(30*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(400 * time.Millisecond)
	cancel()
	time.Sleep(300 * time.Millisecond)

	store.mu.Lock()
	disableCalls := append([]int(nil), store.disableCalls...)
	store.mu.Unlock()
	if len(disableCalls) != 1 {
		t.Errorf("Disable called %d times, want 1: %v", len(disableCalls), disableCalls)
	}
	if len(disableCalls) > 0 && disableCalls[0] != 11 {
		t.Errorf("Disable called with id=%d, want 11", disableCalls[0])
	}
}

// TestOneShotExecuteErrorRetries documents behavior: on Execute error we release the task
// so it can be retried next cycle; we do NOT call Disable. So one-shot will retry until success.
func TestOneShotExecuteErrorRetries(t *testing.T) {
	store := &mutableStubStore{
		tasks: []scheduler.ScheduleTask{{
			ID:               12,
			StartAt:          time.Now().Add(-time.Hour),
			NextRunAt:        time.Now(),
			LastRun:          nil,
			IntervalMinutes:  0,
			Enabled:          true,
		}},
	}
	exec := &failingExecutor{failFor: map[int]int{12: 2}}
	s := scheduler.NewScheduler(store, exec, scheduler.WithResyncInterval(40*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	time.Sleep(600 * time.Millisecond)
	cancel()
	time.Sleep(300 * time.Millisecond)

	store.mu.Lock()
	disableCalls := append([]int(nil), store.disableCalls...)
	store.mu.Unlock()
	if len(disableCalls) != 1 {
		t.Errorf("Disable should be called once after eventual success, got %d", len(disableCalls))
	}
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

// ScheduleNotifier permite notificar o scheduler para re-ler a agenda (ex.: após CRUD noutro pacote).
type ScheduleNotifier interface {
	NotifyScheduleChanged()
}

// App guarda o scheduler numa struct partilhada; outros componentes chamam app.NotifySchedule().
type App struct {
	Sched *scheduler.Scheduler
}

// NotifySchedule re-lê a agenda; pode ser chamado por handlers ou outro código que receba *App.
func (a *App) NotifySchedule() {
	a.Sched.NotifyScheduleChanged()
}

// Example_schedulerInSharedStruct mostra o scheduler guardado numa struct partilhada.
func Example_schedulerInSharedStruct() {
	store := &stubStore{
		tasks: []scheduler.ScheduleTask{{
			ID: 1, StartAt: time.Now(), NextRunAt: time.Now().Add(1 * time.Hour),
			IntervalMinutes: 1, Enabled: true,
		}},
	}
	exec := &exampleExecutor{}
	s := scheduler.NewScheduler(store, exec)

	app := &App{Sched: s}

	ctx, cancel := context.WithCancel(context.Background())
	go app.Sched.Run(ctx)

	// Simula outro ficheiro ou handler que tem acesso a app:
	app.NotifySchedule()

	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(150 * time.Millisecond)
}

// Handler recebe o notifier por dependência e chama NotifyScheduleChanged quando precisar.
func Example_schedulerPassedAsDependency() {
	store := &stubStore{
		tasks: []scheduler.ScheduleTask{{
			ID: 1, StartAt: time.Now(), NextRunAt: time.Now().Add(1 * time.Hour),
			IntervalMinutes: 1, Enabled: true,
		}},
	}
	exec := &exampleExecutor{}
	s := scheduler.NewScheduler(store, exec)

	// Quem notifica recebe a interface (ou *scheduler.Scheduler).
	var notifier ScheduleNotifier = s

	// “Outro pacote”: recebe notifier na inicialização e usa quando a agenda mudar.
	onScheduleUpdated := func() { notifier.NotifyScheduleChanged() }

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	onScheduleUpdated()
	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(150 * time.Millisecond)
}
