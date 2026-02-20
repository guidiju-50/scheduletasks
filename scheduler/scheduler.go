package scheduler

import (
	"context"
	"log"
	"sync"
	"time"
)

const (
	defaultWorkers     = 5
	defaultResync      = 30 * time.Second
	defaultWakeupEarly = 200 * time.Millisecond
	noTasksSleep       = 1 * time.Minute
)

// WithWorkers sets the number of worker goroutines (default 5).
func WithWorkers(n int) Option {
	return func(s *Scheduler) {
		s.workers = n
	}
}

// WithResyncInterval sets how often to re-read the schedule (default 30s).
func WithResyncInterval(d time.Duration) Option {
	return func(s *Scheduler) {
		s.resync = d
	}
}

// WithWakeupEarly sets how much before the next run to wake (default 200ms).
func WithWakeupEarly(d time.Duration) Option {
	return func(s *Scheduler) {
		s.wakeEarly = d
	}
}

// NewScheduler builds a scheduler that uses store for reads/updates and exec to run tasks.
func NewScheduler(store ScheduleStore, exec TaskExecutor, opts ...Option) *Scheduler {
	s := &Scheduler{
		store:      store,
		exec:       exec,
		workers:    defaultWorkers,
		resync:     defaultResync,
		wakeEarly:  defaultWakeupEarly,
		resetCh:     make(chan struct{}, 1),
		inFlight:    make(map[int]struct{}),
		oneShotDone: make(map[int]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	// Buffered channel: allow enqueueing all due tasks without blocking; workers drain it.
	capacity := s.workers * 2
	if capacity < 16 {
		capacity = 16
	}
	s.tasksCh = make(chan runRequest, capacity)
	return s
}

// tryClaim marks task id as in-flight (queued or running). Returns true if we claimed it,
// false if it was already claimed or is a one-shot that was already disabled (prevents duplicate enqueue).
func (s *Scheduler) tryClaim(id int) bool {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	if _, ok := s.oneShotDone[id]; ok {
		return false
	}
	if _, ok := s.inFlight[id]; ok {
		return false
	}
	s.inFlight[id] = struct{}{}
	return true
}

// release removes task id from in-flight so it can be picked again (recurrent at next run, or one-shot retry on error).
func (s *Scheduler) release(id int) {
	s.inFlightMu.Lock()
	delete(s.inFlight, id)
	s.inFlightMu.Unlock()
}

// releaseAsOneShotDone removes id from in-flight and marks it as permanently done (one-shot disabled).
// Run() will never re-enqueue this id even if the store still returns it.
func (s *Scheduler) releaseAsOneShotDone(id int) {
	s.inFlightMu.Lock()
	delete(s.inFlight, id)
	s.oneShotDone[id] = struct{}{}
	s.inFlightMu.Unlock()
}

// Run starts the main loop: sleep until the next run time, execute due tasks via worker pool, repeat.
// It returns when ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	log.Printf("scheduler: started (workers=%d, resync=%v)", s.workers, s.resync)

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.worker()
		}()
	}

	ticker := time.NewTicker(s.resync)
	defer ticker.Stop()

	for {
		tasks, err := s.store.ReadEnabled(ctx)
		if err != nil {
			log.Printf("scheduler: ReadEnabled error: %v", err)
			// Sleep and retry
			timer := time.NewTimer(noTasksSleep)
			select {
			case <-ctx.Done():
				timer.Stop()
				close(s.tasksCh)
				wg.Wait()
				log.Printf("scheduler: stopped")
				return
			case <-s.resetCh:
				timer.Stop()
				continue
			case <-ticker.C:
				timer.Stop()
				continue
			case <-timer.C:
				continue
			}
		}

		// Only consider enabled (store may already filter; be safe)
		var enabled []ScheduleTask
		for _, t := range tasks {
			if t.Enabled {
				enabled = append(enabled, t)
			}
		}

		now := time.Now()
		var minNext time.Time
		var hasNext bool
		for _, t := range enabled {
			runAt := t.NextRunAt
			if runAt.IsZero() {
				runAt = t.StartAt
			}
			if !hasNext || runAt.Before(minNext) {
				minNext = runAt
				hasNext = true
			}
		}

		var timer *time.Timer
		if !hasNext {
			timer = time.NewTimer(noTasksSleep)
		} else {
			wakeAt := minNext.Add(-s.wakeEarly)
			if wakeAt.Before(now) {
				wakeAt = now
			}
			d := time.Until(wakeAt)
			if d < 0 {
				d = 0
			}
			timer = time.NewTimer(d)
		}

		select {
		case <-ctx.Done():
			timer.Stop()
			close(s.tasksCh)
			wg.Wait()
			log.Printf("scheduler: stopped")
			return
		case <-s.resetCh:
			timer.Stop()
			log.Printf("scheduler: schedule reset")
			continue
		case <-ticker.C:
			timer.Stop()
			continue
		case <-timer.C:
			timer.Stop()
		}

		// Re-read and run all due tasks
		tasks, err = s.store.ReadEnabled(ctx)
		if err != nil {
			log.Printf("scheduler: ReadEnabled error (after wake): %v", err)
			continue
		}
		now = time.Now()
		var due []ScheduleTask
		for _, t := range tasks {
			runAt := t.NextRunAt
			if runAt.IsZero() {
				runAt = t.StartAt
			}
			if t.Enabled && !runAt.After(now) {
				due = append(due, t)
			}
		}
		if len(due) == 0 {
			continue
		}
		log.Printf("scheduler: selected %d tasks due", len(due))
		for _, t := range due {
			// Claim before enqueue so the same task is not re-enqueued by a later Run() cycle
			// before the worker has called Disable/UpdateNextRun.
			if !s.tryClaim(t.ID) {
				log.Printf("scheduler: task id=%d already in-flight, skipping reenqueue", t.ID)
				continue
			}
			log.Printf("scheduler: claimed task id=%d, enqueueing", t.ID)
			select {
			case <-ctx.Done():
				s.release(t.ID)
				close(s.tasksCh)
				wg.Wait()
				log.Printf("scheduler: stopped")
				return
			case s.tasksCh <- runRequest{ctx: ctx, t: t}:
				// sent; worker will release on completion
			}
		}
	}
}

// worker reads from tasksCh, executes the task, then updates the store (UpdateNextRun for recurrent, Disable for one-shot).
// On Execute error: we release(id) so the task can be retried next cycle (we do NOT call Disable for one-shot).
// On success: recurrent -> UpdateNextRun then release(id); one-shot -> Disable then releaseAsOneShotDone(id) so it is never re-enqueued.
func (s *Scheduler) worker() {
	for req := range s.tasksCh {
		id := req.t.ID
		log.Printf("scheduler: in-flight task id=%d", id)

		if err := s.exec.Execute(req.ctx, req.t); err != nil {
			log.Printf("scheduler: Execute task id=%d error: %v (released; will retry next cycle if still enabled)", id, err)
			s.release(id)
			continue
		}
		now := time.Now()
		lastRun := now
		if req.t.IntervalMinutes > 0 {
			interval := time.Duration(req.t.IntervalMinutes) * time.Minute
			nextRun := now.Add(interval)
			for nextRun.Before(now) || nextRun.Equal(now) {
				nextRun = nextRun.Add(interval)
			}
			log.Printf("scheduler: executando task recorrente id=%d; next_run_at=%s", id, nextRun.Format(time.RFC3339))
			if err := s.store.UpdateNextRun(req.ctx, id, nextRun, lastRun); err != nil {
				log.Printf("scheduler: UpdateNextRun id=%d error: %v", id, err)
			}
		} else {
			log.Printf("scheduler: executando task one-shot id=%d; desativando", id)
			if err := s.store.Disable(req.ctx, id, lastRun); err != nil {
				log.Printf("scheduler: Disable id=%d error: %v", id, err)
			}
			s.releaseAsOneShotDone(id)
			log.Printf("scheduler: done task id=%d (one-shot, no reenqueue)", id)
			continue
		}
		log.Printf("scheduler: done task id=%d", id)
		s.release(id)
	}
}

// NotifyScheduleChanged signals the scheduler to re-read the schedule immediately (non-blocking).
func (s *Scheduler) NotifyScheduleChanged() {
	select {
	case s.resetCh <- struct{}{}:
	default:
		// already pending reset
	}
}
