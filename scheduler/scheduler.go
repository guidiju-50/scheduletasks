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
		store:    store,
		exec:     exec,
		workers:  defaultWorkers,
		resync:   defaultResync,
		wakeEarly: defaultWakeupEarly,
		resetCh:  make(chan struct{}, 1),
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
			s.worker(ctx)
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
			if !hasNext || t.NextRunAt.Before(minNext) {
				minNext = t.NextRunAt
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
			if t.Enabled && !t.NextRunAt.After(now) {
				due = append(due, t)
			}
		}
		if len(due) == 0 {
			continue
		}
		log.Printf("scheduler: running %d due task(s)", len(due))
		for _, t := range due {
			select {
			case <-ctx.Done():
				close(s.tasksCh)
				wg.Wait()
				log.Printf("scheduler: stopped")
				return
			case s.tasksCh <- runRequest{ctx: ctx, t: t}:
				// sent
			}
		}
	}
}

// worker reads from tasksCh, executes the task, then updates NextRunAt/LastRun via the store.
func (s *Scheduler) worker(ctx context.Context) {
	for req := range s.tasksCh {
		if err := s.exec.Execute(req.ctx, req.t); err != nil {
			log.Printf("scheduler: Execute task id=%d error: %v", req.t.ID, err)
		}
		now := time.Now()
		interval := time.Duration(req.t.IntervalMinutes) * time.Minute
		lastRun := now
		nextRun := now.Add(interval)
		for nextRun.Before(now) || nextRun.Equal(now) {
			nextRun = nextRun.Add(interval)
		}
		if err := s.store.UpdateNextRun(req.ctx, req.t.ID, nextRun, lastRun); err != nil {
			log.Printf("scheduler: UpdateNextRun id=%d error: %v", req.t.ID, err)
		}
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
