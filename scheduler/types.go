package scheduler

import (
	"context"
	"sync"
	"time"
)

// ScheduleTask represents a single scheduled task from the store.
// Fields match the table: ID, StartAt, NextRunAt, LastRun, IntervalMinutes, Enabled.
type ScheduleTask struct {
	ID               int
	StartAt         time.Time
	NextRunAt       time.Time
	LastRun         *time.Time
	IntervalMinutes int
	Enabled         bool
}

// TaskExecutor executes a scheduled task.
// The scheduler calls Execute for each due task.
type TaskExecutor interface {
	Execute(ctx context.Context, t ScheduleTask) error
}

// ScheduleStore provides read and update access to scheduled tasks.
// Compatible with Db.Schedule: ReadEnabled returns enabled tasks; UpdateNextRun updates after execution; Disable marks one-shot tasks done.
type ScheduleStore interface {
	ReadEnabled(ctx context.Context) ([]ScheduleTask, error)
	UpdateNextRun(ctx context.Context, id int, nextRun, lastRun time.Time) error
	// Disable marks the task as disabled (Enabled=false) and sets LastRun. Use after executing a one-shot task (IntervalMinutes <= 0).
	Disable(ctx context.Context, id int, lastRun time.Time) error
}

// Option configures a Scheduler in NewScheduler.
type Option func(*Scheduler)

// Scheduler runs due tasks at their NextRunAt using a worker pool.
type Scheduler struct {
	store    ScheduleStore
	exec     TaskExecutor
	workers  int
	resync   time.Duration
	wakeEarly time.Duration
	resetCh  chan struct{}
	tasksCh  chan runRequest

	// inFlight: task IDs currently queued or being executed. Prevents Run() from
	// re-enqueueing the same task before the worker has called Disable/UpdateNextRun.
	inFlight   map[int]struct{}
	inFlightMu sync.Mutex

	// oneShotDone: task IDs that are one-shot and have been disabled. Never re-enqueue these
	// even if ReadEnabled still returns them (e.g. store eventual consistency).
	oneShotDone   map[int]struct{}
}

// runRequest is sent to the worker pool to run one task.
type runRequest struct {
	ctx context.Context
	t   ScheduleTask
}
