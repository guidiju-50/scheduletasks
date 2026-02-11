package scheduler

import (
	"context"
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
// Compatible with Db.Schedule: ReadEnabled returns enabled tasks; UpdateNextRun updates after execution.
type ScheduleStore interface {
	ReadEnabled(ctx context.Context) ([]ScheduleTask, error)
	UpdateNextRun(ctx context.Context, id int, nextRun, lastRun time.Time) error
}

// Option configures a Scheduler in NewScheduler.
type Option func(*Scheduler)

// Scheduler runs due tasks at their NextRunAt using a worker pool.
type Scheduler struct {
	store   ScheduleStore
	exec    TaskExecutor
	workers int
	resync  time.Duration
	wakeEarly time.Duration
	resetCh chan struct{}
	tasksCh chan runRequest
}

// runRequest is sent to the worker pool to run one task.
type runRequest struct {
	ctx context.Context
	t   ScheduleTask
}
