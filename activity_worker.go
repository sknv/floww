package floww

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

//
// Activities
//

// ActivityStatus represents the status of an activity.
type ActivityStatus string

const (
	ActivityStatusPending   ActivityStatus = "pending"
	ActivityStatusRunning   ActivityStatus = "running"
	ActivityStatusCompleted ActivityStatus = "completed"
	ActivityStatusFailed    ActivityStatus = "failed"
)

// ActivityRecord represents an activity task.
type ActivityRecord struct {
	ID                 uuid.UUID
	IdempotencyKey     uuid.UUID
	WorkflowID         uuid.UUID
	Name               string
	Input              []byte
	Output             []byte
	Status             ActivityStatus
	Priority           int
	Attempts           uint
	MaxAttempts        uint
	StuckTimeoutMillis uint64
	ScheduledAt        time.Time
	RunAt              *time.Time
	StuckAt            *time.Time
	CompletedAt        *time.Time
	ErrorMessage       *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// ToActivityRun converts the activity record into an ActivityRun suitable for handler execution.
func (a *ActivityRecord) ToActivityRun(decoder Decoder) ActivityRun {
	return NewActivityRun(a.ID, a.Input, decoder)
}

// DefaultActivityWorkerConfig returns a default configuration for activity worker.
func DefaultActivityWorkerConfig() *WorkerConfig {
	//nolint:mnd // default values
	return &WorkerConfig{
		Poll: PollConfig{
			BatchSize:    10,
			Concurrency:  10,
			PollInterval: time.Second,
		},
		Processing: ProcessingConfig{
			DbTimeout:      time.Second * 10,
			DefaultBackoff: time.Second * 30,
		},
		ColdCleanup: CleanupConfig{
			DbTimeout:         time.Second * 30,
			RetentionInterval: time.Hour * 24 * 7,
			CleanupBatchSize:  10_000,
		},
		DeadCleanup: CleanupConfig{
			DbTimeout:         time.Second * 30,
			RetentionInterval: time.Hour * 24 * 90,
			CleanupBatchSize:  10_000,
		},
	}
}

// ActivityWorker polls storage for pending activities and dispatches them to registered handlers.
type ActivityWorker struct {
	storage  Storage
	registry *ActivityRegistry
	config   *WorkerConfig

	wg      sync.WaitGroup
	stopped chan struct{}
}

// NewActivityWorker creates a new ActivityWorker with the given storage, registry, and configuration.
func NewActivityWorker(
	storage Storage,
	registry *ActivityRegistry,
	config *WorkerConfig,
) *ActivityWorker {
	worker := &ActivityWorker{
		storage:  storage,
		registry: registry,
		config:   config,

		wg:      sync.WaitGroup{},
		stopped: make(chan struct{}),
	}

	return worker
}

// Start launches the activity polling loop in the background.
// If optional activities argument provided worker will process only specified activities.
func (w *ActivityWorker) Start(ctx context.Context, activities ...string) {
	// Start handler worker
	w.wg.Go(func() {
		// Unlink original context cancellation to gracefully stop the worker later
		workerCtx := context.WithoutCancel(ctx)

		w.runHandlerWorker(workerCtx, activities)
	})
}

// Stop signals the worker to stop and waits until it finishes or the context expires.
// It waits for the polling loop AND all in-flight activity goroutines to complete.
func (w *ActivityWorker) Stop(ctx context.Context) error {
	close(w.stopped) // stop signal

	// Try to wait for graceful shutdown
	done := make(chan struct{})

	go func() {
		w.wg.Wait()

		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context done: %w", ctx.Err())
	}
}

// runHandlerWorker is the main polling loop. It maintains a semaphore of size Concurrency.
//
// On every tick it checks how many slots are free and fetches up to that many activities
// (capped at BatchSize). Each activity is dispatched to its own goroutine immediately;
// the goroutine releases its slot when done so the next tick can fill it again.
//
// This means a slow activity never blocks faster ones from being picked up: as soon as any
// goroutine finishes, its slot becomes available for new work on the very next poll.
func (w *ActivityWorker) runHandlerWorker(ctx context.Context, activities []string) {
	// sem is a counting semaphore. Sending acquires a slot; receiving releases it.
	// Its capacity is the maximum number of concurrently running activity goroutines.
	sem := make(chan struct{}, w.config.Poll.Concurrency)

	ticker := time.NewTicker(w.config.Poll.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Force stop (should never happen though)
			return
		case <-w.stopped:
			// Graceful stop — in-flight goroutines are tracked by w.wg and will be
			// awaited by Stop() after this function returns.
			return
		case <-ticker.C:
			// Keep fetching as long as there are free slots and pending work.
			for {
				if isDrained := w.processActivities(ctx, activities, sem); isDrained {
					break // no more activities for now, wait for the next timer tick
				}

				// There are more activities to process, handle them immediately.
				// But first check if the worker has been stopped during processing.
				select {
				case <-w.stopped:
					return
				default:
				}
			}
		}
	}
}

// processActivities fetches batch of tasks for the specified activities from db respecting semaphore slots
// and routes them to handlers.
// Returns a flag that a queue is drained.
func (w *ActivityWorker) processActivities(
	ctx context.Context,
	activities []string,
	sem chan struct{},
) bool {
	// How many goroutine slots are currently available?
	freeSlots := uint(cap(sem) - len(sem)) //nolint:gosec // unsigned size
	if freeSlots == 0 {
		return true // all slots busy; wait for the next tick
	}

	// Fetch only as many activities as we have room for, up to BatchSize.
	fetchSize := min(freeSlots, w.config.Poll.BatchSize)

	tasks, err := w.fetchActivities(ctx, activities, fetchSize)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "Failed to fetch activities",
			slog.String("component", "floww"),
			slog.String("error", err.Error()),
		)

		return true
	}

	if len(tasks) == 0 {
		return true // queue is empty; wait for the next tick
	}

	// Dispatch each task to its own goroutine immediately.
	// We already verified there are enough free slots, so the send won't block.
	for i := range tasks {
		sem <- struct{}{} // acquire slot

		activity := &tasks[i]

		w.wg.Go(func() {
			defer func() { <-sem }() // release slot when done

			if actErr := w.handleActivity(ctx, activity); actErr != nil {
				slog.LogAttrs(ctx, slog.LevelError, "Failed to handle an activity",
					slog.String("component", "floww"),
					slog.String("activity_id", activity.ID.String()),
					slog.String("error", actErr.Error()),
				)
			}
		})
	}

	// If the storage returned fewer activities than we asked for, the queue is
	// drained for now — no point querying again this tick.
	return uint(len(tasks)) < fetchSize
}

// fetchActivities fetches up to fetchSize activities from storage.
func (w *ActivityWorker) fetchActivities(
	ctx context.Context,
	activities []string,
	fetchSize uint,
) ([]ActivityRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	tasks, err := w.storage.ListActiveActivities(ctx, activities, fetchSize)
	if err != nil {
		return nil, fmt.Errorf("list active activities from storage: %w", err)
	}

	return tasks, nil
}

// handleActivity processes the provided activity by handing it to the corresponding handler
// and finishes it depending on the handling result.
func (w *ActivityWorker) handleActivity(ctx context.Context, activity *ActivityRecord) error {
	handler, exists := w.registry.handlers[activity.Name]
	if !exists {
		slog.LogAttrs(ctx, slog.LevelError, "No handler registered for the activity, a task will be rescheduled",
			slog.String("component", "floww"),
			slog.String("activity", activity.Name),
			slog.String("activity_id", activity.ID.String()),
		)

		return w.handleActivityError(
			ctx,
			&activityHandlerWrapper{}, //nolint:exhaustruct // empty no-op handler
			activity,
			fmt.Errorf("no handler registered for activity '%s'", activity.Name),
		)
	}

	activityRun := activity.ToActivityRun(w.storage.Decoder())

	out, err := handler.handler(ctx, activityRun)
	if err != nil {
		return w.handleActivityError(ctx, handler, activity, err)
	}

	return w.completeActivity(ctx, activity, out)
}

// completeActivity marks an activity as completed.
func (w *ActivityWorker) completeActivity(ctx context.Context, activity *ActivityRecord, out any) error {
	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	if err := w.storage.CompleteActivity(ctx, activity.ID, activity.WorkflowID, out); err != nil {
		return fmt.Errorf("complete activity in storage: %w", err)
	}

	return nil
}

// handleActivityError handles job processing errors with backoff.
func (w *ActivityWorker) handleActivityError(
	ctx context.Context,
	activityHandler *activityHandlerWrapper,
	activity *ActivityRecord,
	err error,
) error {
	// Do not retry unrecoverable errors and tasks that reach their limit
	if IsUnrecoverable(err) || activity.Attempts >= activity.MaxAttempts {
		return w.failActivity(ctx, activity, err)
	}

	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	errMsg := err.Error()

	// Calculate backoff duration
	backoff := activityHandler.calculateBackoff(activity.Attempts, w.config.Processing.DefaultBackoff)
	nextSchedule := time.Now().Add(backoff)

	if err := w.storage.ReScheduleActivity(ctx, activity.ID, nextSchedule, errMsg); err != nil {
		return fmt.Errorf("reschedule activity in storage: %w", err)
	}

	return nil
}

// failActivity immediately fails an activity moving it to the dead letter queue.
func (w *ActivityWorker) failActivity(ctx context.Context, activity *ActivityRecord, err error) error {
	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	errMsg := err.Error()

	if err := w.storage.FailActivity(ctx, activity.ID, activity.WorkflowID, errMsg); err != nil {
		return fmt.Errorf("fail activity in storage: %w", err)
	}

	return nil
}
