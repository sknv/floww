package floww

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
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
func (w *ActivityWorker) Start(ctx context.Context) {
	// Start handler worker
	w.wg.Go(func() {
		// Unlink original context cancellation to gracefully stop the worker later
		workerCtx := context.WithoutCancel(ctx)

		w.runHandlerWorker(workerCtx)
	})
}

// Stop signals the worker to stop and waits until it finishes or the context expires.
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

func (w *ActivityWorker) runHandlerWorker(ctx context.Context) {
	ticker := time.NewTicker(w.config.Poll.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Force stop (should never happen though)
			return
		case <-w.stopped:
			// Graceful stop
			return
		case <-ticker.C:
			for {
				fetched := w.processActivities(ctx)
				if fetched == 0 {
					break // no more tasks, wait for the next timer tick
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

func (w *ActivityWorker) processActivities(ctx context.Context) int {
	// Fetch activities from db first
	activities, err := w.fetchActivities(ctx)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "Failed to fetch activities",
			slog.String("component", "floww"),
			slog.String("error", err.Error()),
		)

		return 0
	}

	if len(activities) == 0 {
		return 0
	}

	// Process tasks concurrently respecting concurrency limit
	gr := errgroup.Group{}
	gr.SetLimit(w.config.Poll.Concurrency)

	for i := range activities {
		gr.Go(func() error {
			activity := &activities[i]

			if actErr := w.handleActivity(ctx, activity); actErr != nil {
				slog.LogAttrs(ctx, slog.LevelError, "Failed to handle an activity",
					slog.String("component", "floww"),
					slog.String("activity_id", activity.ID.String()),
					slog.String("error", actErr.Error()),
				)
			}

			return nil
		})
	}

	if err = gr.Wait(); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "Failed to wait for all activities to complete",
			slog.String("component", "floww"),
			slog.String("error", err.Error()),
		)

		return 0
	}

	return len(activities)
}

// fetchActivities fetches batch of activities from db.
func (w *ActivityWorker) fetchActivities(ctx context.Context) ([]ActivityRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	activities, err := w.storage.ListActiveActivities(ctx, w.config.Poll.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("list activities from storage: %w", err)
	}

	return activities, nil
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

func (w *ActivityWorker) failActivity(ctx context.Context, activity *ActivityRecord, err error) error {
	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	errMsg := err.Error()

	if err := w.storage.FailActivity(ctx, activity.ID, activity.WorkflowID, errMsg); err != nil {
		return fmt.Errorf("fail activity in storage: %w", err)
	}

	return nil
}
