package floww

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

//
// Workflows
//

// WorkflowStatus represents the status of a workflow.
type WorkflowStatus string

const (
	WorkflowStatusRunning   WorkflowStatus = "running"
	WorkflowStatusAborted   WorkflowStatus = "aborted" // unused for now
	WorkflowStatusCompleted WorkflowStatus = "completed"
	WorkflowStatusFailed    WorkflowStatus = "failed"
)

// WorkflowRecord represents a workflow data.
type WorkflowRecord struct {
	ID                 uuid.UUID
	IdempotencyKey     uuid.UUID
	Name               string
	Status             WorkflowStatus
	Input              []byte
	Output             []byte
	Priority           int
	Attempts           uint
	MaxAttempts        uint
	StuckTimeoutMillis uint64
	CompletedAt        *time.Time
	ErrorMessage       *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

//
// Workflow tasks
//

// WorkflowTaskStatus represents the status of a workflow task.
type WorkflowTaskStatus string

const (
	WorkflowTaskStatusPending   WorkflowTaskStatus = "pending"
	WorkflowTaskStatusRunning   WorkflowTaskStatus = "running"
	WorkflowTaskStatusCompleted WorkflowTaskStatus = "completed"
	WorkflowTaskStatusFailed    WorkflowTaskStatus = "failed"
)

// WorkflowTaskRecord represents a workflow resuming task.
type WorkflowTaskRecord struct {
	ID          uuid.UUID
	Workflow    WorkflowRecord
	Status      WorkflowTaskStatus
	ScheduledAt time.Time
	RunAt       *time.Time
	StuckAt     *time.Time
	CompletedAt *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ToWorkflowRun converts the task record into a WorkflowRun suitable for handler execution.
func (t *WorkflowTaskRecord) ToWorkflowRun(decoder Decoder) WorkflowRun {
	return NewWorkflowRun(t.Workflow.ID, t.Workflow.Input, decoder)
}

// DefaultWorkflowWorkerConfig returns a default configuration for workflow worker.
func DefaultWorkflowWorkerConfig() *WorkerConfig {
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

// WorkflowWorker polls storage for pending workflow tasks and dispatches them to registered handlers.
type WorkflowWorker struct {
	storage  Storage
	registry *WorkflowRegistry
	config   *WorkerConfig

	wg      sync.WaitGroup
	stopped chan struct{}
}

// NewWorkflowWorker creates a new WorkflowWorker with the given storage, registry, and configuration.
func NewWorkflowWorker(
	storage Storage,
	registry *WorkflowRegistry,
	config *WorkerConfig,
) *WorkflowWorker {
	return &WorkflowWorker{
		storage:  storage,
		registry: registry,
		config:   config,

		wg:      sync.WaitGroup{},
		stopped: make(chan struct{}),
	}
}

// Start launches the workflow task polling loop in the background.
func (w *WorkflowWorker) Start(ctx context.Context) {
	// Start handler worker
	w.wg.Go(func() {
		// Unlink original context cancellation to gracefully stop the worker later
		workerCtx := context.WithoutCancel(ctx)

		w.runHandlerWorker(workerCtx)
	})
}

// Stop signals the worker to stop and waits until it finishes or the context expires.
func (w *WorkflowWorker) Stop(ctx context.Context) error {
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

func (w *WorkflowWorker) runHandlerWorker(ctx context.Context) {
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
				fetched := w.processWorkflowTasks(ctx)
				if fetched == 0 {
					break // no more tasks, wait for the next timer tick
				}

				// There are more tasks to process, handle them immediately.
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

func (w *WorkflowWorker) processWorkflowTasks(ctx context.Context) int {
	// Fetch tasks from db first
	tasks, err := w.fetchWorkflowTasks(ctx)
	if err != nil {
		log.Printf("[Floww][ERROR] Failed to fetch tasks: %v", err)

		return 0
	}

	if len(tasks) == 0 {
		return 0
	}

	// Process tasks concurrently respecting concurrency limit
	gr := errgroup.Group{}
	gr.SetLimit(w.config.Poll.Concurrency)

	for i := range tasks {
		gr.Go(func() error {
			task := &tasks[i]

			if taskErr := w.handleWorkflowTask(ctx, task); taskErr != nil {
				log.Printf("[Floww][ERROR] Failed to handle task with id '%s': %v", task.ID, taskErr)
			}

			return nil
		})
	}

	if err = gr.Wait(); err != nil {
		log.Printf("[Floww][ERROR] Failed to wait for all tasks to complete: %v", err)

		return 0
	}

	return len(tasks)
}

// fetchWorkflowTasks fetches batch of tasks from db.
func (w *WorkflowWorker) fetchWorkflowTasks(ctx context.Context) ([]WorkflowTaskRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	tasks, err := w.storage.ListActiveWorkflowTasks(ctx, w.config.Poll.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("list active workflow tasks from storage: %w", err)
	}

	return tasks, nil
}

// handleWorkflowTask processes the provided task by handing it to the corresponding handler
// and finishes it depending on the handling result.
func (w *WorkflowWorker) handleWorkflowTask(ctx context.Context, task *WorkflowTaskRecord) error {
	handler, exists := w.registry.handlers[task.Workflow.Name]
	if !exists {
		log.Printf(
			"[Floww][ERROR] No handler registered for workflow '%s', task '%s' will be rescheduled",
			task.Workflow.Name, task.ID,
		)

		return w.handleWorkflowTaskError(
			ctx,
			&workflowHandlerWrapper{}, //nolint:exhaustruct // empty no-op handler
			task,
			fmt.Errorf("no handler registered for workflow '%s'", task.Workflow.Name),
		)
	}

	workflowRun := task.ToWorkflowRun(w.storage.Decoder())

	if err := handler.handler(ctx, w.storage, workflowRun); err != nil {
		return w.handleWorkflowTaskError(ctx, handler, task, err)
	}

	return w.completeWorkflowTask(ctx, task)
}

// completeWorkflowTask marks a task as completed.
func (w *WorkflowWorker) completeWorkflowTask(ctx context.Context, task *WorkflowTaskRecord) error {
	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	if err := w.storage.CompleteWorkflowTask(ctx, task.ID, task.Workflow.ID); err != nil {
		return fmt.Errorf("complete workflow task in storage: %w", err)
	}

	return nil
}

// handleWorkflowTaskError handles job processing errors with backoff.
func (w *WorkflowWorker) handleWorkflowTaskError(
	ctx context.Context,
	workflowHandler *workflowHandlerWrapper,
	task *WorkflowTaskRecord,
	err error,
) error {
	// Do not retry unrecoverable errors and tasks that reach their limit
	if IsUnrecoverable(err) || task.Workflow.Attempts >= task.Workflow.MaxAttempts {
		return w.failWorkflowTask(ctx, task, err)
	}

	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	errMsg := err.Error()

	// Calculate backoff duration
	backoff := workflowHandler.calculateBackoff(task.Workflow.Attempts, w.config.Processing.DefaultBackoff)
	nextSchedule := time.Now().Add(backoff)

	if err := w.storage.ReScheduleWorkflowTask(ctx, task.ID, task.Workflow.ID, nextSchedule, errMsg); err != nil {
		return fmt.Errorf("reschedule workflow task in storage: %w", err)
	}

	return nil
}

func (w *WorkflowWorker) failWorkflowTask(ctx context.Context, task *WorkflowTaskRecord, err error) error {
	ctx, cancel := context.WithTimeout(ctx, w.config.Processing.DbTimeout)
	defer cancel()

	errMsg := err.Error()

	if err := w.storage.FailWorkflowTask(ctx, task.ID, task.Workflow.ID, errMsg); err != nil {
		return fmt.Errorf("fail workflow task in storage: %w", err)
	}

	return nil
}

// CleanColdWorkflows removes completed workflows.
//
// Most of the times the function should be called from some sort of a cron job.
func (w *WorkflowWorker) CleanColdWorkflows(ctx context.Context) error {
	// Keep workflows if there is no retention
	if w.config.ColdCleanup.RetentionInterval <= 0 {
		return nil
	}

	log.Printf("[Floww][INFO] Running cold workflows cleaner...")

	ctx, cancel := context.WithTimeout(ctx, w.config.ColdCleanup.DbTimeout)
	defer cancel()

	cutoffDate := time.Now().Add(-w.config.ColdCleanup.RetentionInterval)

	rowsAffected, err := w.storage.DeleteColdWorkflows(ctx, cutoffDate, w.config.ColdCleanup.CleanupBatchSize)
	if err != nil {
		return fmt.Errorf("delete cold workflows in storage: %w", err)
	}

	if rowsAffected == 0 {
		log.Printf("[Floww][INFO] No cold workflows to be cleaned up")

		return nil
	}

	log.Printf("[Floww][INFO] Cleaned up %d cold workflows", rowsAffected)

	return nil
}

// CleanDeadWorkflows removes failed workflows.
//
// Most of the times the function should be called from some sort of a cron job.
func (w *WorkflowWorker) CleanDeadWorkflows(ctx context.Context) error {
	// Keep workflows if there is no retention
	if w.config.DeadCleanup.RetentionInterval <= 0 {
		return nil
	}

	log.Printf("[Floww][INFO] Running dead workflows cleaner...")

	ctx, cancel := context.WithTimeout(ctx, w.config.DeadCleanup.DbTimeout)
	defer cancel()

	cutoffDate := time.Now().Add(-w.config.DeadCleanup.RetentionInterval)

	rowsAffected, err := w.storage.DeleteDeadWorkflows(ctx, cutoffDate, w.config.DeadCleanup.CleanupBatchSize)
	if err != nil {
		return fmt.Errorf("delete dead workflows in storage: %w", err)
	}

	if rowsAffected == 0 {
		log.Printf("[Floww][INFO] No dead workflows to be cleaned up")

		return nil
	}

	log.Printf("[Floww][INFO] Cleaned up %d dead workflows", rowsAffected)

	return nil
}
