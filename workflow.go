package floww

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrWorkflowSuspended is returned by a workflow handler when it is waiting for a pending activity to complete.
var ErrWorkflowSuspended = errors.New("workflow suspended")

// Workflow represents a named orchestration unit with typed input I.
type Workflow[I any] struct {
	Name string
}

// NewWorkflow creates a new Workflow with the given name.
func NewWorkflow[I any](name string) Workflow[I] {
	return Workflow[I]{
		Name: name,
	}
}

//
// Workflow handler
//

type workflowHandler func(ctx context.Context, storage Storage, workflowRun WorkflowRun) error

type workflowHandlerWrapper struct {
	handler           workflowHandler
	backoffCalculator BackoffCalculator
}

func (h *workflowHandlerWrapper) calculateBackoff(attempt uint, defaultBackoff time.Duration) time.Duration {
	if h.backoffCalculator == nil {
		return defaultBackoff
	}

	return h.backoffCalculator(attempt)
}

// WorkflowHandlerOption is a function to configure workflow handler options.
type WorkflowHandlerOption func(*workflowHandlerWrapper)

// WithWorkflowBackoffCalculator sets a custom backoff calculator for workflow task retries.
func WithWorkflowBackoffCalculator(backoffCalculator BackoffCalculator) WorkflowHandlerOption {
	return func(h *workflowHandlerWrapper) {
		h.backoffCalculator = backoffCalculator
	}
}

//
// Workflow registry
//

// WorkflowRegistry holds registered workflow handlers keyed by workflow name.
type WorkflowRegistry struct {
	handlers map[string]*workflowHandlerWrapper
}

// NewWorkflowRegistry creates a new empty WorkflowRegistry.
func NewWorkflowRegistry() *WorkflowRegistry {
	return &WorkflowRegistry{
		handlers: make(map[string]*workflowHandlerWrapper),
	}
}

// RegisterWorkflow registers a typed handler for the given workflow in the registry.
func RegisterWorkflow[I any](
	r *WorkflowRegistry,
	workflow Workflow[I],
	handler func(*WorkflowContext, I) error,
	opts ...WorkflowHandlerOption,
) {
	handlerWrapper := &workflowHandlerWrapper{
		handler: func(ctx context.Context, storage Storage, workflowRun WorkflowRun) error {
			var input I

			if err := workflowRun.IntoInput(&input); err != nil {
				return fmt.Errorf("decode workflow payload [id = %s]: %w", workflowRun.WorkflowID, err)
			}

			return runWorkflow(ctx, storage, workflowRun.WorkflowID, input, handler)
		},
		backoffCalculator: nil,
	}

	for _, opt := range opts {
		opt(handlerWrapper)
	}

	r.handlers[workflow.Name] = handlerWrapper
}

func runWorkflow[I any](
	ctx context.Context,
	storage Storage,
	id uuid.UUID,
	input I,
	handler func(*WorkflowContext, I) error,
) error {
	history, err := storage.ListHistoryEventsForWorkflow(ctx, id)
	if err != nil {
		return fmt.Errorf("list history events for workflow [id = %s]: %w", id, err)
	}

	workflowCtx := NewWorkflowContext(ctx, id, storage, history)

	err = handler(workflowCtx, input)
	if err != nil {
		switch {
		case errors.Is(err, ErrWorkflowSuspended):
			return nil
		default:
			return err
		}
	}

	if err = storage.CompleteWorkflow(ctx, id); err != nil {
		return fmt.Errorf("complete workflow [id = %s]: %w", id, err)
	}

	return nil
}

// WorkflowOptions is an inner holder of provided workflow options.
type WorkflowOptions struct {
	priority     int
	maxAttempts  uint
	stuckTimeout time.Duration
	scheduledAt  time.Time
}

// defaultWorkflowOptions returns the default options for a workflow.
func defaultWorkflowOptions() WorkflowOptions {
	//nolint:mnd // default values
	return WorkflowOptions{
		priority:     0,
		maxAttempts:  1,
		stuckTimeout: time.Minute * 5,
		scheduledAt:  time.Now(),
	}
}

// Priority returns workflow priority.
func (o WorkflowOptions) Priority() int {
	return o.priority
}

// MaxAttempts returns workflow max attempt count.
func (o WorkflowOptions) MaxAttempts() uint {
	return o.maxAttempts
}

// StuckTimeoutMillis returns workflow stuck timeout in milliseconds.
func (o WorkflowOptions) StuckTimeoutMillis() int64 {
	return int64(o.stuckTimeout / time.Millisecond)
}

// ScheduledAt returns workflow schedule.
func (o WorkflowOptions) ScheduledAt() time.Time {
	return o.scheduledAt
}

// WorkflowOption is a function to configure workflow options.
type WorkflowOption func(*WorkflowOptions)

// WithWorkflowPriority sets the workflow priority.
func WithWorkflowPriority(priority int) WorkflowOption {
	return func(o *WorkflowOptions) {
		o.priority = priority
	}
}

// WithWorkflowMaxAttempts sets the maximum number of attempts.
func WithWorkflowMaxAttempts(attempts uint) WorkflowOption {
	return func(o *WorkflowOptions) {
		o.maxAttempts = attempts
	}
}

// WithWorkflowStuckTimeout sets when the workflow should be considered as stuck.
func WithWorkflowStuckTimeout(t time.Duration) WorkflowOption {
	return func(o *WorkflowOptions) {
		o.stuckTimeout = t
	}
}

// WithWorkflowScheduledAt sets when the workflow should be executed.
func WithWorkflowScheduledAt(t time.Time) WorkflowOption {
	return func(o *WorkflowOptions) {
		o.scheduledAt = t
	}
}

// EnqueueWorkflow enqueues a workflow for asynchronous execution.
// The idempotencyKey prevents duplicate submissions for the same logical request.
func EnqueueWorkflow[I any](
	ctx context.Context,
	storage Storage,
	txer TxBeginner,
	workflow Workflow[I],
	idempotencyKey uuid.UUID,
	input I,
	opts ...WorkflowOption,
) error {
	id := uuid.Must(uuid.NewV7())

	options := defaultWorkflowOptions()
	for _, opt := range opts {
		opt(&options)
	}

	if err := storage.InsertWorkflow(ctx, txer, workflow.Name, id, idempotencyKey, input, options); err != nil {
		return fmt.Errorf("insert workflow [name = %s]: %w", workflow.Name, err)
	}

	return nil
}
