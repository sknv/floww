package floww

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// ErrWorkflowSuspended is returned by a workflow handler when it is waiting for a pending event to complete.
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
	var (
		history Events
		signals Events
	)

	gr, grCtx := errgroup.WithContext(ctx)

	gr.Go(func() error {
		var err error

		history, err = storage.ListHistoryEventsForWorkflow(grCtx, id)
		if err != nil {
			return fmt.Errorf("list history events for workflow [id = %s]: %w", id, err)
		}

		return nil
	})

	gr.Go(func() error {
		var err error

		signals, err = storage.ListWorkflowSignals(grCtx, id)
		if err != nil {
			return fmt.Errorf("list signals for workflow [id = %s]: %w", id, err)
		}

		return nil
	})

	if err := gr.Wait(); err != nil {
		return fmt.Errorf("wait group: %w", err)
	}

	workflowCtx := NewWorkflowContext(ctx, id, storage, history, signals)

	err := handler(workflowCtx, input)
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
) (uuid.UUID, error) {
	// Provide default workflow options first and then apply the provided ones
	options := defaultWorkflowOptions()
	for _, opt := range opts {
		opt(&options)
	}

	id := uuid.Must(uuid.NewV7())

	upsertedID, err := storage.InsertWorkflow(ctx, txer, workflow.Name, id, idempotencyKey, input, options)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert workflow [name = %s]: %w", workflow.Name, err)
	}

	return upsertedID, nil
}

// SendSignal delivers a typed signal to a running workflow.
func SendSignal[I any](
	ctx context.Context,
	storage Storage,
	txer TxBeginner,
	workflowID uuid.UUID,
	signal Signal[I],
	input I,
	opts ...SignalOption,
) error {
	// Provide default signal options with idempotency key first and then apply the provided ones
	options := defaultSignalOptionsFor(
		signal.IdempotencyKey(workflowID),
	)
	for _, opt := range opts {
		opt(&options)
	}

	// Insert a signal with a specified id and idempotency key
	id := uuid.Must(uuid.NewV7())
	idempotencyKey := options.idempotencyKey

	if err := storage.InsertSignal(ctx, txer, workflowID, id, idempotencyKey, signal.Name, input); err != nil {
		return fmt.Errorf("insert signal [name = %s, workflow id = %s]: %w", signal.Name, workflowID, err)
	}

	return nil
}
