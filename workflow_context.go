package floww

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/samber/mo"
)

// WorkflowContext is passed to workflow handlers and provides access to activity scheduling and workflow metadata.
type WorkflowContext struct {
	ctx        context.Context //nolint:containedctx // wrapper
	workflowID uuid.UUID
	storage    Storage
	history    Events
	signals    Events
}

// NewWorkflowContext constructs a WorkflowContext for the given workflow execution.
func NewWorkflowContext(
	ctx context.Context,
	workflowID uuid.UUID,
	storage Storage,
	history Events,
	signals Events,
) *WorkflowContext {
	return &WorkflowContext{
		ctx:        ctx,
		workflowID: workflowID,
		storage:    storage,
		history:    history,
		signals:    signals,
	}
}

// Context returns the underlying Go context.
func (c *WorkflowContext) Context() context.Context {
	return c.ctx
}

// WorkflowID returns the unique identifier of the running workflow.
func (c *WorkflowContext) WorkflowID() uuid.UUID {
	return c.workflowID
}

// ExecuteActivityAsync schedules an activity and returns a Future for its result without blocking.
// If the activity has already completed (found in history), the Future resolves immediately.
func ExecuteActivityAsync[I any, O any](
	ctx *WorkflowContext,
	activity Activity[I, O],
	input I,
	opts ...ActivityOption,
) (Future[O], error) {
	// Provide default activity options with idempotency key first and then apply the provided ones
	options := defaultActivityOptionsFor(
		activity.IdempotencyKey(ctx.WorkflowID()),
	)
	for _, opt := range opts {
		opt(&options)
	}

	// Check if an activity has already been executed
	idempotencyKey := options.idempotencyKey

	if historyEvent, ok := ctx.history[idempotencyKey]; ok {
		return Future[O]{
			event: mo.Some[Valuer](historyEvent),
		}, nil
	}

	// Schedule new activity
	id := uuid.Must(uuid.NewV7())

	if err := ctx.storage.InsertActivity(
		ctx.ctx, activity.Name, id, idempotencyKey, ctx.workflowID, input, options,
	); err != nil {
		return Future[O]{}, fmt.Errorf("insert activity [name = %s]: %w", activity.Name, err)
	}

	return Future[O]{
		event: mo.None[Valuer](),
	}, nil
}

// ExecuteActivity schedules an activity and returns its result,
// suspending the workflow if the activity is still pending.
//
//nolint:ireturn // returns a generic result
func ExecuteActivity[I any, O any](
	ctx *WorkflowContext,
	activity Activity[I, O],
	input I,
	opts ...ActivityOption,
) (O, error) {
	future, err := ExecuteActivityAsync(ctx, activity, input, opts...)
	if err != nil {
		var zero O

		return zero, err
	}

	return future.Get()
}

// ReceiveSignalAsync is the non-blocking form, mirroring ExecuteActivityAsync.
// Callers can fan out multiple ReceiveSignalAsync calls and then call Get on each.
func ReceiveSignalAsync[I any](
	ctx *WorkflowContext,
	signal Signal[I],
	opts ...SignalOption,
) (Future[I], error) {
	// Provide default signal options with idempotency key first and then apply the provided ones
	options := defaultSignalOptionsFor(
		signal.IdempotencyKey(ctx.WorkflowID()),
	)
	for _, opt := range opts {
		opt(&options)
	}

	// Consume a signal with a specified idempotency key
	idempotencyKey := options.idempotencyKey

	if signalEvent, ok := ctx.signals[idempotencyKey]; ok {
		return Future[I]{
			event: mo.Some[Valuer](signalEvent),
		}, nil
	}

	return Future[I]{
		event: mo.None[Valuer](),
	}, nil
}

// ReceiveSignal returns the pending signal of this type,
// or suspends the workflow if none has arrived yet.
//
//nolint:ireturn // returns a generic result
func ReceiveSignal[I any](
	ctx *WorkflowContext,
	signal Signal[I],
	opts ...SignalOption,
) (I, error) {
	future, err := ReceiveSignalAsync(ctx, signal, opts...)
	if err != nil {
		var zero I

		return zero, err
	}

	return future.Get()
}
