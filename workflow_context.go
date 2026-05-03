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
	history    HistoryEvents
}

// NewWorkflowContext constructs a WorkflowContext for the given workflow execution.
func NewWorkflowContext(
	ctx context.Context,
	workflowID uuid.UUID,
	storage Storage,
	history HistoryEvents,
) *WorkflowContext {
	return &WorkflowContext{
		ctx:        ctx,
		workflowID: workflowID,
		storage:    storage,
		history:    history,
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
	idempotencyKey := activity.IdempotencyKey(ctx.WorkflowID())

	if event, ok := ctx.history[idempotencyKey]; ok {
		return Future[O]{
			event: mo.Some(event),
		}, nil
	}

	id := uuid.Must(uuid.NewV7())

	options := defaultActivityOptions()
	for _, opt := range opts {
		opt(&options)
	}

	if err := ctx.storage.InsertActivity(
		ctx.ctx, activity.Name, id, idempotencyKey, ctx.workflowID, input, options,
	); err != nil {
		return Future[O]{}, fmt.Errorf("insert activity [name = %s]: %w", activity.Name, err)
	}

	return Future[O]{
		event: mo.None[HistoryEvent](),
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
	out, err := ExecuteActivityAsync(ctx, activity, input, opts...)
	if err != nil {
		var zero O

		return zero, err
	}

	return out.Get()
}
