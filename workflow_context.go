package floww

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/samber/mo"
)

type WorkflowContext struct {
	ctx        context.Context //nolint:containedctx // wrapper
	workflowID uuid.UUID
	storage    Storage
	history    HistoryEvents
}

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

func (c *WorkflowContext) Context() context.Context {
	return c.ctx
}

func (c *WorkflowContext) WorkflowID() uuid.UUID {
	return c.workflowID
}

func ExecuteActivityAsync[I any, O any](
	ctx *WorkflowContext,
	activity Activity[I, O],
	idempotencyKey uuid.UUID,
	input I,
	opts ...ActivityOption,
) (Future[O], error) {
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

//nolint:ireturn // returns a generic result
func ExecuteActivity[I any, O any](
	ctx *WorkflowContext,
	activity Activity[I, O],
	idempotencyKey uuid.UUID,
	input I,
	opts ...ActivityOption,
) (O, error) {
	out, err := ExecuteActivityAsync(ctx, activity, idempotencyKey, input, opts...)
	if err != nil {
		var zero O

		return zero, err
	}

	return out.Get()
}
