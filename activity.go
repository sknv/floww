package floww

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Activity represents a named unit of work with typed input I and output O.
type Activity[I any, O any] struct {
	Name string
}

// NewActivity creates a new Activity with the given name.
func NewActivity[I any, O any](name string) Activity[I, O] {
	return Activity[I, O]{
		Name: name,
	}
}

// IdempotencyKey provides predictive idempotency key for the provided workflow id.
func (a Activity[I, O]) IdempotencyKey(workflowID uuid.UUID) uuid.UUID {
	return a.IdempotencyKeyFor(workflowID, "")
}

// IdempotencyKeyFor provides predictive idempotency key for the provided workflow id respecting the salt argument.
func (a Activity[I, O]) IdempotencyKeyFor(workflowID uuid.UUID, salt string) uuid.UUID {
	return uuid.NewSHA1(workflowID, []byte(a.Name+salt))
}

//
// Activity handler
//

type activityHandler func(ctx context.Context, activityRun ActivityRun) (any, error)

type activityHandlerWrapper struct {
	handler           activityHandler
	backoffCalculator BackoffCalculator
}

func (h *activityHandlerWrapper) calculateBackoff(attempt uint, defaultBackoff time.Duration) time.Duration {
	if h.backoffCalculator == nil {
		return defaultBackoff
	}

	return h.backoffCalculator(attempt)
}

// ActivityHandlerOption is a function to configure activity handler options.
type ActivityHandlerOption func(*activityHandlerWrapper)

// WithActivityBackoffCalculator sets a custom backoff calculator for activity retries.
func WithActivityBackoffCalculator(backoffCalculator BackoffCalculator) ActivityHandlerOption {
	return func(h *activityHandlerWrapper) {
		h.backoffCalculator = backoffCalculator
	}
}

// ActivityOptions is an inner holder of provided activity options.
type ActivityOptions struct {
	idempotencyKey uuid.UUID
	priority       int
	maxAttempts    uint
	stuckTimeout   time.Duration
	scheduledAt    time.Time
}

// defaultActivityOptionsFor returns the default options for an activity with provided idempotency key.
func defaultActivityOptionsFor(idempotencyKey uuid.UUID) ActivityOptions {
	//nolint:mnd // default values
	return ActivityOptions{
		idempotencyKey: idempotencyKey,
		priority:       0,
		maxAttempts:    1,
		stuckTimeout:   time.Minute * 5,
		scheduledAt:    time.Now(),
	}
}

// Priority returns activity priority.
func (o ActivityOptions) Priority() int {
	return o.priority
}

// MaxAttempts returns activity max attempt count.
func (o ActivityOptions) MaxAttempts() uint {
	return o.maxAttempts
}

// StuckTimeoutMillis returns activity stuck timeout in milliseconds.
func (o ActivityOptions) StuckTimeoutMillis() int64 {
	return int64(o.stuckTimeout / time.Millisecond)
}

// ScheduledAt returns activity schedule.
func (o ActivityOptions) ScheduledAt() time.Time {
	return o.scheduledAt
}

// ActivityOption is a function to configure activity options.
type ActivityOption func(*ActivityOptions)

// WithActivityIdempotencyKey sets the idempotency key explicitly.
func WithActivityIdempotencyKey(idempotencyKey uuid.UUID) ActivityOption {
	return func(o *ActivityOptions) {
		o.idempotencyKey = idempotencyKey
	}
}

// WithActivityPriority sets the activity priority.
func WithActivityPriority(priority int) ActivityOption {
	return func(o *ActivityOptions) {
		o.priority = priority
	}
}

// WithActivityMaxAttempts sets the maximum number of attempts.
func WithActivityMaxAttempts(attempts uint) ActivityOption {
	return func(o *ActivityOptions) {
		o.maxAttempts = attempts
	}
}

// WithActivityStuckTimeout sets when the activity should be considered as stuck.
func WithActivityStuckTimeout(t time.Duration) ActivityOption {
	return func(o *ActivityOptions) {
		o.stuckTimeout = t
	}
}

// WithActivityScheduledAt sets when the activity should be executed.
func WithActivityScheduledAt(t time.Time) ActivityOption {
	return func(o *ActivityOptions) {
		o.scheduledAt = t
	}
}

//
// Activity registry
//

// ActivityRegistry holds registered activity handlers keyed by activity name.
type ActivityRegistry struct {
	handlers map[string]*activityHandlerWrapper
}

// NewActivityRegistry creates a new empty ActivityRegistry.
func NewActivityRegistry() *ActivityRegistry {
	return &ActivityRegistry{
		handlers: make(map[string]*activityHandlerWrapper),
	}
}

// RegisterActivity registers a typed handler for the given activity in the registry.
func RegisterActivity[I any, O any](
	r *ActivityRegistry,
	activity Activity[I, O],
	handler func(context.Context, I) (O, error),
	opts ...ActivityHandlerOption,
) {
	handlerWrapper := &activityHandlerWrapper{
		handler: func(ctx context.Context, activityRun ActivityRun) (any, error) {
			var input I

			if err := activityRun.IntoInput(&input); err != nil {
				return nil, fmt.Errorf("decode activity payload [id = %s]: %w", activityRun.ActivityID, err)
			}

			out, err := handler(ctx, input)
			if err != nil {
				return nil, err
			}

			return out, nil
		},
		backoffCalculator: nil,
	}

	for _, opt := range opts {
		opt(handlerWrapper)
	}

	r.handlers[activity.Name] = handlerWrapper
}
