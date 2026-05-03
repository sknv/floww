package floww

import (
	"context"
	"fmt"
	"time"
)

type Activity[I any, O any] struct {
	Name string
}

func NewActivity[I any, O any](name string) Activity[I, O] {
	return Activity[I, O]{
		Name: name,
	}
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

func WithActivityBackoffCalculator(backoffCalculator BackoffCalculator) ActivityHandlerOption {
	return func(h *activityHandlerWrapper) {
		h.backoffCalculator = backoffCalculator
	}
}

// ActivityOptions is an inner holder of provided activity options.
type ActivityOptions struct {
	priority     int
	maxAttempts  uint
	stuckTimeout time.Duration
	scheduledAt  time.Time
}

// defaultActivityOptions returns the default options for a workflow.
func defaultActivityOptions() ActivityOptions {
	//nolint:mnd // default values
	return ActivityOptions{
		priority:     0,
		maxAttempts:  1,
		stuckTimeout: time.Minute * 5,
		scheduledAt:  time.Now(),
	}
}

// Priority returns workflow priority.
func (o ActivityOptions) Priority() int {
	return o.priority
}

// MaxAttempts returns workflow max attempt count.
func (o ActivityOptions) MaxAttempts() uint {
	return o.maxAttempts
}

// StuckTimeoutMillis returns workflow stuck timeout in milliseconds.
func (o ActivityOptions) StuckTimeoutMillis() int64 {
	return int64(o.stuckTimeout / time.Millisecond)
}

// ScheduledAt returns workflow schedule.
func (o ActivityOptions) ScheduledAt() time.Time {
	return o.scheduledAt
}

// ActivityOption is a function to configure activity options.
type ActivityOption func(*ActivityOptions)

// WithActivityPriority sets the workflow priority.
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

// WithActivityStuckTimeout sets when the workflow should be considered as stuck.
func WithActivityStuckTimeout(t time.Duration) ActivityOption {
	return func(o *ActivityOptions) {
		o.stuckTimeout = t
	}
}

// WithActivityScheduledAt sets when the workflow should be executed.
func WithActivityScheduledAt(t time.Time) ActivityOption {
	return func(o *ActivityOptions) {
		o.scheduledAt = t
	}
}

//
// Activity registry
//

type ActivityRegistry struct {
	handlers map[string]*activityHandlerWrapper
}

func NewActivityRegistry() *ActivityRegistry {
	return &ActivityRegistry{
		handlers: make(map[string]*activityHandlerWrapper),
	}
}

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
