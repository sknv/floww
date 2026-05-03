package floww

import (
	"fmt"

	"github.com/samber/mo"
)

// Future represents the pending result of an asynchronous activity execution.
type Future[O any] struct {
	event mo.Option[HistoryEvent]
}

// Get returns the activity output, or ErrWorkflowSuspended if the activity has not yet completed.
//
//nolint:ireturn // returns a generic result
func (f Future[O]) Get() (O, error) {
	var out O

	if f.event.IsNone() {
		return out, ErrWorkflowSuspended
	}

	if err := f.event.MustGet().IntoOutput(&out); err != nil {
		return out, fmt.Errorf("decode history event output: %w", err)
	}

	return out, nil
}
