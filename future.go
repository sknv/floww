package floww

import (
	"fmt"

	"github.com/samber/mo"
)

// Valuer decodes the raw value into v.
type Valuer interface {
	IntoValue(v any) error
}

// Future represents the pending result of an asynchronous event execution.
type Future[O any] struct {
	event mo.Option[Valuer]
}

// Get returns the event output, or ErrWorkflowSuspended if the event has not yet completed.
//
//nolint:ireturn // returns a generic result
func (f Future[O]) Get() (O, error) {
	var out O

	if f.event.IsNone() {
		return out, ErrWorkflowSuspended
	}

	if err := f.event.MustGet().IntoValue(&out); err != nil {
		return out, fmt.Errorf("decode future event value: %w", err)
	}

	return out, nil
}
