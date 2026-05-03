package floww

import (
	"fmt"

	"github.com/samber/mo"
)

type Future[O any] struct {
	event mo.Option[HistoryEvent]
}

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
