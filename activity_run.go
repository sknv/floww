package floww

import "github.com/google/uuid"

// ActivityRun holds the execution context for a single activity invocation.
type ActivityRun struct {
	ActivityID uuid.UUID

	activityInput []byte
	decoder       Decoder
}

// NewActivityRun constructs an ActivityRun with the provided ID, raw input bytes, and decoder.
func NewActivityRun(
	activityID uuid.UUID,
	activityInput []byte,
	decoder Decoder,
) ActivityRun {
	return ActivityRun{
		ActivityID: activityID,

		activityInput: activityInput,
		decoder:       decoder,
	}
}

// IntoInput decodes the raw activity input into v.
func (r ActivityRun) IntoInput(v any) error {
	return r.decoder(r.activityInput, v)
}
