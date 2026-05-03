package floww

import "github.com/google/uuid"

// HistoryEvent represents the recorded outcome of a completed activity within a workflow.
type HistoryEvent struct {
	ActivityIdempotencyKey uuid.UUID

	activityOutput []byte
	decoder        Decoder
}

// NewHistoryEvent constructs a HistoryEvent with the given idempotency key, raw output bytes, and decoder.
func NewHistoryEvent(
	activityIdempotencyKey uuid.UUID,
	activityOutput []byte,
	decoder Decoder,
) HistoryEvent {
	return HistoryEvent{
		ActivityIdempotencyKey: activityIdempotencyKey,

		activityOutput: activityOutput,
		decoder:        decoder,
	}
}

// IntoOutput decodes the raw activity output into v.
func (e HistoryEvent) IntoOutput(v any) error {
	return e.decoder(e.activityOutput, v)
}

// HistoryEvents is a map of activity idempotency key to a history event.
type HistoryEvents map[uuid.UUID]HistoryEvent
