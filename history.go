package floww

import "github.com/google/uuid"

type HistoryEvent struct {
	ActivityIdempotencyKey uuid.UUID

	activityOutput []byte
	decoder        Decoder
}

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

func (e HistoryEvent) IntoOutput(v any) error {
	return e.decoder(e.activityOutput, v)
}

// HistoryEvents is a map of activity idempotency key to a history event.
type HistoryEvents map[uuid.UUID]HistoryEvent
