package floww

import "github.com/google/uuid"

// Event represents the recorded event within a workflow.
type Event struct {
	IdempotencyKey uuid.UUID

	value   []byte
	decoder Decoder
}

// NewEvent constructs a Event with the given id, idempotencyKey, raw value bytes and decoder.
func NewEvent(
	idempotencyKey uuid.UUID,
	value []byte,
	decoder Decoder,
) Event {
	return Event{
		IdempotencyKey: idempotencyKey,

		value:   value,
		decoder: decoder,
	}
}

// IntoValue decodes the raw event value into v.
func (e Event) IntoValue(v any) error {
	return e.decoder(e.value, v)
}

// Events is a map of event idempotency key to a event.
type Events map[uuid.UUID]Event
