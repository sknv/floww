package floww

import (
	"github.com/google/uuid"
)

// Signal is a named, typed signal that can be sent to a running workflow.
// I is the payload type; it is encoded/decoded by the storage encoder.
//
// Each signal is identified in history by an idempotency key derived from the
// signal name and the workflow ID. This means the same named signal can only be
// received once per workflow by default. To send and receive multiple signals of
// the same type, supply a unique key per invocation via WithSignalIdempotencyKey
// or IdempotencyKeyByString — the same key must be used on both the send and
// receive sides so they resolve to the same history entry on replay.
type Signal[I any] struct {
	Name string
}

// NewSignal creates a Signal with the given name.
func NewSignal[I any](name string) Signal[I] {
	return Signal[I]{
		Name: name,
	}
}

// IdempotencyKey returns the default idempotency key for this signal in the
// given workflow. It is a deterministic SHA-1 UUID derived from the signal name
// and the workflow ID, so the same call always produces the same key.
//
// Use this when a signal is expected exactly once per workflow.
func (s Signal[I]) IdempotencyKey(workflowID uuid.UUID) uuid.UUID {
	return s.IdempotencyKeyByString(workflowID, "")
}

// IdempotencyKeyByString returns a deterministic idempotency key that
// incorporates str as an additional discriminator. Use this when the same signal
// needs to be sent and received more than once within a single workflow — for
// example, inside a loop — by passing a unique value (e.g. a loop index) as str.
//
// The same str must be used on both the SendSignal and ReceiveSignal sides so
// that the key resolves to the same history entry during replay.
func (s Signal[I]) IdempotencyKeyByString(workflowID uuid.UUID, str string) uuid.UUID {
	return uuid.NewSHA1(workflowID, []byte(s.Name+str))
}

// SignalOptions is an inner holder of provided signal options.
type SignalOptions struct {
	idempotencyKey uuid.UUID
}

// defaultSignalOptionsFor returns the default options for a signal with provided idempotency key.
func defaultSignalOptionsFor(idempotencyKey uuid.UUID) SignalOptions {
	return SignalOptions{
		idempotencyKey: idempotencyKey,
	}
}

// SignalOption is a function to configure signal options.
type SignalOption func(*SignalOptions)

// WithSignalIdempotencyKey overrides the idempotency key used when sending or
// receiving this signal. Use it when the same signal type must be sent more than
// once to the same workflow. The key passed to SendSignal and ReceiveSignal must
// match so both sides refer to the same history entry.
func WithSignalIdempotencyKey(idempotencyKey uuid.UUID) SignalOption {
	return func(o *SignalOptions) {
		o.idempotencyKey = idempotencyKey
	}
}
