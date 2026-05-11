package floww

import (
	"github.com/google/uuid"
)

// Signal is a named, typed signal that can be sent to a running workflow.
// I is the payload type.
type Signal[I any] struct {
	Name string
}

// NewSignal creates a Signal with the given name.
func NewSignal[I any](name string) Signal[I] {
	return Signal[I]{
		Name: name,
	}
}

// IdempotencyKey provides predictive idempotency key for the provided workflow id.
func (s Signal[I]) IdempotencyKey(workflowID uuid.UUID) uuid.UUID {
	return s.IdempotencyKeyByString(workflowID, "")
}

// IdempotencyKeyByString provides predictive idempotency key for the provided workflow id
// respecting the string argument.
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

// WithSignalIdempotencyKey sets the idempotency key explicitly.
func WithSignalIdempotencyKey(idempotencyKey uuid.UUID) SignalOption {
	return func(o *SignalOptions) {
		o.idempotencyKey = idempotencyKey
	}
}
