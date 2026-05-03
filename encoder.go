package floww

// Encoder marshals and unmarshals job payload.
type Encoder interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}
