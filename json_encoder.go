package floww

import (
	"encoding/json"
)

// JsonEncoder is the default encoder/decoder for the storage.
type JsonEncoder struct{}

func (*JsonEncoder) Encode(v any) ([]byte, error) {
	return json.Marshal(v) //nolint:wrapcheck // proxy error
}

func (*JsonEncoder) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v) //nolint:wrapcheck // proxy error
}
