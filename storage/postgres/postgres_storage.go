package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sknv/floww"
)

// StorageOption is a function to configure workflow worker options.
type StorageOption func(*Storage)

// WithStorageQueueEncoder sets the encoder to marshal and unmarshal workflow and activity inputs.
func WithStorageQueueEncoder(encoder floww.Encoder) StorageOption {
	return func(w *Storage) {
		w.encoder = encoder
	}
}

type Storage struct {
	db      *pgxpool.Pool
	encoder floww.Encoder
}

func NewStorage(
	db *pgxpool.Pool,
	opts ...StorageOption,
) *Storage {
	storage := &Storage{
		db:      db,
		encoder: &floww.JsonEncoder{},
	}

	for _, opt := range opts {
		opt(storage)
	}

	return storage
}

func (s *Storage) Decoder() floww.Decoder {
	return s.encoder.Decode
}
