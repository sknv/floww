package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/sknv/floww"
)

// InsertSignal durably delivers a signal to a workflow and schedules a workflow task
// to wake it up. Both writes happen inside one transaction.
func (s *Storage) InsertSignal(
	ctx context.Context,
	txer floww.TxBeginner,
	workflowID uuid.UUID,
	signalID uuid.UUID,
	signalIdempotencyKey uuid.UUID,
	signalName string,
	signalInput any,
) error {
	err := pgx.BeginFunc(ctx, txer, func(tx pgx.Tx) error {
		// Insert signal
		if txErr := s.insertSignal(
			ctx, tx, workflowID, signalID, signalIdempotencyKey, signalName, signalInput,
		); txErr != nil {
			return fmt.Errorf("insert signal: %w", txErr)
		}

		// Resume workflow
		taskID := uuid.Must(uuid.NewV7())

		if txErr := s.insertWorkflowTask(ctx, tx, taskID, workflowID, time.Now()); txErr != nil {
			return fmt.Errorf("insert workflow task: %w", txErr)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("exec transaction: %w", err)
	}

	return nil
}

// insertSignal inserts a signal record into storage.
func (s *Storage) insertSignal(
	ctx context.Context,
	execer floww.Execer,
	workflowID uuid.UUID,
	signalID uuid.UUID,
	signalIdempotencyKey uuid.UUID,
	signalName string,
	signalInput any,
) error {
	var inputBytes []byte

	if signalInput != nil {
		var err error

		inputBytes, err = s.encoder.Encode(signalInput)
		if err != nil {
			return fmt.Errorf("encode input: %w", err)
		}
	}

	const sql = `
		INSERT INTO floww_signals (id, idempotency_key, workflow_id, name, input)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err := execer.Exec(
		ctx,
		sql,
		signalID,
		signalIdempotencyKey,
		workflowID,
		signalName,
		inputBytes,
	)
	if err != nil {
		return fmt.Errorf("exec signal inserting query: %w", err)
	}

	return nil
}

// signalEventRecord holds data to construct an signal event.
type signalEventRecord struct {
	IdempotencyKey uuid.UUID
	Input          []byte
}

// ToSignalEvent converts a raw DB record into an signal event.
//
// IMPORTANT:
// - Decoding is deferred to the provided decoder to allow custom serialization formats.
// - Assumes Input is encoded with the same encoder/decoder pair.
func (e signalEventRecord) ToSignalEvent(decoder floww.Decoder) floww.Event {
	return floww.NewEvent(e.IdempotencyKey, e.Input, decoder)
}

// ListWorkflowSignals returns all unconsumed signals for a workflow,
// ordered by arrival time (id ASC). Called once at the start of each workflow
// task execution.
func (s *Storage) ListWorkflowSignals(ctx context.Context, workflowID uuid.UUID) (floww.Events, error) {
	const sql = `
		SELECT idempotency_key, input
		FROM floww_signals
		WHERE workflow_id = $1
		ORDER BY id
	`

	rows, err := s.db.Query(ctx, sql, workflowID)
	if err != nil {
		return nil, fmt.Errorf("query workflow signal events: %w", err)
	}
	defer rows.Close()

	signalEvents := make(floww.Events)

	for rows.Next() {
		var signalEvent signalEventRecord

		err = rows.Scan(
			&signalEvent.IdempotencyKey,
			&signalEvent.Input,
		)
		if err != nil {
			return nil, fmt.Errorf("scan workflow signal event: %w", err)
		}

		signalEvents[signalEvent.IdempotencyKey] = signalEvent.ToSignalEvent(s.Decoder())
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over workflow signal events: %w", err)
	}

	return signalEvents, nil
}
