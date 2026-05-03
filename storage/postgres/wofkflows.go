package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/sknv/floww"
)

// InsertWorkflow creates a new workflow and schedules its first workflow task.
// The workflow insert and task creation are executed in a single transaction.
// If a workflow with the same idempotency key already exists, the insert is ignored.
func (s *Storage) InsertWorkflow(
	ctx context.Context,
	txer floww.TxBeginner,
	name string,
	id uuid.UUID,
	idempotencyKey uuid.UUID,
	input any,
	options floww.WorkflowOptions,
) error {
	err := pgx.BeginFunc(ctx, txer, func(tx pgx.Tx) error {
		if txErr := s.insertWorkflow(
			ctx,
			tx,
			name,
			id,
			idempotencyKey,
			input,
			options.Priority(),
			options.MaxAttempts(),
			options.StuckTimeoutMillis(),
		); txErr != nil {
			return fmt.Errorf("insert workflow tx: %w", txErr)
		}

		taskID := uuid.Must(uuid.NewV7())

		if txErr := s.insertWorkflowTask(ctx, tx, taskID, id, options.ScheduledAt()); txErr != nil {
			return fmt.Errorf("insert workflow task tx: %w", txErr)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("exec transaction: %w", err)
	}

	return nil
}

// insertWorkflow inserts a workflow record into storage.
//
// It encodes the input payload (if provided) and persists execution parameters.
//
// IMPORTANT:
// - Intended to be called inside a transaction together with task creation.
// - Uses idempotency key to guarantee safe retries (ON CONFLICT DO NOTHING).
// - Does not verify whether the insert actually happened (caller may rely on idempotency).
func (s *Storage) insertWorkflow(
	ctx context.Context,
	execer floww.Execer,
	name string,
	id uuid.UUID,
	idempotencyKey uuid.UUID,
	input any,
	priority int,
	maxAttempts uint,
	stuckTimeoutMillis int64,
) error {
	var inputBytes []byte

	if input != nil {
		var err error

		inputBytes, err = s.encoder.Encode(input)
		if err != nil {
			return fmt.Errorf("encode input: %w", err)
		}
	}

	const sql = `
		INSERT INTO floww_workflows (
		  id,
		  idempotency_key,
		  name,
		  input,
		  priority,
		  max_attempts,
		  stuck_timeout_millis
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (idempotency_key) DO NOTHING
	`

	_, err := execer.Exec(
		ctx,
		sql,
		id,
		idempotencyKey,
		name,
		inputBytes,
		priority,
		maxAttempts,
		stuckTimeoutMillis,
	)
	if err != nil {
		return fmt.Errorf("exec workflow inserting query: %w", err)
	}

	return nil
}

type historyEventRecord struct {
	ActivityIdempotencyKey uuid.UUID
	ActivityOutput         []byte
}

// ToHistoryEvent converts a raw DB record into a domain HistoryEvent.
//
// IMPORTANT:
// - Decoding is deferred to the provided decoder to allow custom serialization formats.
// - Assumes ActivityOutput is encoded with the same encoder/decoder pair.
func (e historyEventRecord) ToHistoryEvent(decoder floww.Decoder) floww.HistoryEvent {
	return floww.NewHistoryEvent(e.ActivityIdempotencyKey, e.ActivityOutput, decoder)
}

// ListHistoryEventsForWorkflow returns all completed activity outputs for a workflow,
// mapped by activity idempotency key. Results are ordered by activity ID.
func (s *Storage) ListHistoryEventsForWorkflow(ctx context.Context, id uuid.UUID) (floww.HistoryEvents, error) {
	const sql = `
		SELECT idempotency_key, output
		FROM floww_activities
		WHERE workflow_id = $1
		  AND status = $2
		ORDER BY id
	`

	rows, err := s.db.Query(ctx, sql, id, floww.ActivityStatusCompleted)
	if err != nil {
		return nil, fmt.Errorf("query workflow history events: %w", err)
	}
	defer rows.Close()

	historyEvents := make(floww.HistoryEvents)

	for rows.Next() {
		var historyEvent historyEventRecord

		err = rows.Scan(
			&historyEvent.ActivityIdempotencyKey,
			&historyEvent.ActivityOutput,
		)
		if err != nil {
			return nil, fmt.Errorf("scan workflow history event: %w", err)
		}

		historyEvents[historyEvent.ActivityIdempotencyKey] = historyEvent.ToHistoryEvent(s.Decoder())
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over workflow history events: %w", err)
	}

	return historyEvents, nil
}

// CompleteWorkflow marks the workflow as completed and clears any error message.
// Returns an error if the workflow does not exist.
func (s *Storage) CompleteWorkflow(ctx context.Context, id uuid.UUID) error {
	const sql = `
		UPDATE floww_workflows
		SET status = $2,
		    completed_at = now(),
		    error_message = NULL
		WHERE id = $1
	`

	cmd, err := s.db.Exec(ctx, sql, id, floww.WorkflowStatusCompleted)
	if err != nil {
		return fmt.Errorf("exec workflow completing query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("workflow with id '%s' was not marked as completed", id)
	}

	return nil
}

// failWorkflow marks a workflow as failed.
//
// It updates the workflow status, sets the error message, and records completion time.
//
// IMPORTANT:
// - Must be called within a transaction when coupled with task/activity failure.
// - Does not enforce state transitions.
// - Returns an error if the workflow does not exist.
func (s *Storage) failWorkflow(
	ctx context.Context,
	execer floww.Execer,
	id uuid.UUID,
	errorMessage string,
) error {
	const sql = `
		UPDATE floww_workflows
		SET status = $2,
		    error_message = $3,
		    completed_at = now()
		WHERE id = $1
	`

	cmd, err := execer.Exec(ctx, sql, id, floww.WorkflowStatusFailed, errorMessage)
	if err != nil {
		return fmt.Errorf("exec workflow failing query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("workflow with id '%s' was not marked as failed", id)
	}

	return nil
}

// DeleteColdWorkflows deletes completed workflows older than the provided cutoff date.
// The number of deleted records is limited by the given limit.
func (s *Storage) DeleteColdWorkflows(ctx context.Context, cutoffDate time.Time, limit uint) (uint, error) {
	return s.deleteWorkflows(ctx, floww.WorkflowStatusCompleted, cutoffDate, limit)
}

// DeleteDeadWorkflows deletes failed workflows older than the provided cutoff date.
// The number of deleted records is limited by the given limit.
func (s *Storage) DeleteDeadWorkflows(ctx context.Context, cutoffDate time.Time, limit uint) (uint, error) {
	return s.deleteWorkflows(ctx, floww.WorkflowStatusFailed, cutoffDate, limit)
}

// deleteWorkflows deletes workflows with the given status older than cutoffDate.
//
// It deletes up to 'limit' records using a subquery to avoid long-running full-table deletes.
//
// IMPORTANT:
// - Not transactional across batches; caller should loop if full cleanup is needed.
// - Uses LIMIT inside subquery for controlled batch deletion.
// - Returns number of rows actually deleted.
func (s *Storage) deleteWorkflows(
	ctx context.Context,
	status floww.WorkflowStatus,
	cutoffDate time.Time,
	limit uint,
) (uint, error) {
	const sql = `
		DELETE FROM floww_workflows
		WHERE id IN (
		  SELECT id FROM floww_workflows
		  WHERE status = $1
		    AND created_at < $2
		  LIMIT $3
		)
	`

	cmd, err := s.db.Exec(ctx, sql, status, cutoffDate, limit)
	if err != nil {
		return 0, fmt.Errorf("exec workflows deleting query: %w", err)
	}

	rowsAffected := cmd.RowsAffected()

	return uint(rowsAffected), nil //nolint:gosec // signed i64 should always be enough
}
