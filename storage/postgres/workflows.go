package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/sknv/floww"
)

// InsertWorkflow creates a new workflow, schedules its first workflow task and returns an upserted workflow id.
// The workflow insert and task creation are executed in a single transaction.
func (s *Storage) InsertWorkflow(
	ctx context.Context,
	txer floww.TxBeginner,
	name string,
	id uuid.UUID,
	idempotencyKey uuid.UUID,
	input any,
	options floww.WorkflowOptions,
) (uuid.UUID, error) {
	var upsertedID uuid.UUID

	err := pgx.BeginFunc(ctx, txer, func(tx pgx.Tx) error {
		var txErr error

		// Insert a workflow first
		upsertedID, txErr = s.insertWorkflow(
			ctx,
			tx,
			name,
			id,
			idempotencyKey,
			input,
			options.Priority(),
			options.MaxAttempts(),
			options.StuckTimeoutMillis(),
		)
		if txErr != nil {
			return fmt.Errorf("insert workflow: %w", txErr)
		}

		// Insert a corresponding task
		taskID := uuid.Must(uuid.NewV7())

		if txErr = s.insertWorkflowTask(ctx, tx, taskID, id, options.ScheduledAt()); txErr != nil {
			return fmt.Errorf("insert workflow task: %w", txErr)
		}

		return nil
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("exec transaction: %w", err)
	}

	return upsertedID, nil
}

// insertWorkflow inserts a workflow record into storage and returns an upserted workflow id.
//
// It encodes the input payload (if provided) and persists execution parameters.
//
// IMPORTANT:
// - Intended to be called inside a transaction together with task creation.
// - Uses idempotency key to guarantee safe retries (ON CONFLICT DO UPDATE).
func (s *Storage) insertWorkflow(
	ctx context.Context,
	queryer floww.QueryRower,
	name string,
	id uuid.UUID,
	idempotencyKey uuid.UUID,
	input any,
	priority int,
	maxAttempts uint,
	stuckTimeoutMillis int64,
) (uuid.UUID, error) {
	var (
		inputBytes []byte
		upsertedID uuid.UUID
	)

	if input != nil {
		var err error

		inputBytes, err = s.encoder.Encode(input)
		if err != nil {
			return uuid.Nil, fmt.Errorf("encode input: %w", err)
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
		ON CONFLICT (idempotency_key) DO UPDATE
		SET id = floww_workflows.id
		RETURNING id
	`

	err := queryer.QueryRow(
		ctx,
		sql,
		id,
		idempotencyKey,
		name,
		inputBytes,
		priority,
		maxAttempts,
		stuckTimeoutMillis,
	).Scan(
		&upsertedID,
	)
	if err != nil {
		return uuid.Nil, fmt.Errorf("exec workflow inserting query: %w", err)
	}

	return upsertedID, nil
}

// historyEventRecord holds data to construct an activity history event.
type historyEventRecord struct {
	IdempotencyKey uuid.UUID
	Output         []byte
}

// ToHistoryEvent converts a raw DB record into an activity history event.
//
// IMPORTANT:
// - Decoding is deferred to the provided decoder to allow custom serialization formats.
// - Assumes Output is encoded with the same encoder/decoder pair.
func (e historyEventRecord) ToHistoryEvent(decoder floww.Decoder) floww.Event {
	return floww.NewEvent(e.IdempotencyKey, e.Output, decoder)
}

// ListHistoryEventsForWorkflow returns all completed activity outputs for a workflow,
// mapped by activity idempotency key. Results are ordered by activity ID.
func (s *Storage) ListHistoryEventsForWorkflow(ctx context.Context, id uuid.UUID) (floww.Events, error) {
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

	historyEvents := make(floww.Events)

	for rows.Next() {
		var historyEvent historyEventRecord

		err = rows.Scan(
			&historyEvent.IdempotencyKey,
			&historyEvent.Output,
		)
		if err != nil {
			return nil, fmt.Errorf("scan workflow history event: %w", err)
		}

		historyEvents[historyEvent.IdempotencyKey] = historyEvent.ToHistoryEvent(s.Decoder())
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
