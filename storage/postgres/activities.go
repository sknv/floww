package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/sknv/floww"
)

// InsertActivity inserts a new activity record.
// It encodes the input payload (if provided) and stores scheduling and execution options.
// If an activity with the same idempotency key already exists, the insert is ignored.
func (s *Storage) InsertActivity(
	ctx context.Context,
	name string,
	activityID uuid.UUID,
	activityIdempotencyKey uuid.UUID,
	workflowID uuid.UUID,
	input any,
	options floww.ActivityOptions,
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
		INSERT INTO floww_activities (
		  id,
		  idempotency_key,
		  workflow_id,
		  name,
		  input,
		  priority,
		  max_attempts,
		  stuck_timeout_millis,
		  scheduled_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (idempotency_key) DO NOTHING
	`

	_, err := s.db.Exec(
		ctx,
		sql,
		activityID,
		activityIdempotencyKey,
		workflowID,
		name,
		inputBytes,
		options.Priority(),
		options.MaxAttempts(),
		options.StuckTimeoutMillis(),
		options.ScheduledAt(),
	)
	if err != nil {
		return fmt.Errorf("exec activity inserting query: %w", err)
	}

	return nil
}

const _fetchActivitiesSQL = `
	WITH pre_candidates AS (
	  (
	    SELECT id, priority, scheduled_at
	    FROM floww_activities
	    WHERE status = $1
	      AND scheduled_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $3
	  )
	  UNION ALL
	  (
	    SELECT id, priority, scheduled_at
	    FROM floww_activities
	    WHERE status = $2
	      AND stuck_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $3
	  )
	),
	candidates AS (
	  SELECT id
	  FROM pre_candidates
	  ORDER BY priority DESC, scheduled_at
	  LIMIT $3
	  FOR NO KEY UPDATE SKIP LOCKED
	)

	UPDATE floww_activities AS a
	SET status = $2,
	    attempts = attempts + 1,
	    run_at = now(),
	    stuck_at = now() + (stuck_timeout_millis * interval '1 millisecond')
	FROM candidates
	WHERE a.id = candidates.id
	RETURNING
	  a.id,
	  a.idempotency_key,
	  a.workflow_id,
	  a.name,
	  a.status,
	  a.input,
	  a.output,
	  a.priority,
	  a.attempts,
	  a.max_attempts,
	  a.stuck_timeout_millis,
	  a.scheduled_at,
	  a.run_at,
	  a.stuck_at,
	  a.completed_at,
	  a.error_message,
	  a.created_at,
	  a.updated_at
`

const _fetchActivitiesWithNamesSQL = `
	WITH pre_candidates AS (
	  (
	    SELECT id, priority, scheduled_at
	    FROM floww_activities
	    WHERE name = ANY($1)
		  AND status = $2
	      AND scheduled_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $4
	  )
	  UNION ALL
	  (
	    SELECT id, priority, scheduled_at
	    FROM floww_activities
	    WHERE name = ANY($1)
		  AND status = $3
	      AND stuck_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $4
	  )
	),
	candidates AS (
	  SELECT id
	  FROM pre_candidates
	  ORDER BY priority DESC, scheduled_at
	  LIMIT $4
	  FOR NO KEY UPDATE SKIP LOCKED
	)

	UPDATE floww_activities AS a
	SET status = $3,
	    attempts = attempts + 1,
	    run_at = now(),
	    stuck_at = now() + (stuck_timeout_millis * interval '1 millisecond')
	FROM candidates
	WHERE a.id = candidates.id
	RETURNING
	  a.id,
	  a.idempotency_key,
	  a.workflow_id,
	  a.name,
	  a.status,
	  a.input,
	  a.output,
	  a.priority,
	  a.attempts,
	  a.max_attempts,
	  a.stuck_timeout_millis,
	  a.scheduled_at,
	  a.run_at,
	  a.stuck_at,
	  a.completed_at,
	  a.error_message,
	  a.created_at,
	  a.updated_at
`

// ListActiveActivities fetches a batch of activities that are ready to run.
// It selects pending or stuck activities, locks them, marks them as running,
// increments attempt counters, and returns the updated records.
// If no activities specified tasks for all activities will be fetched.
//
//nolint:funlen // linear logic
func (s *Storage) ListActiveActivities(
	ctx context.Context, activities []string, batchSize uint,
) ([]floww.ActivityRecord, error) {
	var (
		sql  string
		args []any
	)

	if len(activities) > 0 {
		sql = _fetchActivitiesWithNamesSQL
		args = []any{
			activities,
			floww.ActivityStatusPending,
			floww.ActivityStatusRunning,
			batchSize,
		}
	} else {
		sql = _fetchActivitiesSQL
		args = []any{
			floww.ActivityStatusPending,
			floww.ActivityStatusRunning,
			batchSize,
		}
	}

	rows, err := s.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query activities: %w", err)
	}
	defer rows.Close()

	tasks := make([]floww.ActivityRecord, 0, batchSize)

	for rows.Next() {
		var activity floww.ActivityRecord

		err = rows.Scan(
			&activity.ID,
			&activity.IdempotencyKey,
			&activity.WorkflowID,
			&activity.Name,
			&activity.Status,
			&activity.Input,
			&activity.Output,
			&activity.Priority,
			&activity.Attempts,
			&activity.MaxAttempts,
			&activity.StuckTimeoutMillis,
			&activity.ScheduledAt,
			&activity.RunAt,
			&activity.StuckAt,
			&activity.CompletedAt,
			&activity.ErrorMessage,
			&activity.CreatedAt,
			&activity.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan activity: %w", err)
		}

		tasks = append(tasks, activity)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over activities: %w", err)
	}

	return tasks, nil
}

// CompleteActivity marks the activity as completed and schedules the next workflow task.
// The operation is executed in a transaction to ensure atomicity.
func (s *Storage) CompleteActivity(
	ctx context.Context,
	activityID uuid.UUID,
	workflowID uuid.UUID,
	output any,
) error {
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if txErr := s.completeActivity(ctx, tx, activityID, output); txErr != nil {
			return fmt.Errorf("complete activity tx: %w", txErr)
		}

		taskID := uuid.Must(uuid.NewV7())
		taskSchedule := time.Now()

		if txErr := s.insertWorkflowTask(ctx, tx, taskID, workflowID, taskSchedule); txErr != nil {
			return fmt.Errorf("insert workflow task tx: %w", txErr)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("exec transaction: %w", err)
	}

	return nil
}

// completeActivity updates an activity record as completed.
//
// It encodes the output payload (if provided), sets the status to completed,
// clears any previous error, and records completion timestamp.
//
// IMPORTANT:
// - Must be executed within a transaction when part of a larger workflow step.
// - Uses the provided execer (tx or db) to allow composition.
// - Fails if no rows are affected (i.e., activity does not exist).
func (s *Storage) completeActivity(
	ctx context.Context,
	execer floww.Execer,
	id uuid.UUID,
	output any,
) error {
	var outputBytes []byte

	if output != nil {
		var err error

		outputBytes, err = s.encoder.Encode(output)
		if err != nil {
			return fmt.Errorf("encode output: %w", err)
		}
	}

	const sql = `
		UPDATE floww_activities
		SET status = $2,
		    output = $3,
		    completed_at = now(),
		    error_message = NULL
		WHERE id = $1
	`

	cmd, err := execer.Exec(ctx, sql, id, floww.ActivityStatusCompleted, outputBytes)
	if err != nil {
		return fmt.Errorf("exec activity completing query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("activity with id '%s' was not marked as completed", id)
	}

	return nil
}

// ReScheduleActivity moves the activity back to pending state with a new schedule time
// and updates the error message describing the reason for rescheduling.
func (s *Storage) ReScheduleActivity(
	ctx context.Context,
	id uuid.UUID,
	scheduledAt time.Time,
	errorMessage string,
) error {
	const sql = `
		UPDATE floww_activities
		SET status = $2,
		    scheduled_at = $3,
		    error_message = $4
		WHERE id = $1
	`

	cmd, err := s.db.Exec(ctx, sql, id, floww.ActivityStatusPending, scheduledAt, errorMessage)
	if err != nil {
		return fmt.Errorf("exec activity rescheduling query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("activity with id '%s' was not rescheduled", id)
	}

	return nil
}

// FailActivity marks the activity as failed and also fails the associated workflow.
// The operation is executed in a transaction to keep both updates consistent.
func (s *Storage) FailActivity(
	ctx context.Context,
	activityID uuid.UUID,
	workflowID uuid.UUID,
	errorMessage string,
) error {
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if txErr := s.failActivity(ctx, tx, activityID, errorMessage); txErr != nil {
			return fmt.Errorf("fail activity tx: %w", txErr)
		}

		if txErr := s.failWorkflow(ctx, tx, workflowID, errorMessage); txErr != nil {
			return fmt.Errorf("fail workflow: %w", txErr)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("exec transaction: %w", err)
	}

	return nil
}

// failActivity marks an activity as failed.
//
// It sets the failure status, stores the error message, and records completion time.
//
// IMPORTANT:
// - Designed to be called inside a transaction together with workflow failure.
// - Does not validate current state transitions (caller responsibility).
// - Returns an error if the activity does not exist.
func (s *Storage) failActivity(
	ctx context.Context,
	execer floww.Execer,
	id uuid.UUID,
	errorMessage string,
) error {
	const sql = `
		UPDATE floww_activities
		SET status = $2,
		    error_message = $3,
		    completed_at = now()
		WHERE id = $1
	`

	cmd, err := execer.Exec(ctx, sql, id, floww.ActivityStatusFailed, errorMessage)
	if err != nil {
		return fmt.Errorf("exec activity failing query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("activity with id '%s' was not marked as failed", id)
	}

	return nil
}
