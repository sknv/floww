package postgres

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/sknv/floww"
)

// insertWorkflowTask creates a workflow task derived from the workflow record.
//
// It copies priority and timeout configuration from the workflow at insert time.
//
// IMPORTANT:
// - Must be called inside a transaction together with workflow creation or progression.
// - Uses INSERT ... SELECT to ensure workflow existence.
// - ON CONFLICT DO NOTHING makes this operation idempotent for the same task ID.
// - If the workflow does not exist, no row is inserted (no explicit error).
func (s *Storage) insertWorkflowTask(
	ctx context.Context,
	execer floww.Execer,
	taskID uuid.UUID,
	workflowID uuid.UUID,
	scheduledAt time.Time,
) error {
	const sql = `
		INSERT INTO floww_workflow_tasks (
		  id,
		  workflow_id,
		  priority,
		  stuck_timeout_millis,
		  scheduled_at
		)
		SELECT
		  $1,
		  w.id,
		  w.priority,
		  w.stuck_timeout_millis,
		  $3
		FROM floww_workflows AS w
		WHERE w.id = $2
		ON CONFLICT (id) DO NOTHING
	`

	_, err := execer.Exec(
		ctx,
		sql,
		taskID,
		workflowID,
		scheduledAt,
	)
	if err != nil {
		return fmt.Errorf("exec workflow task inserting query: %w", err)
	}

	return nil
}

const _fetchWorkflowTasksSQL = `
	WITH pre_candidates AS (
	  (
	    SELECT id, priority, scheduled_at
	    FROM floww_workflow_tasks
	    WHERE status = $1
	      AND scheduled_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $3
	  )
	  UNION ALL
	  (
	    SELECT id, priority, scheduled_at
	    FROM floww_workflow_tasks
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
	),
	updated_tasks AS (
	  UPDATE floww_workflow_tasks AS t
	  SET status = $2,
	      run_at = now(),
	      stuck_at = now() + (stuck_timeout_millis * interval '1 millisecond')
	  FROM candidates
	  WHERE t.id = candidates.id
	  RETURNING t.id,
	            t.workflow_id,
	            t.status,
	            t.scheduled_at,
	            t.run_at,
	            t.stuck_at,
	            t.completed_at,
	            t.created_at,
	            t.updated_at
	)

	SELECT
	  ut.id AS task_id,
	  ut.status AS task_status,
	  ut.scheduled_at AS task_scheduled_at,
	  ut.run_at AS task_run_at,
	  ut.stuck_at AS task_stuck_at,
	  ut.completed_at AS task_completed_at,
	  ut.created_at AS task_created_at,
	  ut.updated_at AS task_updated_at,

	  w.id AS workflow_id,
	  w.idempotency_key AS workflow_idempotency_key,
	  w.name AS workflow_name,
	  w.status AS workflow_status,
	  w.input AS workflow_input,
	  w.output AS workflow_output,
	  w.priority AS workflow_priority,
	  w.attempts AS workflow_attempts,
	  w.max_attempts AS workflow_max_attempts,
	  w.stuck_timeout_millis AS workflow_stuck_timeout_millis,
	  w.completed_at AS workflow_completed_at,
	  w.error_message AS workflow_error_message,
	  w.created_at AS workflow_created_at,
	  w.updated_at AS workflow_updated_at
	FROM updated_tasks AS ut
	  JOIN floww_workflows AS w ON w.id = ut.workflow_id
`

// ListActiveWorkflowTasks fetches a batch of workflow tasks ready for execution.
// It selects pending or stuck tasks, locks them, marks them as running,
// and returns both task and associated workflow data.
func (s *Storage) ListActiveWorkflowTasks(ctx context.Context, batchSize uint) ([]floww.WorkflowTaskRecord, error) {
	rows, err := s.db.Query(
		ctx, _fetchWorkflowTasksSQL, floww.WorkflowTaskStatusPending, floww.WorkflowTaskStatusRunning, batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("query workflow tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]floww.WorkflowTaskRecord, 0, batchSize)

	for rows.Next() {
		var task floww.WorkflowTaskRecord

		err = rows.Scan(
			&task.ID,
			&task.Status,
			&task.ScheduledAt,
			&task.RunAt,
			&task.StuckAt,
			&task.CompletedAt,
			&task.CreatedAt,
			&task.UpdatedAt,

			&task.Workflow.ID,
			&task.Workflow.IdempotencyKey,
			&task.Workflow.Name,
			&task.Workflow.Status,
			&task.Workflow.Input,
			&task.Workflow.Output,
			&task.Workflow.Priority,
			&task.Workflow.Attempts,
			&task.Workflow.MaxAttempts,
			&task.Workflow.StuckTimeoutMillis,
			&task.Workflow.CompletedAt,
			&task.Workflow.ErrorMessage,
			&task.Workflow.CreatedAt,
			&task.Workflow.UpdatedAt,
		)
		if err != nil {
			log.Printf("[Floww][ERROR] Failed to scan workflow task: %v", err)

			continue
		}

		tasks = append(tasks, task)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over workflow tasks: %w", err)
	}

	return tasks, nil
}

// CompleteWorkflowTask marks the workflow task as completed and clears
// the workflow error message. Returns an error if the task does not exist.
func (s *Storage) CompleteWorkflowTask(ctx context.Context, workflowTaskID uuid.UUID, workflowID uuid.UUID) error {
	const sql = `
		WITH updated_task AS (
		  UPDATE floww_workflow_tasks
		  SET status = $3,
		      completed_at = now()
		  WHERE id = $1
		)

		UPDATE floww_workflows
		SET error_message = NULL
		WHERE id = $2
	`

	cmd, err := s.db.Exec(ctx, sql, workflowTaskID, workflowID, floww.WorkflowTaskStatusCompleted)
	if err != nil {
		return fmt.Errorf("exec workflow task completing query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("workflow task with id '%s' was not marked as completed", workflowTaskID)
	}

	return nil
}

// ReScheduleWorkflowTask moves the task back to pending state with a new schedule time,
// increments workflow attempts, and updates the workflow error message.
func (s *Storage) ReScheduleWorkflowTask(
	ctx context.Context,
	workflowTaskID uuid.UUID,
	workflowID uuid.UUID,
	scheduledAt time.Time,
	errorMessage string,
) error {
	const sql = `
		WITH updated_task AS (
		  UPDATE floww_workflow_tasks
		  SET status = $3,
		      scheduled_at = $4
		  WHERE id = $1
		)

		UPDATE floww_workflows
		SET attempts = attempts + 1,
		    error_message = $5
		WHERE id = $2
	`

	cmd, err := s.db.Exec(
		ctx,
		sql,
		workflowTaskID,
		workflowID,
		floww.WorkflowTaskStatusPending,
		scheduledAt,
		errorMessage,
	)
	if err != nil {
		return fmt.Errorf("exec workflow task rescheduling query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("workflow task with id '%s' was not rescheduled", workflowTaskID)
	}

	return nil
}

// FailWorkflowTask marks the workflow task as failed and also fails the associated workflow.
// The operation is executed in a transaction to ensure consistency.
func (s *Storage) FailWorkflowTask(
	ctx context.Context,
	workflowTaskID uuid.UUID,
	workflowID uuid.UUID,
	errorMessage string,
) error {
	err := pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if txErr := s.failWorkflowTask(ctx, tx, workflowTaskID); txErr != nil {
			return fmt.Errorf("fail workflow task tx: %w", txErr)
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

// failWorkflowTask marks a workflow task as failed.
//
// It updates the task status and completion timestamp.
//
// IMPORTANT:
// - Intended to be used within a transaction together with workflow failure.
// - Does not update the workflow itself (caller must handle that).
// - Returns an error if the task does not exist.
func (s *Storage) failWorkflowTask(ctx context.Context, execer floww.Execer, id uuid.UUID) error {
	const sql = `
		UPDATE floww_workflow_tasks
		SET status = $2,
		    completed_at = now()
		WHERE id = $1
	`

	cmd, err := execer.Exec(ctx, sql, id, floww.WorkflowTaskStatusFailed)
	if err != nil {
		return fmt.Errorf("exec workflow task failing query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("workflow task with id '%s' was not marked as failed", id)
	}

	return nil
}
