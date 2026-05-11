package floww

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Storage defines the persistence contract required by workflow and activity workers.
//
//nolint:interfacebloat // single unit
type Storage interface {
	InsertWorkflow(
		ctx context.Context,
		txer TxBeginner,
		name string,
		id uuid.UUID,
		idempotencyKey uuid.UUID,
		input any,
		options WorkflowOptions,
	) (uuid.UUID, error)
	ListHistoryEventsForWorkflow(ctx context.Context, id uuid.UUID) (Events, error)
	CompleteWorkflow(ctx context.Context, id uuid.UUID) error
	DeleteColdWorkflows(ctx context.Context, cutoffDate time.Time, limit uint) (uint, error)
	DeleteDeadWorkflows(ctx context.Context, cutoffDate time.Time, limit uint) (uint, error)

	InsertActivity(
		ctx context.Context,
		name string,
		activityID uuid.UUID,
		activityIdempotencyKey uuid.UUID,
		workflowID uuid.UUID,
		input any,
		options ActivityOptions,
	) error
	ListActiveActivities(ctx context.Context, activities []string, batchSize uint) ([]ActivityRecord, error)
	CompleteActivity(
		ctx context.Context,
		activityID uuid.UUID,
		workflowID uuid.UUID,
		output any,
	) error
	ReScheduleActivity(
		ctx context.Context,
		id uuid.UUID,
		scheduledAt time.Time,
		errorMessage string,
	) error
	FailActivity(
		ctx context.Context,
		activityID uuid.UUID,
		workflowID uuid.UUID,
		errorMessage string,
	) error

	ListActiveWorkflowTasks(ctx context.Context, workflows []string, batchSize uint) ([]WorkflowTaskRecord, error)
	CompleteWorkflowTask(ctx context.Context, workflowTaskID uuid.UUID, workflowID uuid.UUID) error
	ReScheduleWorkflowTask(
		ctx context.Context,
		workflowTaskID uuid.UUID,
		workflowID uuid.UUID,
		scheduledAt time.Time,
		errorMessage string,
	) error
	FailWorkflowTask(
		ctx context.Context,
		workflowTaskID uuid.UUID,
		workflowID uuid.UUID,
		errorMessage string,
	) error

	InsertSignal(
		ctx context.Context,
		txer TxBeginner,
		workflowID uuid.UUID,
		signalID uuid.UUID,
		signalIdempotencyKey uuid.UUID,
		signalName string,
		signalInput any,
	) error
	ListWorkflowSignals(ctx context.Context, workflowID uuid.UUID) (Events, error)

	Decoder() Decoder
}

// Decoder is a function that deserialises raw bytes into a typed value.
type Decoder func(data []byte, v any) error
