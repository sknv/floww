# floww

A lightweight, PostgreSQL-backed durable workflow engine for Go. Workflows are long-running orchestrations that coordinate a sequence of **activities** — individual units of work. Both are stored durably in Postgres, so they survive process restarts. The engine guarantees at-least-once execution, automatic retries with configurable backoff, stuck-task recovery, and idempotent scheduling.

Uses a short-polling mechanism for fetching updates, making it compatible with connection poolers in transaction mode.

## How it works

floww follows the **event-sourcing execution model** (similar to Temporal/Cadence, but self-hosted and dependency-light):

1. When a workflow is enqueued a **workflow task** is immediately scheduled.
2. The **WorkflowWorker** picks up the task and runs the workflow handler function from the very beginning.
3. Inside the handler, calls to `ExecuteActivity` check a **history** of already-completed activities loaded from Postgres.
   - If an activity is already in history its stored output is returned immediately.
   - If not, the activity is scheduled in Postgres and the workflow is **suspended** by returning `ErrWorkflowSuspended`.
4. The **ActivityWorker** picks up the pending activity, executes its handler, writes the output to Postgres, and schedules a new workflow task to resume the workflow.
5. Steps 2–4 repeat until the workflow handler returns without suspending, at which point the workflow is marked completed.

Because the handler always re-executes from the top, workflow logic must be **deterministic** — side effects belong in activities, not in the workflow function itself.

```
EnqueueWorkflow
      │
      ▼
[floww_workflows] ──► [floww_workflow_tasks] (pending)
                                │
                        WorkflowWorker picks up
                                │
                    Run handler (replay history)
                       │              │
               activity in        activity NOT in
                history               history
                   │                    │
             return output         InsertActivity
                                        │
                               [floww_activities] (pending)
                                        │
                               ActivityWorker picks up
                                        │
                               Execute handler
                                        │
                               CompleteActivity +
                               schedule new workflow task
                                        │
                               WorkflowWorker resumes …
```

## Features

- **Durable execution** — workflows and activities survive crashes and restarts
- **Event-sourcing replay** — workflow handlers re-execute deterministically using completed activity history
- **Typed generics API** — `Workflow[I]`, `Activity[I, O]`, and `Future[O]` carry compile-time types
- **At-least-once delivery** — both workers recover stuck tasks automatically
- **Idempotent scheduling** — duplicate workflow or activity submissions with the same key are silently ignored
- **Priority scheduling** — higher-priority work is always picked up first
- **Delayed execution** — schedule workflows and activities for a future time
- **Configurable retries and backoff** — per-handler custom `BackoffCalculator`; unrecoverable errors skip retries immediately
- **Concurrent processing** — bounded concurrency via `errgroup`
- **Graceful shutdown** — workers drain in-flight tasks before stopping
- **Pluggable encoder** — JSON by default; swap in any `Encoder`
- **Custom storage** — implement the `Storage` interface to use a different backend
- **Periodic cleanup** — built-in helpers prune old completed and failed records

## Requirements

- Go 1.26+
- PostgreSQL with the `uuidv7()` function
- [`pgx/v5`](https://github.com/jackc/pgx)

## Installation

```bash
go get github.com/sknv/floww
```

## Database Setup

Apply the migration to create the three required tables and their indexes:

```bash
psql -d your_database -f init_floww.up.sql
```

The migration creates:

- `floww_workflows` — one row per workflow instance
- `floww_workflow_tasks` — resumption tasks that drive workflow re-execution
- `floww_activities` — individual activity runs with input/output storage

All three tables have partial indexes optimised for the polling queries and an `updated_at` trigger.

## Quick Start

Take a look in the `example` folder.

## Defining Workflows and Activities

### Workflows

A workflow is a generic struct parameterised by its input type. The handler receives a `*WorkflowContext` and the typed input.

```go
type ProcessOrderInput struct {
    OrderID string
    Amount  float64
}

var processOrder = floww.NewWorkflow[ProcessOrderInput]("process-order")

func processOrderHandler(ctx *floww.WorkflowContext, input ProcessOrderInput) error {
    // Step 1 — schedules the activity and suspends if it hasn't run yet
    receipt, err := floww.ExecuteActivity(ctx, chargeCard, ChargeInput{Amount: input.Amount})
    if err != nil {
        return err
    }

    // Step 2 — only reached once chargeCard has completed
    _, err = floww.ExecuteActivity(ctx, sendReceipt, ReceiptInput{
        OrderID:   input.OrderID,
        ReceiptID: receipt.ID,
    }, floww.WithActivityMaxAttempts(3))

    return err
}
```

### Activities

An activity is parameterised by its input and output types. The handler is a plain Go function.

```go
type ChargeInput  struct{ Amount float64 }
type ChargeOutput struct{ ID     string  }

var chargeCard = floww.NewActivity[ChargeInput, ChargeOutput]("charge-card")

floww.RegisterActivity(activityRegistry, chargeCard,
    func(ctx context.Context, input ChargeInput) (ChargeOutput, error) {
        id, err := paymentProvider.Charge(input.Amount)
        if err != nil {
            // Permanent failure — skip all remaining retries
            return ChargeOutput{}, floww.Unrecoverable(err)
        }

        return ChargeOutput{ID: id}, nil
    },
    floww.WithActivityBackoffCalculator(func(attempt uint) time.Duration {
        return time.Duration(attempt) * time.Minute
    }),
)
```

### Async activities and `Future`

`ExecuteActivity` suspends the workflow when the activity is pending. Use `ExecuteActivityAsync` to schedule multiple activities concurrently and collect their results later:

```go
func fanOutHandler(ctx *floww.WorkflowContext, input MyInput) error {
    // Both activities are scheduled in the same workflow task execution
    futA, err := floww.ExecuteActivityAsync(ctx, activityA, inputA)
    if err != nil { return err }

    futB, err := floww.ExecuteActivityAsync(ctx, activityB, inputB)
    if err != nil { return err }

    // Workflow suspends here until both futures are in history
    resultA, err := futA.Get()
    if err != nil { return err } // returns ErrWorkflowSuspended if still pending

    resultB, err := futB.Get()
    if err != nil { return err }

    _ = resultA
    _ = resultB

    return nil
}
```

## Enqueueing Workflows

```go
err = floww.EnqueueWorkflow(ctx, storage, db, myWorkflow,
    idempotencyKey, // uuid.UUID — reuse to deduplicate
    input,
    floww.WithWorkflowPriority(10),
    floww.WithWorkflowMaxAttempts(5),
    floww.WithWorkflowStuckTimeout(10*time.Minute),
    floww.WithWorkflowScheduledAt(time.Now().Add(1*time.Hour)),
)
```

Pass a `pgx.Tx` as the `txer` argument to enqueue atomically within your own transaction — for example, alongside the database write that triggered the workflow.

### Workflow options

| Option | Default | Description |
|---|---|---|
| `WithWorkflowPriority(n)` | `0` | Higher values are processed first |
| `WithWorkflowMaxAttempts(n)` | `1` | Max workflow task executions before the workflow is failed |
| `WithWorkflowStuckTimeout(d)` | `5m` | How long a running task may be silent before recovery |
| `WithWorkflowScheduledAt(t)` | `now()` | Earliest time the first task will be picked up |

### Activity options

| Option | Default | Description |
|---|---|---|
| `WithActivityPriority(n)` | `0` | Higher values are processed first |
| `WithActivityMaxAttempts(n)` | `1` | Max attempts before the activity (and workflow) fails |
| `WithActivityStuckTimeout(d)` | `5m` | How long a running activity may be silent before recovery |
| `WithActivityScheduledAt(t)` | `now()` | Earliest time the activity will be picked up |

## Starting and Stopping Workers

```go
// Process all workflow and activity types
workflowWorker.Start(ctx)
activityWorker.Start(ctx)

// Process only specific named workflows / activities
workflowWorker.Start(ctx, "process-order", "onboard-user")
activityWorker.Start(ctx, "charge-card", "send-email")

// Graceful shutdown — waits for in-flight tasks to finish
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := workflowWorker.Stop(shutdownCtx); err != nil {
    log.Printf("workflow worker did not stop cleanly: %v", err)
}
if err := activityWorker.Stop(shutdownCtx); err != nil {
    log.Printf("activity worker did not stop cleanly: %v", err)
}
```

## Configuration

Both workers share the same `WorkerConfig` shape. Use the defaults and override what you need:

```go
cfg := &floww.WorkerConfig{
    Poll: floww.PollConfig{
        BatchSize:    20,
        Concurrency:  20,
        PollInterval: 500 * time.Millisecond,
    },
    Processing: floww.ProcessingConfig{
        DbTimeout:      10 * time.Second,
        DefaultBackoff: 30 * time.Second,
    },
    ColdCleanup: floww.CleanupConfig{ // completed workflows
        DbTimeout:         30 * time.Second,
        RetentionInterval: 7 * 24 * time.Hour,
        CleanupBatchSize:  10_000,
    },
    DeadCleanup: floww.CleanupConfig{ // failed workflows
        DbTimeout:         30 * time.Second,
        RetentionInterval: 90 * 24 * time.Hour,
        CleanupBatchSize:  10_000,
    },
}

workflowWorker := floww.NewWorkflowWorker(storage, workflowRegistry, cfg)
activityWorker  := floww.NewActivityWorker(storage, activityRegistry, cfg)
```

### Default values

| Setting | Default |
|---|---|
| `Poll.BatchSize` | `10` |
| `Poll.Concurrency` | `10` |
| `Poll.PollInterval` | `1s` |
| `Processing.DbTimeout` | `10s` |
| `Processing.DefaultBackoff` | `30s` |
| `ColdCleanup.RetentionInterval` | `7 days` |
| `DeadCleanup.RetentionInterval` | `90 days` |
| `*.CleanupBatchSize` | `10 000` |

## Error Handling

### Retriable errors

Returning a plain error from an activity or workflow handler reschedules the task after the configured backoff. The task is retried until `MaxAttempts` is exhausted, at which point the activity — and the entire workflow — transitions to `failed`.

### Unrecoverable errors

Wrap an error in `floww.Unrecoverable` to skip all remaining retries immediately:

```go
if isPermanentBusinessError(err) {
    return floww.Unrecoverable(err)
}
```

### Custom backoff

```go
floww.RegisterWorkflow(registry, myWorkflow, handler,
    floww.WithWorkflowBackoffCalculator(func(attempt uint) time.Duration {
        // Exponential backoff capped at 10 minutes
        d := time.Duration(1<<attempt) * time.Second
        if d > 10*time.Minute {
            d = 10 * time.Minute
        }

        return d
    }),
)
```

## Cleanup

Completed and failed workflows accumulate over time. Call the cleanup helpers periodically from a cron job or a goroutine. Setting `RetentionInterval` to `0` or a negative value disables cleanup for that category.

```go
if err := workflowWorker.CleanColdWorkflows(ctx); err != nil {
    log.Printf("cold workflow cleanup failed: %v", err)
}

if err := workflowWorker.CleanDeadWorkflows(ctx); err != nil {
    log.Printf("dead workflow cleanup failed: %v", err)
}
```

Activity records are removed automatically via `ON DELETE CASCADE` when their parent workflow is deleted.

## Custom Encoder

The default encoder is JSON. Replace it when creating the storage:

```go
storage := flowwpostgres.NewStorage(db,
    flowwpostgres.WithStorageQueueEncoder(myMsgpackEncoder{}),
)
```

Any type satisfying the `Encoder` interface works:

```go
type Encoder interface {
    Encode(v any) ([]byte, error)
    Decode(data []byte, v any) error
}
```

## Custom Storage

Implement the `Storage` interface to use a different database backend:

## Workflow Lifecycle

```
EnqueueWorkflow
      │
      ▼
   running ──► workflow task fires ──► handler suspends (activity pending)
      │                 ▲                        │
      │                 │                activity completes
      │                 └──── new workflow task scheduled
      │
      ├──► completed   (handler returned nil with no suspension)
      └──► failed      (attempts >= maxAttempts  OR  Unrecoverable error)
```

## Writing Correct Workflow Handlers

Because the handler re-executes from scratch on every workflow task, a few rules apply:

- **Put all side effects in activities.** Network calls, database writes, random number generation, and anything non-deterministic must live inside an activity handler, not in the workflow function.
- **Do not use wall-clock time inside the handler.** Use `WithActivityScheduledAt` to delay an activity instead.
- **Keep the step order stable.** Activity idempotency keys are derived by hashing the activity name against the workflow ID, so reordering or removing steps across deployments would cause history mismatches for in-flight workflows.

## License

MIT
