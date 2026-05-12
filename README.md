# floww

A lightweight, PostgreSQL-backed durable workflow engine for Go. Workflows are long-running orchestrations that coordinate a sequence of **activities** — individual units of work — and can pause to wait for external **signals**. Everything is stored durably in Postgres, so it survives process restarts. The engine guarantees at-least-once execution, automatic retries with configurable backoff, stuck-task recovery, and idempotent scheduling.

Uses a short-polling mechanism for fetching updates, making it compatible with connection poolers in transaction mode.

## How it works

floww follows the **event-sourcing execution model** (similar to Temporal/Cadence, but self-hosted and dependency-light):

1. When a workflow is enqueued a **workflow task** is immediately scheduled.
2. The **WorkflowWorker** picks up the task and runs the workflow handler function from the very beginning.
3. Inside the handler, calls to `ExecuteActivity` and `ReceiveSignal` check a **history** of already-completed events loaded from Postgres.
   - If the event is already in history its stored output is returned immediately.
   - If not, the activity is scheduled (or the signal is awaited) and the workflow is **suspended** by returning `ErrWorkflowSuspended`.
4. The **ActivityWorker** picks up the pending activity, executes its handler, writes the output to Postgres, and schedules a new workflow task to resume the workflow.
5. An external caller can unblock a waiting workflow at any time by calling `SendSignal`, which also schedules a new workflow task.
6. Steps 2–5 repeat until the workflow handler returns without suspending, at which point the workflow is marked completed.

Because the handler always re-executes from the top, workflow logic must be **deterministic** — side effects belong in activities, not in the workflow function itself.

```
EnqueueWorkflow
      │
      ▼
[floww_workflows] ──► [floww_workflow_tasks] (pending)
                                │
                        WorkflowWorker picks up
                                │
              Run handler (replay history + signals)
                 │                            │
         event in history             event NOT in history
                 │                            │
           return value               schedule activity
                                      OR await signal
                                             │
                              ┌──────────────┴──────────────┐
                              │                             │
                     ActivityWorker                   SendSignal (external)
                     completes activity               inserts signal record
                              │                             │
                      CompleteActivity +            schedule workflow task
                      schedule workflow task                │
                              └──────────────┬─────────────┘
                                             │
                                    WorkflowWorker resumes …
```

## Features

- **Durable execution** — workflows, activities, and signals survive crashes and restarts
- **Event-sourcing replay** — workflow handlers re-execute deterministically using a unified history of completed activities and received signals
- **Signals** — external events that pause a workflow until a named message arrives, then resume it
- **Typed generics API** — `Workflow[I]`, `Activity[I, O]`, `Signal[I]`, and `Future[O]` carry compile-time types
- **At-least-once delivery** — both workers recover stuck tasks automatically
- **Idempotent scheduling** — duplicate workflow, activity, or signal submissions with the same key are silently ignored
- **Priority scheduling** — higher-priority work is always picked up first
- **Delayed execution** — schedule workflows and activities for a future time
- **Configurable retries and backoff** — per-handler custom `BackoffCalculator`; unrecoverable errors skip retries immediately
- **Concurrent processing** — bounded concurrency via semaphore
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

Apply the migration to create the required tables and their indexes:

```bash
psql -d your_database -f init_floww.up.sql
```

The migration creates:

- `floww_workflows` — one row per workflow instance
- `floww_workflow_tasks` — resumption tasks that drive workflow re-execution
- `floww_activities` — individual activity runs with input/output storage
- `floww_signals` — durable signal records delivered to workflow instances

All tables have partial indexes optimised for the polling queries and an `updated_at` trigger.

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

### Running the same activity multiple times

Activity idempotency keys are derived from the activity name and the workflow ID, so the same activity cannot be inserted twice by default. When you need to run the same activity more than once — for example, inside a loop — use `WithActivityIdempotencyKey` or `Activity.IdempotencyKeyByString` to give each invocation a distinct key:

```go
func batchHandler(ctx *floww.WorkflowContext, input BatchInput) error {
    for i, item := range input.Items {
        // Derive a unique key per iteration so each run is tracked independently
        key := processItem.IdempotencyKeyByString(ctx.WorkflowID(), strconv.Itoa(i))

        _, err := floww.ExecuteActivity(ctx, processItem, item,
            floww.WithActivityIdempotencyKey(key),
        )
        if err != nil {
            return err
        }
    }
    return nil
}
```

## Signals

Signals are durable, named messages that can be sent to a running workflow from outside. A workflow can pause and wait for a signal, just like it waits for an activity. When the signal arrives the workflow is woken up; on the next replay the signal value is returned immediately from history.

### Defining a signal

```go
type ApprovalPayload struct {
    ApprovedBy string
    Note       string
}

var approvalSignal = floww.NewSignal[ApprovalPayload]("order-approved")
```

### Receiving a signal in a workflow handler

```go
func processOrderHandler(ctx *floww.WorkflowContext, input OrderInput) error {
    // Step 1: charge the card
    receipt, err := floww.ExecuteActivity(ctx, chargeCard, ChargeInput{Amount: input.Amount})
    if err != nil {
        return err
    }

    // Step 2: wait for a human approval — suspends until the signal arrives
    approval, err := floww.ReceiveSignal(ctx, approvalSignal)
    if err != nil {
        return err // ErrWorkflowSuspended until the signal is sent
    }

    // Step 3: fulfil the order — only reached after approval
    _, err = floww.ExecuteActivity(ctx, fulfillOrder, FulfillInput{
        ReceiptID:  receipt.ID,
        ApprovedBy: approval.ApprovedBy,
    })
    return err
}
```

Use `ReceiveSignalAsync` to wait for multiple signals concurrently, mirroring `ExecuteActivityAsync`:

```go
func reviewHandler(ctx *floww.WorkflowContext, input ReviewInput) error {
    futApproval, err := floww.ReceiveSignalAsync(ctx, approvalSignal)
    if err != nil { return err }

    futRejection, err := floww.ReceiveSignalAsync(ctx, rejectionSignal)
    if err != nil { return err }

    // Whichever signal arrives first will be present in history; the other
    // will still return ErrWorkflowSuspended.
    if approval, err := futApproval.Get(); err == nil {
        return floww.ExecuteActivity(ctx, approve, approval)
    }
    if rejection, err := futRejection.Get(); err == nil {
        return floww.ExecuteActivity(ctx, reject, rejection)
    }

    return ErrWorkflowSuspended // neither signal has arrived yet
}
```

### Sending a signal

```go
err := floww.SendSignal(ctx, storage, db, workflowID, approvalSignal,
    ApprovalPayload{ApprovedBy: "alice", Note: "looks good"},
)
```

Pass a `pgx.Tx` as the `txer` argument to send the signal atomically alongside your own database write:

```go
tx, _ := db.Begin(ctx)
defer tx.Rollback(ctx)

_, _ = tx.Exec(ctx, `UPDATE orders SET approved = true WHERE id = $1`, orderID)

err := floww.SendSignal(ctx, storage, tx, workflowID, approvalSignal,
    ApprovalPayload{ApprovedBy: "alice", Note: "looks good"},
)
if err != nil { ... }

tx.Commit(ctx)
```

### Signal idempotency

Each signal is identified by an idempotency key. By default the key is derived from the signal name and the workflow ID, so the same named signal can only be received once per workflow. To send multiple signals of the same type — for example, a stream of progress updates — pass a unique key per send using `WithSignalIdempotencyKey`:

```go
for i, update := range updates {
    key := progressSignal.IdempotencyKeyByString(workflowID, strconv.Itoa(i))

    err := floww.SendSignal(ctx, storage, db, workflowID, progressSignal, update,
        floww.WithSignalIdempotencyKey(key),
    )
    if err != nil { ... }
}
```

On the receiving side, use the matching key so each invocation resolves to a distinct history entry:

```go
for i := range expectedUpdates {
    key := progressSignal.IdempotencyKeyByString(ctx.WorkflowID(), strconv.Itoa(i))

    update, err := floww.ReceiveSignal(ctx, progressSignal,
        floww.WithSignalIdempotencyKey(key),
    )
    if err != nil {
        return err
    }
    _ = update
}
```

## Enqueueing Workflows

`EnqueueWorkflow` returns the ID of the workflow that was created (or the ID of the existing workflow if a duplicate idempotency key was submitted). Store it to send signals later.

```go
workflowID, err := floww.EnqueueWorkflow(ctx, storage, db, myWorkflow,
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
| `WithActivityIdempotencyKey(k)` | derived from name+workflow | Override the idempotency key (use when running the same activity more than once) |
| `WithActivityPriority(n)` | `0` | Higher values are processed first |
| `WithActivityMaxAttempts(n)` | `1` | Max attempts before the activity (and workflow) fails |
| `WithActivityStuckTimeout(d)` | `5m` | How long a running activity may be silent before recovery |
| `WithActivityScheduledAt(t)` | `now()` | Earliest time the activity will be picked up |

### Signal options

| Option | Default | Description |
|---|---|---|
| `WithSignalIdempotencyKey(k)` | derived from name+workflow | Override the idempotency key (use when sending the same signal more than once) |

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

Activity and signal records are removed automatically via `ON DELETE CASCADE` when their parent workflow is deleted.

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

Implement the `Storage` interface to use a different database backend.

## Workflow Lifecycle

```
EnqueueWorkflow
      │
      ▼
   running ──► workflow task fires ──► handler suspends
      │                 ▲              (activity pending OR signal not yet received)
      │                 │                        │
      │                 │         activity completes OR signal sent
      │                 └──────── new workflow task scheduled
      │
      ├──► completed   (handler returned nil with no suspension)
      └──► failed      (attempts >= maxAttempts  OR  Unrecoverable error)
```

## Writing Correct Workflow Handlers

Because the handler re-executes from scratch on every workflow task, a few rules apply:

- **Put all side effects in activities.** Network calls, database writes, random number generation, and anything non-deterministic must live inside an activity handler, not in the workflow function.
- **Do not use wall-clock time inside the handler.** Use `WithActivityScheduledAt` to delay an activity instead.
- **Keep the step order stable.** Activity and signal idempotency keys are derived from their name and the workflow ID. Reordering or removing steps across deployments would cause history mismatches for in-flight workflows.
- **Use `IdempotencyKeyByString` for repeated steps.** If you need to run the same activity or receive the same signal more than once, pass a unique discriminator (e.g. a loop index) so each invocation maps to a distinct history entry.

## License

MIT
