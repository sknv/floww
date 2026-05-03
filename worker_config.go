package floww

import "time"

type (
	// PollConfig configures the poller loop.
	PollConfig struct {
		BatchSize    uint          // number of jobs to claim per poll
		Concurrency  int           // max in-flight jobs
		PollInterval time.Duration // sleep when no jobs claimed
	}

	// ProcessingConfig configures job processing.
	ProcessingConfig struct {
		DbTimeout      time.Duration // database timeout for background operations
		DefaultBackoff time.Duration // default job backoff
	}

	// CleanupConfig configures cold and dead jobs cleaning process.
	CleanupConfig struct {
		DbTimeout         time.Duration // database timeout for background operations
		RetentionInterval time.Duration // time to keep jobs in storage
		CleanupBatchSize  uint          // how many records should we delete at once
	}

	// WorkerConfig aggregates all configuration sections for a workflow or activity worker.
	WorkerConfig struct {
		Poll        PollConfig
		Processing  ProcessingConfig
		ColdCleanup CleanupConfig
		DeadCleanup CleanupConfig
	}
)

// BackoffCalculator defines a function to calculate backoff for retries.
type BackoffCalculator func(attempt uint) time.Duration
