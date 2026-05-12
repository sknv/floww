package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sknv/floww"
	"github.com/sknv/floww/storage/postgres"
)

func main() {
	// Database connection
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create connection pool
	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Fatal("Failed to parse database URL:", err)
	}

	// Configure pool settings
	poolConfig.MaxConns = 20
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = 1 * time.Hour
	poolConfig.MaxConnIdleTime = 30 * time.Minute

	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Test connection
	if err = db.Ping(ctx); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	// Register activities and workflows
	activityRegistry := floww.NewActivityRegistry()
	workflowRegistry := floww.NewWorkflowRegistry()

	RegisterWorkflow(activityRegistry, workflowRegistry)

	// Use the default Postgres storage
	storage := postgres.NewStorage(db)

	// Create workers with custom configuration
	config := &floww.WorkerConfig{
		Poll: floww.PollConfig{
			BatchSize:    5,
			Concurrency:  5,
			PollInterval: time.Second * 3,
		},
		Processing: floww.ProcessingConfig{
			DbTimeout:      time.Second * 10,
			DefaultBackoff: time.Second * 30,
		},
		ColdCleanup: floww.CleanupConfig{
			DbTimeout:         time.Second * 30,
			RetentionInterval: time.Minute * 5,
			CleanupBatchSize:  5,
		},
		DeadCleanup: floww.CleanupConfig{
			DbTimeout:         time.Second * 30,
			RetentionInterval: time.Minute * 10,
			CleanupBatchSize:  5,
		},
	}

	activityWorker := floww.NewActivityWorker(storage, activityRegistry, config)
	workflowWorker := floww.NewWorkflowWorker(storage, workflowRegistry, config)

	// Start the workers
	activityWorker.Start(ctx)
	log.Println("Activity worker started successfully")

	workflowWorker.Start(ctx)
	log.Println("Workflow worker started successfully")

	// Example: start order workflow
	if err = EnqueueOrderWorkflow(ctx, storage, db); err != nil {
		log.Printf("Failed to enqueue example workflow: %v", err)
	}

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down process...")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	var wg sync.WaitGroup

	wg.Go(func() {
		if err := activityWorker.Stop(shutdownCtx); err != nil {
			log.Printf("Failed to stop activity worker: %v", err)
		}
	})

	wg.Go(func() {
		if err := workflowWorker.Stop(shutdownCtx); err != nil {
			log.Printf("Failed to stop workflow worker: %v", err)
		}
	})

	wg.Wait()

	log.Println("Process stopped")
}
