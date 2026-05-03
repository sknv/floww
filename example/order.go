package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/sknv/floww"
)

//
// Example activities
//

type (
	ChargeInput struct {
		UserID string
		Amount int
	}
	ChargeOutput struct {
		IsCharged     bool
		ChargedAmount int
	}
)

type (
	EmailInput struct {
		UserID        string
		ChargedAmount int
	}
	EmailOutput struct {
		IsSent bool
	}
)

const (
	chargeCardActivityName = "ChargeCard"
	sendEmailActivityName  = "SendEmail"
)

var (
	ChargeCardActivity = floww.NewActivity[ChargeInput, ChargeOutput](chargeCardActivityName)
	SendEmailActivity  = floww.NewActivity[EmailInput, EmailOutput](sendEmailActivityName)
)

//
// Example workflow
//

type OrderInput struct {
	UserID string
	Amount int
}

const orderWorkflowName = "OrderWorkflow"

var OrderWorkflow = floww.NewWorkflow[OrderInput](orderWorkflowName)

// RegisterWorkflow registers activities and workflows in the corresponding registries.
func RegisterWorkflow(activityRegistry *floww.ActivityRegistry, workflowRegistry *floww.WorkflowRegistry) {
	floww.RegisterActivity(activityRegistry, ChargeCardActivity, ChargeCard)
	floww.RegisterActivity(activityRegistry, SendEmailActivity, SendEmail,
		floww.WithActivityBackoffCalculator(calculateBackoff))

	floww.RegisterWorkflow(workflowRegistry, OrderWorkflow, RunOrderWorkflow)
}

// EnqueueOrderWorkflow schedules workflow processing.
func EnqueueOrderWorkflow(ctx context.Context, storage floww.Storage, txer floww.TxBeginner) error {
	if err := floww.EnqueueWorkflow(
		ctx,
		storage,
		txer,
		OrderWorkflow,
		uuid.Must(uuid.NewV7()),
		OrderInput{
			UserID: uuid.NewString(),
			Amount: rand.IntN(100),
		},
		floww.WithWorkflowMaxAttempts(3),
	); err != nil {
		return err
	}

	fmt.Println("Workflow", orderWorkflowName, "is scheduled successfully")

	return nil
}

//
// Actual logic
//

func RunOrderWorkflow(ctx *floww.WorkflowContext, in OrderInput) error {
	charged, err := floww.ExecuteActivity(
		ctx,
		ChargeCardActivity,
		activityIdempotencyKey(ctx.WorkflowID(), chargeCardActivityName),
		ChargeInput{
			UserID: in.UserID,
			Amount: in.Amount,
		},
		floww.WithActivityMaxAttempts(3),
	)
	if err != nil {
		return fmt.Errorf("charge card: %w", err)
	}

	// !!! Note that this line will be repeated during history replay !!!
	fmt.Println("Charge result is:", charged.IsCharged, "| user is:", in.UserID, "| workflow is", ctx.WorkflowID())

	if !charged.IsCharged {
		// Implement your custom error handling logic here
		fmt.Println("[ERROR] Can not charge customer", in.UserID)

		return floww.Unrecoverable(fmt.Errorf("can not charge customer %s", in.UserID))
	}

	// Wait for 1 minute before notifying
	_, err = floww.ExecuteActivity(
		ctx,
		SendEmailActivity,
		activityIdempotencyKey(ctx.WorkflowID(), sendEmailActivityName),
		EmailInput{
			UserID:        in.UserID,
			ChargedAmount: charged.ChargedAmount,
		},
		floww.WithActivityMaxAttempts(3),
		floww.WithActivityScheduledAt(time.Now().Add(time.Minute)),
	)
	if err != nil {
		return fmt.Errorf("notify customer: %w", err)
	}

	return nil
}

func ChargeCard(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	fmt.Println("Charging customer for amount:", in.Amount, "| user is:", in.UserID)

	// Simulate delay
	time.Sleep(time.Millisecond * 100)

	if in.Amount%2 == 0 {
		fmt.Println("Succcessfully charged for user", in.UserID)

		return ChargeOutput{
			IsCharged:     true,
			ChargedAmount: in.Amount,
		}, nil
	}

	fmt.Println("Nothing charged for user", in.UserID)

	return ChargeOutput{
		IsCharged:     false,
		ChargedAmount: 0,
	}, nil
}

func SendEmail(ctx context.Context, in EmailInput) (EmailOutput, error) {
	fmt.Println("Email to:", in.UserID)
	fmt.Println("Charged amount:", in.ChargedAmount)

	// Simulate delay
	time.Sleep(time.Millisecond * 100)

	return EmailOutput{
		IsSent: true,
	}, nil
}

func calculateBackoff(attempt uint) time.Duration {
	return time.Minute * time.Duration(attempt)
}

// activityIdempotencyKey provides predictive idempotency key for activity for provided workflow and activity name.
func activityIdempotencyKey(workflowID uuid.UUID, activityName string) uuid.UUID {
	return uuid.NewSHA1(workflowID, []byte(activityName))
}
