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

var (
	ChargeCardActivity = floww.NewActivity[ChargeInput, ChargeOutput]("ChargeCard")
	SendEmailActivity  = floww.NewActivity[EmailInput, EmailOutput]("SendEmail")
)

//
// Example workflow
//

type OrderInput struct {
	UserID string
	Amount int
}

var OrderWorkflow = floww.NewWorkflow[OrderInput]("OrderWorkflow")

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
		uuid.Must(uuid.NewV7()), // provide predictive idempotency key instead if you want to
		OrderInput{
			UserID: uuid.NewString(),
			Amount: rand.IntN(100),
		},
		floww.WithWorkflowMaxAttempts(3),
	); err != nil {
		return err
	}

	fmt.Println("Order workflow is scheduled successfully")

	return nil
}

//
// Actual logic
//

func RunOrderWorkflow(ctx *floww.WorkflowContext, in OrderInput) error {
	charged, err := floww.ExecuteActivity(
		ctx,
		ChargeCardActivity,
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
