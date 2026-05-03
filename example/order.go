package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sknv/floww"
)

//
// Example activities
//

type (
	ChargeInput struct {
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

var OrderWorkflow = floww.NewWorkflow[OrderInput]("OrderWorkflow")

func RegisterWorkflow(activityRegistry *floww.ActivityRegistry, workflowRegistry *floww.WorkflowRegistry) {
	floww.RegisterActivity(activityRegistry, ChargeCardActivity, ChargeCard)
	floww.RegisterActivity(activityRegistry, SendEmailActivity, SendEmail,
		floww.WithActivityBackoffCalculator(calculateBackoff))

	floww.RegisterWorkflow(workflowRegistry, OrderWorkflow, RunOrderWorkflow)
}

func RunOrderWorkflow(ctx *floww.WorkflowContext, in OrderInput) error {
	charged, err := floww.ExecuteActivity(
		ctx,
		ChargeCardActivity,
		activityIdempotencyKey(ctx.WorkflowID(), chargeCardActivityName),
		ChargeInput{Amount: in.Amount},
		floww.WithActivityMaxAttempts(3),
	)
	if err != nil {
		return fmt.Errorf("charge card: %w", err)
	}

	// Note that this line will be repeated during history replay
	fmt.Println("Charge result is:", charged.IsCharged)

	if !charged.IsCharged {
		return floww.Unrecoverable(errors.New("can not charge customer"))
	}

	// Wait for 1 hour before notifying
	_, err = floww.ExecuteActivity(
		ctx,
		SendEmailActivity,
		activityIdempotencyKey(ctx.WorkflowID(), sendEmailActivityName),
		EmailInput{
			UserID:        in.UserID,
			ChargedAmount: charged.ChargedAmount,
		},
		floww.WithActivityMaxAttempts(3),
		floww.WithActivityScheduledAt(time.Now().Add(time.Minute*time.Duration(3))),
	)
	if err != nil {
		return fmt.Errorf("notify customer: %w", err)
	}

	return nil
}

func ChargeCard(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	fmt.Println("Charging customer:", in.Amount)

	// Simulate delay
	time.Sleep(time.Millisecond * 100)

	if in.Amount%2 == 0 {
		fmt.Println("Succcessfully charged")

		return ChargeOutput{
			IsCharged:     true,
			ChargedAmount: in.Amount,
		}, nil
	}

	fmt.Println("Nothing charged")

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

func activityIdempotencyKey(workflowID uuid.UUID, activityName string) uuid.UUID {
	return uuid.NewSHA1(workflowID, []byte(activityName))
}
