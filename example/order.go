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
		IsCharged     bool
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
// Example signal
//

type NotifySignalInput struct {
	ShouldNotify bool
}

var NotifySignal = floww.NewSignal[NotifySignalInput]("NotifySignal")

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
	workflowIdempotencyKey := uuid.Must(uuid.NewV7()) // provide predictive idempotency key instead if you want to

	workflowID, err := floww.EnqueueWorkflow(
		ctx,
		storage,
		txer,
		OrderWorkflow,
		workflowIdempotencyKey,
		OrderInput{
			UserID: uuid.NewString(),
			Amount: rand.IntN(100),
		},
		floww.WithWorkflowMaxAttempts(3),
	)
	if err != nil {
		return err
	}

	fmt.Println("Order workflow is scheduled successfully")

	// Simulate external signal sending using goroutine for simplicity
	go func() {
		time.Sleep(time.Second * 30)

		shouldNotify := rand.IntN(2)%2 == 0

		if err := floww.SendSignal(
			ctx,
			storage,
			txer,
			workflowID,
			NotifySignal,
			NotifySignalInput{
				ShouldNotify: shouldNotify,
			},
		); err != nil {
			fmt.Println("[ERROR] Can not emit notify signal for workflow id", workflowID, err)

			return
		}

		fmt.Println("Notify signal is emited successfully, should notify user:", shouldNotify)
	}()

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

	// Wait for an external signal to make a decision if a user should be notified
	notify, err := floww.ReceiveSignal(ctx, NotifySignal)
	if err != nil {
		return fmt.Errorf("can not handle notify signal %w", err)
	}

	if !notify.ShouldNotify {
		fmt.Println("Should not notify customer", in.UserID)

		return nil
	}

	// Wait for 30 seconds before notifying
	_, err = floww.ExecuteActivity(
		ctx,
		SendEmailActivity,
		EmailInput{
			UserID:        in.UserID,
			IsCharged:     charged.IsCharged,
			ChargedAmount: charged.ChargedAmount,
		},
		floww.WithActivityMaxAttempts(3),
		floww.WithActivityScheduledAt(time.Now().Add(time.Second*30)),
	)
	if err != nil {
		return fmt.Errorf("notify customer: %w", err)
	}

	return nil
}

func ChargeCard(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	fmt.Println("Charging customer for amount:", in.Amount, "| user is:", in.UserID, "...")

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
	fmt.Println("Sending email to:", in.UserID, "...")

	if in.IsCharged {
		fmt.Println("Notify about charged amount:", in.ChargedAmount)
	} else {
		fmt.Println("Notify about nothing charged")
	}

	// Simulate delay
	time.Sleep(time.Millisecond * 100)

	return EmailOutput{
		IsSent: true,
	}, nil
}

func calculateBackoff(attempt uint) time.Duration {
	return time.Minute * time.Duration(attempt)
}
