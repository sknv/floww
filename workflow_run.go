package floww

import "github.com/google/uuid"

type WorkflowRun struct {
	WorkflowID uuid.UUID

	workflowInput []byte
	decoder       Decoder
}

func NewWorkflowRun(
	workflowID uuid.UUID,
	workflowInput []byte,
	decoder Decoder,
) WorkflowRun {
	return WorkflowRun{
		WorkflowID: workflowID,

		workflowInput: workflowInput,
		decoder:       decoder,
	}
}

func (r WorkflowRun) IntoInput(v any) error {
	return r.decoder(r.workflowInput, v)
}
