package floww

import "github.com/google/uuid"

// WorkflowRun holds the execution context for a single workflow invocation.
type WorkflowRun struct {
	WorkflowID uuid.UUID

	workflowInput []byte
	decoder       Decoder
}

// NewWorkflowRun constructs a WorkflowRun with the provided ID, raw input bytes, and decoder.
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

// IntoInput decodes the raw workflow input into v.
func (r WorkflowRun) IntoInput(v any) error {
	return r.decoder(r.workflowInput, v)
}
