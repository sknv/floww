package floww

import "github.com/google/uuid"

type ActivityRun struct {
	ActivityID uuid.UUID

	activityInput []byte
	decoder       Decoder
}

func NewActivityRun(
	activityID uuid.UUID,
	activityInput []byte,
	decoder Decoder,
) ActivityRun {
	return ActivityRun{
		ActivityID: activityID,

		activityInput: activityInput,
		decoder:       decoder,
	}
}

func (r ActivityRun) IntoInput(v any) error {
	return r.decoder(r.activityInput, v)
}
