package updates

import (
	"encoding/json"
	"github.com/google/uuid"

	log "github.com/sirupsen/logrus"
)

// UpdateType is an enum of types of update between the proxy and migration services
type UpdateType int

// TableUpdate, Start, Complete, ... are the enums of the update types
const (
	TableUpdate = iota
	Start
	Complete
	Shutdown
	Success
	Failure
)

// Update represents a request between the migration and proxy services
type Update struct {
	ID    uuid.UUID
	Type  UpdateType
	Data  []byte
	Error error
}

func New(updateType UpdateType, data []byte) *Update {
	return &Update{
		ID: uuid.New(),
		Type: updateType,
		Data: data,
		Error: nil,
	}
}

func SuccessResponse(update *Update) []byte {
	resp := Update{
		ID:   update.ID,
		Type: Success,
	}

	marshaled, err := json.Marshal(resp)
	if err != nil {
		log.Error(err)
	}

	return marshaled
}

func FailureResponse(update *Update, err error) []byte {
	resp := Update{
		ID:    update.ID,
		Type:  Failure,
		Error: err,
	}

	marshaled, err := json.Marshal(resp)
	if err != nil {
		log.Error(err)
	}

	return marshaled
}
