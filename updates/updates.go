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

// TODO: maybe add an error field so the two services can communicate what
// 	actually happened during failures
// Update represents a request between the migration and proxy services
type Update struct {
	id   uuid.UUID
	Type UpdateType
	Data []byte
}

func SuccessResponse(update *Update) []byte {
	resp := Update{
		id:   update.id,
		Type: Success,
	}

	marshaled, err := json.Marshal(resp)
	if err != nil {
		log.Error(err)
	}

	return marshaled
}

func FailureResponse(update *Update) []byte {
	resp := Update{
		id:   update.id,
		Type: Failure,
	}

	marshaled, err := json.Marshal(resp)
	if err != nil {
		log.Error(err)
	}

	return marshaled
}
