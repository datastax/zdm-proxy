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
	// TODO: ID type string
	ID    uuid.UUID
	Type  UpdateType
	Data  []byte
	Error string
}

func New(updateType UpdateType, data []byte) *Update {
	return &Update{
		ID:    uuid.New(),
		Type:  updateType,
		Data:  data,
		Error: "",
	}
}

func (u *Update) Success() []byte {
	resp := Update{
		ID:   u.ID,
		Type: Success,
	}

	marshaled, err := json.Marshal(resp)
	if err != nil {
		log.Error(err)
	}

	return marshaled
}

func (u *Update) Failure(err error) []byte {
	resp := Update{
		ID:    u.ID,
		Type:  Failure,
		Error: err.Error(),
	}

	marshaled, err := json.Marshal(resp)
	if err != nil {
		log.Error(err)
	}

	return marshaled
}
