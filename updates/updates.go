package updates

import (
	"github.com/google/uuid"
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
	id   uuid.UUID
	Type UpdateType
	Data []byte
}
