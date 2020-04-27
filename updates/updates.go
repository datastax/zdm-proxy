package updates

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"

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
	ID    string
	Type  UpdateType
	Data  []byte
	Error string
}

// New returns a new Update struct with a random UUID, the passed in Type and the passed in data.
func New(updateType UpdateType, data []byte) *Update {
	return &Update{
		ID:    uuid.New().String(),
		Type:  updateType,
		Data:  data,
		Error: "",
	}
}

// Success returns a serialized success response for the Update struct
func (u *Update) Success() ([]byte, error) {
	resp := Update{
		ID:   u.ID,
		Type: Success,
	}

	return resp.Serialize()
}

// Failure returns a serialized failure response for the Update struct
func (u *Update) Failure(err error) ([]byte, error) {
	resp := Update{
		ID:    u.ID,
		Type:  Failure,
		Error: err.Error(),
	}

	return resp.Serialize()
}

func (u *Update) Serialize() ([]byte, error) {
	marshaled, err := json.Marshal(u)
	if err != nil {
		return nil, err
	}

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(marshaled)))
	withLen := append(length, marshaled...)

	return withLen, nil
}

func CommunicationHandler(src net.Conn, dst net.Conn, handler func(update *Update) error) {
	defer src.Close()

	length := make([]byte, 4)
	for {
		bytesRead, err := src.Read(length)
		if err != nil {
			if err == io.EOF {
				log.Error(err)
				continue
			} else {
				return
			}
		}

		if bytesRead < 4 {
			log.Error("not full update length header")
			continue
		}

		updateLen := binary.BigEndian.Uint32(length)
		buf := make([]byte, updateLen)
		bytesRead, err = src.Read(buf)
		if uint32(bytesRead) < updateLen {
			log.Error("full update not sent")
			continue
		}

		var update Update
		err = json.Unmarshal(buf, &update)
		if err != nil {
			log.Error(err)
			continue
		}

		var resp []byte
		err = handler(&update)
		if err != nil {
			resp, err = update.Failure(err)
		} else {
			resp, err = update.Success()
		}

		// Failure marshaling a response
		if err != nil {
			continue
		}

		_, err = dst.Write(resp)
		if err != nil {
			log.Error(err)
		}
	}

}
