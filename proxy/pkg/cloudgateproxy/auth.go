package cloudgateproxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
)

const (
	maxAuthRetries = 5
)

// Returns a proper response frame to authenticate using passed in username and password
// Utilizes the users request frame to maintain the correct version & stream id.
func authFrame(username string, password string, referenceFrame *frame.RawFrame) (*frame.RawFrame, error) {
	tokenLen := 2 + len(username) + len(password)
	token := &bytes.Buffer{}
	binary.Write(token, binary.BigEndian, uint32(tokenLen))
	token.WriteByte(0)
	token.WriteString(username)
	token.WriteByte(0)
	token.WriteString(password)
	tokenBytes := token.Bytes()

	authResponseMsg := &message.AuthResponse{Token: tokenBytes}

	authResponseFrame, err := frame.NewRequestFrame(
		referenceFrame.RawHeader.Version, referenceFrame.RawHeader.StreamId, false, nil, authResponseMsg)
	if err != nil {
		return nil, fmt.Errorf("could not create auth response request: %w", err)
	}

	authResponseRawFrame, err := defaultCodec.ConvertToRawFrame(authResponseFrame)
	if err != nil {
		return nil, fmt.Errorf("could not convert auth response frame to raw frame: %w", err)
	}

	return authResponseRawFrame, nil
}
