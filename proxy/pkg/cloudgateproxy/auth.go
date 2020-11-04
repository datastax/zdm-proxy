package cloudgateproxy

import (
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

const (
	maxAuthRetries = 5
)

// Returns a proper response frame to authenticate using passed in username and password
// Utilizes the users request frame to maintain the correct version & stream id.
func authFrame(username string, password string, referenceFrame *frame.RawFrame) (*frame.RawFrame, error) {
	token := &bytes.Buffer{}
	token.WriteByte(0)
	token.WriteString(username)
	token.WriteByte(0)
	token.WriteString(password)
	tokenBytes := token.Bytes()

	authResponseMsg := &message.AuthResponse{Token: tokenBytes}

	authResponseFrame, err := frame.NewRequestFrame(
		referenceFrame.Header.Version, referenceFrame.Header.StreamId, false, nil, authResponseMsg, false)
	if err != nil {
		return nil, fmt.Errorf("could not create auth response request: %w", err)
	}

	authResponseRawFrame, err := defaultCodec.ConvertToRawFrame(authResponseFrame)
	if err != nil {
		return nil, fmt.Errorf("could not convert auth response frame to raw frame: %w", err)
	}

	return authResponseRawFrame, nil
}
