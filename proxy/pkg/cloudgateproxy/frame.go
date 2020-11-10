package cloudgateproxy

import (
	"bytes"
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"net"
)

type shutdownError struct {
	err string
}

func (e *shutdownError) Error() string {
	return e.err
}

var defaultCodec = frame.NewRawCodec()

var ShutdownErr = &shutdownError{err: "aborted due to shutdown request"}

func adaptConnErr(connection net.Conn, clientHandlerContext context.Context, err error) error {
	if err != nil {
		if clientHandlerContext.Err() != nil {
			return fmt.Errorf("connection error (%v) but shutdown requested (connection to %v): %w", err, connection.RemoteAddr(), ShutdownErr)
		}

		return err
	}

	return nil
}

// Simple function that writes a rawframe with a single call to writeToConnection
func writeRawFrame(connection net.Conn, clientHandlerContext context.Context, frame *frame.RawFrame) error {
	buffer := &bytes.Buffer{}
	err := defaultCodec.EncodeRawFrame(frame, buffer)
	if err != nil {
		return err
	}
	err = writeToConnection(connection, buffer.Bytes())
	return adaptConnErr(connection, clientHandlerContext, err)
}

// Simple function that reads data from a connection and builds a frame
func readRawFrame(connection net.Conn, clientHandlerContext context.Context) (*frame.RawFrame, error) {

	rawFrame, err := defaultCodec.DecodeRawFrame(connection)
	if err != nil {
		return nil, adaptConnErr(connection, clientHandlerContext, err)
	}

	return rawFrame, nil
}
