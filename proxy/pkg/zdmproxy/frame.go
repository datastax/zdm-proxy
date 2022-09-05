package zdmproxy

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"io"
)

type shutdownError struct {
	err string
}

func (e *shutdownError) Error() string {
	return e.err
}

var defaultCodec = frame.NewRawCodec()

var ShutdownErr = &shutdownError{err: "aborted due to shutdown request"}

func adaptConnErr(connectionAddr string, clientHandlerContext context.Context, err error) error {
	if err != nil {
		if clientHandlerContext.Err() != nil {
			return fmt.Errorf("connection error (%v) but shutdown requested (connection to %v): %w", err, connectionAddr, ShutdownErr)
		}

		return err
	}

	return nil
}

// Simple function that writes a rawframe with a single call to writeToConnection
func writeRawFrame(writer io.Writer, connectionAddr string, clientHandlerContext context.Context, frame *frame.RawFrame) error {
	err := defaultCodec.EncodeRawFrame(frame, writer)
	return adaptConnErr(connectionAddr, clientHandlerContext, err)
}

// Simple function that reads data from a connection and builds a frame
func readRawFrame(reader io.Reader, connectionAddr string, clientHandlerContext context.Context) (*frame.RawFrame, error) {
	rawFrame, err := defaultCodec.DecodeRawFrame(reader)
	if err != nil {
		return nil, adaptConnErr(connectionAddr, clientHandlerContext, err)
	}

	return rawFrame, nil
}
