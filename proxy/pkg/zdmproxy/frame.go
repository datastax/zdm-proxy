package zdmproxy

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
	"github.com/datastax/go-cassandra-native-protocol/compression/snappy"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type shutdownError struct {
	err string
}

func (e *shutdownError) Error() string {
	return e.err
}

var defaultCodec = frame.NewRawCodec()

var codecs = map[primitive.Compression]frame.RawCodec{
	primitive.CompressionNone:       defaultCodec,
	primitive.CompressionLz4:        frame.NewRawCodecWithCompression(lz4.Compressor{}),
	primitive.CompressionSnappy:     frame.NewRawCodecWithCompression(snappy.Compressor{}),
	primitive.Compression("none"):   defaultCodec,
	primitive.Compression("lz4"):    frame.NewRawCodecWithCompression(lz4.Compressor{}),
	primitive.Compression("snappy"): frame.NewRawCodecWithCompression(snappy.Compressor{}),
}

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
	err := defaultCodec.EncodeRawFrame(frame, writer) // body is already compressed if needed, so we can use default codec
	return adaptConnErr(connectionAddr, clientHandlerContext, err)
}

// Simple function that reads data from a connection and builds a frame
func readRawFrame(reader io.Reader, connectionAddr string, clientHandlerContext context.Context) (*frame.RawFrame, error) {
	rawFrame, err := defaultCodec.DecodeRawFrame(reader) // body is not being decompressed, so we can use default codec
	if err != nil {
		return nil, adaptConnErr(connectionAddr, clientHandlerContext, err)
	}

	return rawFrame, nil
}
