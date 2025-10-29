package zdmproxy

import (
	"context"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
	"github.com/datastax/go-cassandra-native-protocol/compression/snappy"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/go-cassandra-native-protocol/segment"
)

type shutdownError struct {
	err string
}

func (e *shutdownError) Error() string {
	return e.err
}

var defaultFrameCodec = frame.NewRawCodec()
var defaultSegmentCodec = segment.NewCodec()

var frameCodecs = map[primitive.Compression]frame.RawCodec{
	primitive.CompressionNone:       defaultFrameCodec,
	primitive.CompressionLz4:        frame.NewRawCodecWithCompression(lz4.Compressor{}),
	primitive.CompressionSnappy:     frame.NewRawCodecWithCompression(snappy.Compressor{}),
	primitive.Compression("none"):   defaultFrameCodec,
	primitive.Compression("lz4"):    frame.NewRawCodecWithCompression(lz4.Compressor{}),
	primitive.Compression("snappy"): frame.NewRawCodecWithCompression(snappy.Compressor{}),
}

var segmentCodecs = map[primitive.Compression]segment.Codec{
	primitive.CompressionNone:     defaultSegmentCodec,
	primitive.CompressionLz4:      segment.NewCodecWithCompression(lz4.Compressor{}),
	primitive.Compression("none"): defaultSegmentCodec,
	primitive.Compression("lz4"):  segment.NewCodecWithCompression(lz4.Compressor{}),
}

func getFrameCodec(compression primitive.Compression) (frame.RawCodec, error) {
	codec, ok := frameCodecs[compression]
	if !ok {
		return nil, fmt.Errorf("no codec for compression: %v", compression)
	}
	return codec, nil
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
	err := defaultFrameCodec.EncodeRawFrame(frame, writer) // body is already compressed if needed, so we can use default codec
	return adaptConnErr(connectionAddr, clientHandlerContext, err)
}

// TODO
// Simple function that reads data from a connection and builds a frame
func asdasdreadRawFrame(reader io.Reader, connectionAddr string, clientHandlerContext context.Context) (*frame.RawFrame, error) {
	rawFrame, err := defaultFrameCodec.DecodeRawFrame(reader) // body is not being decompressed, so we can use default codec
	if err != nil {
		return nil, adaptConnErr(connectionAddr, clientHandlerContext, err)
	}

	return rawFrame, nil
}
