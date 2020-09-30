package cloudgateproxy

import (
	"context"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
)

type Frame struct {
	Direction int // [Alice] 0 if from client application to db
	Version   uint8
	Flags     uint8
	Stream    uint16
	Opcode    uint8
	Length    uint32
	Body      []byte
	RawBytes  []byte
}

func NewFrame(frame []byte) *Frame {
	return &Frame{
		Direction: int(frame[0]&0x80) >> 7,
		Version:   frame[0],
		Flags:     frame[1],
		Stream:    binary.BigEndian.Uint16(frame[2:4]),
		Opcode:    frame[4],
		Length:    binary.BigEndian.Uint32(frame[5:9]),
		Body:      frame[9:],
		RawBytes:  frame,
	}
}

type shutdownError struct {
	err string
}

func (e *shutdownError) Error() string {
	return e.err
}

var ShutdownErr = &shutdownError{err: "aborted due to shutdown request"}

// Simple function that reads data from a connection and builds a frame
func readAndParseFrame(
	connection net.Conn, frameHeader []byte, clientHandlerContext context.Context) (*Frame, error) {
	sourceAddress := connection.RemoteAddr().String()

	// [Alice] read the frameHeader, whose length is constant (9 bytes), and put it into this slice
	//log.Debugf("reading frame header from connection %s", sourceAddress)
	_, err := io.ReadFull(connection, frameHeader)
	if err != nil {
		select {
		case <-clientHandlerContext.Done():
			log.Infof("Shutting down connection to %s", sourceAddress)
			return nil, ShutdownErr
		default:
			return nil, err
		}
	}
	//log.Debugf("frameheader number of bytes read by ReadFull %d", bytesRead) // [Alice]
	//log.Debugf("frameheader content read by ReadFull %v", frameHeader) // [Alice]
	bodyLen := binary.BigEndian.Uint32(frameHeader[5:9])
	//log.Debugf("bodyLen %d", bodyLen) // [Alice]
	data := frameHeader
	bytesSoFar := 0

	//log.Debugf("data: %v", data)

	if bodyLen != 0 {
		for bytesSoFar < int(bodyLen) {
			rest := make([]byte, int(bodyLen)-bytesSoFar)
			bytesRead, err := io.ReadFull(connection, rest)
			if err != nil {
				select {
				case <-clientHandlerContext.Done():
					log.Infof("Shutting down connection to %s", sourceAddress)
					return nil, ShutdownErr
				default:
					return nil, err
				}
			}
			data = append(data, rest[:bytesRead]...)
			bytesSoFar += bytesRead
		}
	}
	//log.Debugf("(from %s): %v", connection.RemoteAddr(), string(data))
	f := NewFrame(data)

	if f.Flags&0x01 == 1 {
		log.Errorf("compression flag for stream %d set, unable to parse query beyond header", f.Stream)
	}

	return f, nil
}
