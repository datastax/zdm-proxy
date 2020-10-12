package cloudgateproxy

import (
	"context"
	"encoding/binary"
	"errors"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
)

type OpCode uint8

const (
	// requests
	OpCodeStartup      = OpCode(0x01)
	OpCodeOptions      = OpCode(0x05)
	OpCodeQuery        = OpCode(0x07)
	OpCodePrepare      = OpCode(0x09)
	OpCodeExecute      = OpCode(0x0A)
	OpCodeRegister     = OpCode(0x0B)
	OpCodeBatch        = OpCode(0x0D)
	OpCodeAuthResponse = OpCode(0x0F)
	// responses
	OpCodeError         = OpCode(0x00)
	OpCodeReady         = OpCode(0x02)
	OpCodeAuthenticate  = OpCode(0x03)
	OpCodeSupported     = OpCode(0x06)
	OpCodeResult        = OpCode(0x08)
	OpCodeEvent         = OpCode(0x0C)
	OpCodeAuthChallenge = OpCode(0x0E)
	OpCodeAuthSuccess   = OpCode(0x10)
)

type ResultKind uint32

const (
	ResultKindVoid         = ResultKind(0x0001)
	ResultKindRows         = ResultKind(0x0002)
	ResultKindSetKeyspace  = ResultKind(0x0003)
	ResultKindPrepared     = ResultKind(0x0004)
	ResultKindSchemaChange = ResultKind(0x0005)
)

type ErrorCode uint32

const (
	ErrorCodeServerError          = ErrorCode(0x0000)
	ErrorCodeProtocolError        = ErrorCode(0x000A)
	ErrorCodeAuthenticationError  = ErrorCode(0x0100)
	ErrorCodeUnavailableException = ErrorCode(0x1000)
	ErrorCodeOverloaded           = ErrorCode(0x1001)
	ErrorCodeIsBootstrapping      = ErrorCode(0x1002)
	ErrorCodeTruncateError        = ErrorCode(0x1003)
	ErrorCodeWriteTimeout         = ErrorCode(0x1100)
	ErrorCodeReadTimeout          = ErrorCode(0x1200)
	ErrorCodeReadFailure          = ErrorCode(0x1300)
	ErrorCodeFunctionFailure      = ErrorCode(0x1400)
	ErrorCodeWriteFailure         = ErrorCode(0x1500)
	ErrorCodeSyntaxError          = ErrorCode(0x2000)
	ErrorCodeUnauthorized         = ErrorCode(0x2100)
	ErrorCodeInvalid              = ErrorCode(0x2200)
	ErrorCodeConfigError          = ErrorCode(0x2300)
	ErrorCodeAlreadyExists        = ErrorCode(0x2400)
	ErrorCodeUnprepared           = ErrorCode(0x2500)
)

type Frame struct {
	Direction int // [Alice] 0 if from client application to db
	Version   uint8
	Flags     uint8
	StreamId  uint16
	Opcode    OpCode
	Length    uint32
	Body      []byte
	RawBytes  []byte
}

func NewFrame(frame []byte) *Frame {
	return &Frame{
		Direction: int(frame[0]&0x80) >> 7,
		Version:   frame[0],
		Flags:     frame[1],
		StreamId:  binary.BigEndian.Uint16(frame[2:4]),
		Opcode:    OpCode(frame[4]),
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
		log.Errorf("compression flag for stream %d set, unable to parse query beyond header", f.StreamId)
	}

	return f, nil
}

// Protocol types decoding functions

// Reads a uint16 from the slice.
func readShort(data []byte) (uint16, error) {
	length := len(data)
	if length < 2 {
		return 0, errors.New("not enough bytes to read a short")
	}
	return binary.BigEndian.Uint16(data[0:2]), nil
}

// Reads a uint32 from the slice.
func readInt(data []byte) (uint32, error) {
	length := len(data)
	if length < 4 {
		return 0, errors.New("not enough bytes to read an int")
	}
	return binary.BigEndian.Uint32(data[0:4]), nil
}

// Reads a string from the slice. A string is defined as a [short] n,
// followed by n bytes representing an UTF-8 string.
func readString(data []byte) (string, error) {
	queryLen, err := readShort(data)
	if err != nil {
		return "", err
	}
	length := len(data)
	if length < 2+int(queryLen) {
		return "", errors.New("not enough bytes to read a string")
	}
	str := string(data[2 : 2+queryLen])
	return str, nil
}

// Reads a "long string" from the slice. A long string is defined as an [int] n,
// followed by n bytes representing an UTF-8 string.
func readLongString(data []byte) (string, error) {
	queryLen, err := readInt(data)
	if err != nil {
		return "", err
	}
	length := len(data)
	if length < 4+int(queryLen) {
		return "", errors.New("not enough bytes to read a long string")
	}
	str := string(data[4 : 4+queryLen])
	return str, nil
}

// Reads a "short bytes" from the slice. A short bytes is defined as a [short] n,
// followed by n bytes if n >= 0.
func readShortBytes(data []byte) ([]byte, error) {
	idLen, err := readShort(data)
	if err != nil {
		return nil, err
	}
	length := len(data)
	if length < 2+int(idLen) {
		return nil, errors.New("not enough bytes to read a short bytes")
	}
	shortBytes := data[2 : 2+idLen]
	return shortBytes, nil
}
