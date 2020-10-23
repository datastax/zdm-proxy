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
	OpCodeStartup             = OpCode(0x01)
	OpCodeOptions             = OpCode(0x05)
	OpCodeQuery               = OpCode(0x07)
	OpCodePrepare             = OpCode(0x09)
	OpCodeExecute             = OpCode(0x0A)
	OpCodeRegister            = OpCode(0x0B)
	OpCodeBatch               = OpCode(0x0D)
	OpCodeAuthResponseRequest = OpCode(0x0F)
	// responses
	OpCodeError                = OpCode(0x00)
	OpCodeReady                = OpCode(0x02)
	OpCodeAuthenticateResponse = OpCode(0x03)
	OpCodeSupported            = OpCode(0x06)
	OpCodeResult               = OpCode(0x08)
	OpCodeEvent                = OpCode(0x0C)
	OpCodeAuthChallenge        = OpCode(0x0E)
	OpCodeAuthSuccess          = OpCode(0x10)
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
	Direction int // 0 if from client application to db
	Version   uint8
	Flags     uint8
	StreamId  int16
	Opcode    OpCode
	Length    uint32
	Body      []byte
	RawBytes  []byte
}

func NewFrame(frame []byte) *Frame {
	version := frame[0] & 0x7F
	hdrLen := GetHeaderLength(version)
	var streamId int16
	if Uses2BytesStreamIds(version) {
		streamId = int16(binary.BigEndian.Uint16(frame[2:4]))
	} else {
		streamId = int16(frame[2])
	}
	return &Frame{
		Direction: int(frame[0]&0x80) >> 7,
		Version:   version,
		Flags:     frame[1],
		StreamId:  streamId,
		Opcode:    OpCode(frame[hdrLen-5]),
		Length:    binary.BigEndian.Uint32(frame[hdrLen-4:hdrLen]),
		Body:      frame[hdrLen:],
		RawBytes:  frame,
	}
}

func NewUnpreparedResponse(version uint8, streamId int16, preparedId []byte) []byte {
	var response []byte

	// *** header
	//
	// version (from request) and direction
	// flags:
	//   - compression (hardcode to false)
	//   - tracing (not relevant)
	//   - custom payload (not relevant)
	//   - warning (not relevant)
	// stream id (from request)
	// opcode (hardcode to ERROR, 0x00)

	header := make([]byte, 9)
	writeByte(setDirectionForResponseFrame(version), &header, 0)		// setting the direction (MSB) to 1 as this is a response
	writeByte(0x00, &header, 1)								// no flags need to be set here
	writeShortBytes(streamId, &header, 2, 4)
	writeByte(uint8(OpCodeError), &header, 4)
	bodyLen := 4 + 2 + len(preparedId)
	writeIntBytes(uint32(bodyLen), &header, 5, 9)

	// *** body
	//
	// bodyLen = error code (1 byte) and preparedId length
	//
	// error code  (unprepared, 0x2500)
	// unpreparedId (from request)

	body := make([]byte, 4+2)	// bodyLen (uint32) + messageLen (uint16)
	writeIntBytes(uint32(ErrorCodeUnprepared), &body, 0, 4)
	writeShortBytes(int16(len(preparedId)), &body, 4, 6)
	body = append(body, preparedId...)

	response = append(header, body...)

	return response
}

type shutdownError struct {
	err string
}

func (e *shutdownError) Error() string {
	return e.err
}

var ShutdownErr = &shutdownError{err: "aborted due to shutdown request"}

func readPartialFrame(connection net.Conn, clientHandlerContext context.Context, buf []byte) error {
	_, err := io.ReadFull(connection, buf)
	if err != nil {
		select {
		case <-clientHandlerContext.Done():
			log.Infof("Shutting down connection to %s", connection.RemoteAddr().String())
			return ShutdownErr
		default:
			return err
		}
	}

	return nil
}

// Simple function that reads data from a connection and builds a frame
func readAndParseFrame(
	connection net.Conn, clientHandlerContext context.Context) (*Frame, error) {

	frameHeader := make([]byte, 9) // max header length
	err := readPartialFrame(connection, clientHandlerContext, frameHeader[:1])
	if err != nil {
		return nil, err
	}

	version := frameHeader[0] & 0x7F
	frameHeader = frameHeader[:GetHeaderLength(version)]
	err = readPartialFrame(connection, clientHandlerContext, frameHeader[1:])
	if err != nil {
		return nil, err
	}

	if frameHeader[1] & 0x01 == 1 {
		return nil, errors.New("compression flag set, unable to parse query beyond header")
	}

	bodyLen := binary.BigEndian.Uint32(frameHeader[len(frameHeader)-4:])
	body := make([]byte, bodyLen)
	err = readPartialFrame(connection, clientHandlerContext, body)
	if err != nil {
		return nil, err
	}

	f := NewFrame(append(frameHeader, body...))

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

func Uses2BytesStreamIds(version byte) bool {
	return version >= 3
}

func GetHeaderLength(version byte) int {
	if Uses2BytesStreamIds(version) {
		return 9
	} else {
		return 8
	}
}
// Reads an "int bytes" from the slice. An "int bytes" is defined as an [int] n,
// followed by n bytes if n >= 0.
func readIntBytes(data []byte) ([]byte, error) {
	idLen, err := readInt(data)
	if err != nil {
		return nil, err
	}
	length := len(data)
	if length < 4+int(idLen) {
		return nil, errors.New("not enough bytes to read an int bytes")
	}
	intBytes := data[4 : 4+idLen]
	return intBytes, nil
}

func setDirectionForResponseFrame(version byte) byte {
	return version | 0x80
}

func writeByte(byteVal uint8, data *[]byte, index int){
	(*data)[index] = byteVal
}

func writeShortBytes(shortVal int16, data *[]byte, startIndex int, endIndex int) {
	binary.BigEndian.PutUint16((*data)[startIndex:endIndex], uint16(shortVal))	//TODO check and fix - not sure it is fine
}

func writeIntBytes(intVal uint32, data *[]byte, startIndex int, endIndex int) {
	binary.BigEndian.PutUint32((*data)[startIndex:endIndex], intVal)
}