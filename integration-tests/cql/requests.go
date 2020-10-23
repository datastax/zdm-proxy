package cql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"io"
	"math"
)

type AuthenticateResponse struct {
	AuthenticatorName string
}

type UnpreparedResponse struct {
	preparedId []byte
}

type Frame struct {
	Direction int
	Version   uint8
	Flags     uint8
	StreamId  uint16
	Opcode    cloudgateproxy.OpCode
	Length    int32
	Body      []byte
	RawBytes  []byte
}

func ParseFrame(frame []byte) (*Frame, error) {
	version := 0x7F & frame[0]
	hdrLen := GetHeaderLength(version)
	var streamId uint16
	var err error
	if Uses2BytesStreamIds(version) {
		streamId, err = readShort(frame[2:4])
		if err != nil {
			return nil, err
		}
	} else {
		streamId = uint16(frame[2])
	}
	bodyLen, err := readInt(frame[hdrLen-4 : hdrLen])
	if err != nil {
		return nil, err
	}
	return &Frame{
		Direction: int(frame[0]&0x80) >> 7,
		Version:   version,
		Flags:     frame[1],
		StreamId:  streamId,
		Opcode:    cloudgateproxy.OpCode(frame[hdrLen-5]),
		Length:    bodyLen,
		Body:      frame[hdrLen:],
		RawBytes:  frame,
	}, nil
}

func NewStartupRequest(version byte) ([]byte, error) {
	baseRequest := newBaseRequest(version, cloudgateproxy.OpCodeStartup)
	buffer := &bytes.Buffer{}
	err := writeInt16(buffer, 1)
	if err != nil {
		return nil, err
	}

	err = writeString(buffer, "CQL_VERSION")
	if err != nil {
		return nil, err
	}

	err = writeString(buffer, "3.0.0")
	if err != nil {
		return nil, err
	}

	return setBody(baseRequest, buffer.Bytes())
}

func NewAuthResponseRequest(version byte, token []byte) ([]byte, error) {
	baseRequest := newBaseRequest(version, cloudgateproxy.OpCodeAuthResponseRequest)
	buffer := &bytes.Buffer{}
	err := writeBytes(buffer, token)
	if err != nil {
		return nil, err
	}
	return setBody(baseRequest, buffer.Bytes())
}

func NewExecuteRequest(version byte, preparedId []byte) ([]byte, error) {
	baseRequest := newBaseRequest(version, cloudgateproxy.OpCodeExecute)

	buffer := &bytes.Buffer{}
	err := writeShortBytes(buffer, preparedId)
	if err != nil {
		return nil, err
	}
	return setBody(baseRequest, buffer.Bytes())
}

func NewPrepareRequest(version byte, query string) ([]byte, error) {
	baseRequest := newBaseRequest(version, cloudgateproxy.OpCodePrepare)
	buffer := &bytes.Buffer{}
	err := writeLongString(buffer, query)
	if err != nil {
		return nil, err
	}
	return setBody(baseRequest, buffer.Bytes())
}

func NewBaseRequestWithStreamId(version byte, streamId uint16, opCode cloudgateproxy.OpCode) ([]byte, error) {
	header := newBaseRequest(version, opCode)
	err := SetStreamId(version, header, streamId)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func SetStreamId(version byte, buffer []byte, streamId uint16) error {
	if Uses2BytesStreamIds(version) {
		binary.BigEndian.PutUint16(buffer[2:4], streamId)
		return nil
	} else {
		if streamId > 127 {
			return errors.New("stream id must be smaller than 128 for version " + string(version))
		}
		buffer[2] = byte(streamId)
		return nil
	}
}

func setBody(request []byte, body []byte) ([]byte, error) {
	hdrLen := GetHeaderLength(request[0])
	buf := &bytes.Buffer{}
	err := write(buf, request[:hdrLen-4])
	if err != nil {
		return nil, err
	}
	err = writeInt32(buf, len(body))
	if err != nil {
		return nil, err
	}
	err = write(buf, body)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func newBaseRequest(version byte, opCode cloudgateproxy.OpCode) []byte {
	buffer := &bytes.Buffer{}

	buffer.WriteByte(version & 0x7F)
	buffer.WriteByte(0) // flags: tracing, custom payload, beta protocol

	if Uses2BytesStreamIds(version) {
		buffer.WriteByte(0)
		buffer.WriteByte(0)
	} else {
		buffer.WriteByte(0)
	}
	buffer.WriteByte(byte(opCode))

	// body length
	buffer.WriteByte(0)
	buffer.WriteByte(0)
	buffer.WriteByte(0)
	buffer.WriteByte(0)
	return buffer.Bytes()
}

func (frame *Frame) IsReadyResponse() bool {
	return frame.Opcode == 0x02
}

func (frame *Frame) ParseAuthenticateResponse() (*AuthenticateResponse, error) {
	if frame.Opcode != 0x03 {
		return nil, errors.New("not an authenticate response")
	}

	authenticator, err := readString(frame.Body)
	if err != nil {
		return nil, err
	}

	return &AuthenticateResponse{
		AuthenticatorName: authenticator,
	}, nil
}

type ErrorResponse struct {
	ErrorCode int
	Message   string
}

func (frame *Frame) ParseErrorResponse() (*ErrorResponse, error) {
	if frame.Opcode != 0 {
		return nil, errors.New("not an error response")
	}

	errorCode, err := readInt(frame.Body[:4])
	if err != nil {
		return nil, err
	}
	msg, err := readString(frame.Body[4:])
	if err != nil {
		return nil, err
	}

	return &ErrorResponse{
		ErrorCode: int(errorCode),
		Message:   msg,
	}, nil
}

type PreparedResponse struct {
	PreparedId []byte
}

func (frame *Frame) ParsePreparedResponse() (*PreparedResponse, error) {
	if frame.Opcode != cloudgateproxy.OpCodeResult {
		return nil, errors.New("not an error response")
	} else if resultCode, err := readInt(frame.Body[:4]); err != nil {
		return nil, errors.New("can't read result code")
	} else if cloudgateproxy.ResultKind(resultCode) != cloudgateproxy.ResultKindPrepared {
		return nil, errors.New("can't read result code")
	} else if preparedId, err := readShortBytes(frame.Body[4:]); err != nil {
		return nil, errors.New("can't read prepared id")
	} else {
		return &PreparedResponse{PreparedId: preparedId}, nil
	}
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

// Reads a uint32 from the slice.
func readInt(data []byte) (int32, error) {
	length := len(data)
	if length < 4 {
		return 0, errors.New("not enough bytes to read an int")
	}
	return int32(binary.BigEndian.Uint32(data[0:4])), nil
}

// Reads a uint16 from the slice.
func readShort(data []byte) (uint16, error) {
	length := len(data)
	if length < 2 {
		return 0, errors.New("not enough bytes to read a short")
	}
	return binary.BigEndian.Uint16(data[0:2]), nil
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

func readShortBytes(data []byte) ([]byte, error) {
	bytesLen, err := readShort(data)
	if err != nil {
		return nil, err
	}
	length := len(data)
	if length < 2+int(bytesLen) {
		return nil, errors.New("not enough bytes to read a short bytes")
	}
	return data[2 : 2+bytesLen], nil
}

func writeInt16(writer io.Writer, integer int) error {
	if integer > math.MaxInt16 || integer < math.MinInt16 {
		return errors.New(fmt.Sprintf("integer larger than 16 bits: %d", integer))
	}
	return binary.Write(writer, binary.BigEndian, uint16(integer))
}

func writeInt32(writer io.Writer, integer int) error {
	if integer > math.MaxInt32 || integer < math.MinInt32 {
		return errors.New(fmt.Sprintf("integer larger than 32 bits: %d", integer))
	}
	return binary.Write(writer, binary.BigEndian, uint32(integer))
}

func writeBytes(writer io.Writer, bytes []byte) error {
	if bytes == nil {
		return writeInt32(writer, -1)
	}
	err := writeInt32(writer, len(bytes))
	if err != nil {
		return err
	}
	return write(writer, bytes)
}

func writeShortBytes(writer io.Writer, bytes []byte) error {
	err := writeInt16(writer, len(bytes))
	if err != nil {
		return err
	}
	return write(writer, bytes)
}

func writeString(writer io.Writer, str string) error {
	bytes := []byte(str)
	err := writeInt16(writer, len(bytes))
	if err != nil {
		return err
	}
	err = write(writer, bytes)
	if err != nil {
		return err
	}
	return nil
}

func writeLongString(writer io.Writer, str string) error {
	b := []byte(str)
	err := writeInt32(writer, len(b))
	if err != nil {
		return err
	}
	err = write(writer, b)
	if err != nil {
		return err
	}
	return nil
}

func write(writer io.Writer, bytes []byte) error {
	n, err := writer.Write(bytes)
	if err != nil {
		return err
	}
	if n != len(bytes) {
		return errors.New("could not write all bytes to buffer")
	}
	return nil
}
