package frame

import (
	"encoding/binary"
)

type Frame struct {
	Direction int
	Version   uint8
	Flags     uint8
	Stream    uint16
	Opcode    uint8
	Length    uint32
	Body      []byte
	RawBytes  []byte
}

func New(frame []byte) *Frame {
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

func (f *Frame) setBody(body []byte) {
	f.Body = body
	f.Length = uint32(len(body))
}

func (f *Frame) ToBytes() []byte {
	frame := make([]byte, 9)
	frame[0] = f.Version
	frame[1] = f.Flags
	binary.BigEndian.PutUint16(frame[2:4], f.Stream)
	frame[4] = f.Opcode
	binary.BigEndian.PutUint32(frame[5:9], f.Length)
	frame = append(frame, f.Body...)

	return frame
}
