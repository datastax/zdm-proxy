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
