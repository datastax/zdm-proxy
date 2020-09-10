package cloudgateproxy

import (
	"encoding/binary"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
)

type Frame struct {
	Direction int		// [Alice] 0 if from client application to db
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

// Simple function that reads data from a connection and builds a frame
func parseFrame(src net.Conn, frameHeader []byte, metrics *metrics.Metrics) (*Frame, error) {
	sourceAddress := src.RemoteAddr().String()

	// [Alice] read the frameHeader, whose length is constant (9 bytes), and put it into this slice
	log.Debugf("reading frame header from src %s", sourceAddress)
	bytesRead, err := io.ReadFull(src, frameHeader)
	if err != nil {
		if err != io.EOF {
			log.Debugf("%s disconnected", sourceAddress)
		} else {
			log.Debugf("error reading frame header. bytesRead %d", bytesRead)
			log.Error(err)
		}
		return nil, err
	}
	log.Debugf("frameheader number of bytes read by ReadFull %d", bytesRead) // [Alice]
	log.Debugf("frameheader content read by ReadFull %v", frameHeader) // [Alice]
	bodyLen := binary.BigEndian.Uint32(frameHeader[5:9])
	log.Debugf("bodyLen %d", bodyLen) // [Alice]
	data := frameHeader
	bytesSoFar := 0

	log.Debugf("data: %v", data)

	if bodyLen != 0 {
		for bytesSoFar < int(bodyLen) {
			rest := make([]byte, int(bodyLen)-bytesSoFar)
			bytesRead, err := io.ReadFull(src, rest)
			if err != nil {
				log.Error(err)
				continue	// [Alice] next iteration of this small for loop, not the outer infinite one
			}
			data = append(data, rest[:bytesRead]...)
			bytesSoFar += bytesRead
		}
	}
	//log.Debugf("(from %s): %v", src.RemoteAddr(), string(data))
	f := NewFrame(data)
	metrics.IncrementFrames()

	if f.Flags&0x01 == 1 {
		log.Errorf("compression flag for stream %d set, unable to parse query beyond header", f.Stream)
	}

	return f, err
}
