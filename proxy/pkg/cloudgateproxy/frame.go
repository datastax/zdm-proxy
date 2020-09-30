package cloudgateproxy

import (
	"encoding/binary"
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
func parseFrame(connection net.Conn, frameHeader []byte) (*Frame, error) {
	sourceAddress := connection.RemoteAddr().String()

	// [Alice] read the frameHeader, whose length is constant (9 bytes), and put it into this slice
	//log.Debugf("reading frame header from connection %s", sourceAddress)
	bytesRead, err := io.ReadFull(connection, frameHeader)
	if err != nil {
		if err == io.EOF {
			log.Debugf("in parseFrame: %s disconnected", sourceAddress)
		} else {
			log.Errorf("in parseFrame: error reading frame header. bytesRead %d, err %s", bytesRead, err)
		}
		return nil, err
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
				log.Error(err)
				continue	// [Alice] next iteration of this small for loop, not the outer infinite one
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

	return f, err
}
