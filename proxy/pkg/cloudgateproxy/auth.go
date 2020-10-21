package cloudgateproxy

import (
	"encoding/binary"
)

const (
	maxAuthRetries = 5
)

// Returns a proper response frame to authenticate using passed in username and password
// Utilizes the users request frame to maintain the correct version & stream id.
func authFrame(username string, password string, referenceFrame *Frame) *Frame {
	respBody := make([]byte, 2+len(username)+len(password))
	respBody[0] = 0
	copy(respBody[1:], username)
	respBody[len(username)+1] = 0
	copy(respBody[2+len(username):], password)

	bodyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(bodyLen, uint32(len(respBody)))

	respBody = append(bodyLen, respBody...)

	hdrLen := GetHeaderLength(referenceFrame.Version)
	authResp := make([]byte, hdrLen)
	authResp[0] = referenceFrame.Version // Protocol version from client
	authResp[1] = 0x00                   // No flags

	if Uses2BytesStreamIds(referenceFrame.Version) {
		authResp[2] = referenceFrame.RawBytes[2] // Stream ID from client
		authResp[3] = referenceFrame.RawBytes[3] // Stream ID from client
	} else {
		authResp[2] = referenceFrame.RawBytes[2]
	}
	authResp[hdrLen-5] = byte(OpCodeAuthResponseRequest)                         // AUTH_RESP opcode
	binary.BigEndian.PutUint32(authResp[hdrLen-4:hdrLen], uint32(len(respBody))) // Length of body

	authResp = append(authResp, respBody...)

	return &Frame{
		Direction: int(authResp[0]&0x80) >> 7,
		Version:   authResp[0],
		Flags:     authResp[1],
		StreamId:  referenceFrame.StreamId,
		Opcode:    OpCode(authResp[4]),
		Length:    uint32(len(respBody)),
		Body:      authResp[hdrLen:],
		RawBytes:  authResp,
	}
}
