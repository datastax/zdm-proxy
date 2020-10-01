package cloudgateproxy

import (
	"encoding/binary"
)

const (
	maxAuthRetries = 5
)

// Returns a proper response frame to authenticate using passed in username and password
// Utilizes the users initial startup frame to maintain the correct version & stream id.
func authFrame(username string, password string, startupFrame *Frame) *Frame {
	respBody := make([]byte, 2+len(username)+len(password))
	respBody[0] = 0
	copy(respBody[1:], username)
	respBody[len(username)+1] = 0
	copy(respBody[2+len(username):], password)

	bodyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(bodyLen, uint32(len(respBody)))

	respBody = append(bodyLen, respBody...)

	authResp := make([]byte, 9)
	authResp[0] = startupFrame.RawBytes[0]                           // Protocol version from client
	authResp[1] = 0x00                                               // No flags
	authResp[2] = startupFrame.RawBytes[2]                           // Stream ID from client
	authResp[3] = startupFrame.RawBytes[3]                           // Stream ID from client
	authResp[4] = 0x0F                                               // AUTH_RESP opcode
	binary.BigEndian.PutUint32(authResp[5:9], uint32(len(respBody))) // Length of body

	authResp = append(authResp, respBody...)

	return &Frame{
		Direction: int(authResp[0]&0x80) >> 7,
		Version:   authResp[0],
		Flags:     authResp[1],
		Stream:    binary.BigEndian.Uint16(authResp[2:4]),
		Opcode:    authResp[4],
		Length:    uint32(len(respBody)),
		Body:      authResp[9:],
		RawBytes:  authResp,
	}
}
