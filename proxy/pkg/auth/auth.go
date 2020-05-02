package auth

import (
	"encoding/binary"
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
)

const (
	maxAuthRetries = 5
)

// HandleStartup will do the startup handshake and authentication to the database on behalf of
// the user. To the client, it appears as if they are connecting to a database that does not need
// any credentials.
// Currently only supports Username/Password authentication.
func HandleStartup(client net.Conn, db net.Conn, username string, password string, startupFrame []byte, writeBack bool) error {

	log.Infof("Setting up connection from %s to %s", client.RemoteAddr().String(), db.RemoteAddr().String())

	authAttempts := 0

	// Send client's initial startup frame to the database
	db.Write(startupFrame)

	buf := make([]byte, 0xfffff)
	for {
		bytesRead, err := db.Read(buf)
		if err != nil {
			return err
		}

		f := buf[:bytesRead]

		// OPCode of message received from database
		switch f[4] {
		case 0x02:
			// READY (server didn't ask for authentication), relay it back to client
			log.Debugf("%s did not request authorization for connection %s",
				db.RemoteAddr().String(), client.RemoteAddr().String())
			if writeBack {
				_, err := client.Write(f)
				if err != nil {
					return err
				}
			}

			return nil
		case 0x03, 0x0E:
			// Authenticate
			log.Debug("%s requested authentication for connection %s",
				db.RemoteAddr().String(), client.RemoteAddr().String())

			authResp := authFrame(username, password, startupFrame)
			_, err := db.Write(authResp)
			if err != nil {
				return err
			}

			authAttempts++
			if authAttempts == maxAuthRetries {
				return fmt.Errorf("failed to successfully authenticate connection to %s",
					db.RemoteAddr().String())
			}
		case 0x10:
			// Auth successful, mimic ready response back to client
			// Response is of form [VERSION 0 0 0 2 0 0 0 0]
			log.Info("Connection %s authenticated", client.RemoteAddr().String())

			if writeBack {
				_, err := client.Write(readyMessage(startupFrame))
				if err != nil {
					return err
				}
			}

			return nil
		}
	}
}

// Returns a proper response frame to authenticate using passed in username and password
// Utilizes the users initial startup frame to mimic version & streamID.
func authFrame(username string, password string, startupFrame []byte) []byte {
	resp := make([]byte, 2+len(username)+len(password))
	resp[0] = 0
	copy(resp[1:], username)
	resp[len(username)+1] = 0
	copy(resp[2+len(username):], password)

	respLen := make([]byte, 4)
	binary.BigEndian.PutUint32(respLen, uint32(len(resp)))

	resp = append(respLen, resp...)

	authFrame := make([]byte, 9)
	authFrame[0] = startupFrame[0]                                // Protocol version from client
	authFrame[1] = 0x00                                           // No flags
	authFrame[2] = startupFrame[2]                                // Stream ID from client
	authFrame[3] = startupFrame[3]                                // Stream ID from client
	authFrame[4] = 0x0F                                           // AUTH_RESP OpCode
	binary.BigEndian.PutUint32(authFrame[5:9], uint32(len(resp))) // Length of body

	authFrame = append(authFrame, resp...)

	return authFrame
}

// TODO: Make sure we don't need to match stream id's
func readyMessage(startupFrame []byte) []byte {
	f := make([]byte, 9)
	f[0] = startupFrame[0] | 0x80 // Maintain user's version
	f[4] = 2

	return f
}
