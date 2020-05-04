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
// the user. To the client, it appears as if they are connecting to a database that does not require
// credentials.
// Currently only supports Username/Password authentication.
func HandleStartup(client net.Conn, db net.Conn, username string, password string, startupFrame []byte, writeBack bool) error {
	log.Debugf("Setting up connection from %s to %s", client.RemoteAddr(), db.RemoteAddr())

	// Send client's initial startup frame to the database
	_, err := db.Write(startupFrame)
	if err != nil {
		return fmt.Errorf("unable to send startup frame from client %s to %s",
			client.RemoteAddr(), db.RemoteAddr())
	}

	authAttempts := 0
	buf := make([]byte, 0xffffff)
	for {
		bytesRead, err := db.Read(buf)
		if err != nil {
			return err
		}

		f := buf[:bytesRead]
		opcode := f[4]

		switch opcode {
		case 0x02:
			// READY (server didn't ask for authentication)
			log.Debugf("%s did not request authorization for connection %s",
				db.RemoteAddr(), client.RemoteAddr())

			if writeBack {
				_, err := client.Write(f)
				if err != nil {
					return err
				}
			}

			return nil
		case 0x03, 0x0E:
			// AUTHENTICATE/AUTH_CHALLENGE (server requests authentication)
			if authAttempts >= maxAuthRetries {
				return fmt.Errorf("failed to authenticate connection to %s for %s",
					db.RemoteAddr(), client.RemoteAddr())
			}

			log.Debugf("%s requested authentication for connection %s",
				db.RemoteAddr(), client.RemoteAddr())

			authResp := authFrame(username, password, startupFrame)
			_, err := db.Write(authResp)
			if err != nil {
				return err
			}

			authAttempts++
		case 0x10:
			// AUTH_SUCCESS (authentication successful)
			if writeBack {
				// Write a ready message back to client
				// To the client this makes it seem like they sent a STARTUP message and
				// immediately received a READY message, signifying that no authentication
				// was necessary.
				_, err := client.Write(readyMessage(f))
				if err != nil {
					return err
				}
			}

			return nil
		}
	}
}

// Returns a proper response frame to authenticate using passed in username and password
// Utilizes the users initial startup frame to maintain the correct version & stream id.
func authFrame(username string, password string, startupFrame []byte) []byte {
	respBody := make([]byte, 2+len(username)+len(password))
	respBody[0] = 0
	copy(respBody[1:], username)
	respBody[len(username)+1] = 0
	copy(respBody[2+len(username):], password)

	bodyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(bodyLen, uint32(len(respBody)))

	respBody = append(bodyLen, respBody...)

	authResp := make([]byte, 9)
	authResp[0] = startupFrame[0]                                    // Protocol version from client
	authResp[1] = 0x00                                               // No flags
	authResp[2] = startupFrame[2]                                    // Stream ID from client
	authResp[3] = startupFrame[3]                                    // Stream ID from client
	authResp[4] = 0x0F                                               // AUTH_RESP opcode
	binary.BigEndian.PutUint32(authResp[5:9], uint32(len(respBody))) // Length of body

	authResp = append(authResp, respBody...)

	return authResp
}

// readyMessage creates the correct READY frame response for a given client's
// STARTUP request.
func readyMessage(startupFrame []byte) []byte {
	f := make([]byte, 9)
	f[0] = startupFrame[0] | 0x80 // Maintain user's version & ensure direction bit is 1
	f[2] = startupFrame[2]        // Maintain user's streamID
	f[3] = startupFrame[3]        // Maintain user's streamID
	f[4] = 2                      // READY opcode

	return f
}

// HandleOptions tunnels the OPTIONS request from the client to the database, and then
// tunnels the corresponding SUPPORTED response back from the database to the client.
func HandleOptions(client net.Conn, db net.Conn, f []byte) error {
	if f[4] != 0x05 {
		return fmt.Errorf("non OPTIONS frame sent into HandleOption for connection %s -> %s",
			client.RemoteAddr(), db.RemoteAddr())
	}

	_, err := db.Write(f)
	if err != nil {
		return err
	}

	buf := make([]byte, 0xffffff)
	bytesRead, err := db.Read(buf)
	if err != nil {
		return err
	}

	if bytesRead < 9 {
		return fmt.Errorf("received invalid CQL response from database while setting up OPTIONS for " +
			"connection %s -> %s", client.RemoteAddr(), db.RemoteAddr())
	}

	resp := buf[:bytesRead]
	if resp[4] != 0x06 {
		return fmt.Errorf("non SUPPORTED frame received from database for connection %s -> %s",
			client.RemoteAddr(), db.RemoteAddr())
	}

	_, err = client.Write(resp)
	if err != nil {
		return err
	}

	return nil
}
