package auth

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"

	"cloud-gate/proxy/pkg/frame"

	log "github.com/sirupsen/logrus"
)

const (
	maxAuthRetries = 5
	authenticator  = "com.datastax.bdp.cassandra.auth.DseAuthenticator"
)

// CheckAuthentication queries the client to provide credentials and then verifies that the
// client's provided credentials match the passed in credentials.
//
// Sends the client an AUTHENTICATE message, providing the given authenticator as the means of
// authentication. Then, it checks the client's credentials in their AUTH_RESPONSE reply. If the
// credentials match the passed in username and password, it sends back an AUTH_SUCCESS and
// returns true, nil. If the credentials do not match, it returns false and an error.
func CheckAuthentication(client net.Conn, username string, password string, startupFrame []byte) error {
	log.Debugf("Checking provided credentials for client %s", client.RemoteAddr())

	authReq := make([]byte, 11)
	authReq[0] = startupFrame[0] ^ 0x80
	authReq[1] = 0x00
	authReq[2] = startupFrame[2]
	authReq[3] = startupFrame[3]
	authReq[4] = 0x03
	binary.BigEndian.PutUint32(authReq[5:9], uint32(len(authenticator)+2))
	binary.BigEndian.PutUint16(authReq[9:11], uint16(len(authenticator)))
	authReq = append(authReq, []byte(authenticator)...)

	_, err := client.Write(authReq)
	if err != nil {
		return fmt.Errorf("unable to send AUTHENTICATE request to client %s", client.RemoteAddr())
	}

	buf := make([]byte, 0xfffff)
	bytesRead, err := client.Read(buf)
	if err != nil {
		return err
	}

	f := frame.New(buf[:bytesRead])

	if f.Opcode == 0x0F {
		// AUTH_RESP
		if validAuthResp(f.RawBytes, username, password) {
			authSuccess := []byte{f.RawBytes[0] ^ 0x80, 0x00, f.RawBytes[2], f.RawBytes[3], 0x10, 0x00, 0x00, 0x00,
				0x04, 0xFF, 0xFF, 0xFF, 0xFF}

			_, err := client.Write(authSuccess)
			if err != nil {
				return fmt.Errorf("unable to send AUTH_SUCCESS response to client %s", client.RemoteAddr())
			}

			return nil
		} else {
			errorMessage := "Failed to login. Please re-try."

			errResp := make([]byte, 15)
			errResp[0] = f.RawBytes[0] ^ 0x80
			errResp[1] = 0x00
			errResp[2] = f.RawBytes[2]
			errResp[3] = f.RawBytes[3]
			errResp[4] = 0x00
			binary.BigEndian.PutUint32(errResp[5:9], uint32(len(errorMessage))+6)
			errResp[11] = 0x01 // Bad credentials error code
			errResp[12] = 0x00 // Bad credentials error code
			binary.BigEndian.PutUint16(errResp[13:15], uint16(len(errorMessage)))
			errResp = append(errResp, []byte(errorMessage)...)

			_, err := client.Write(errResp)
			if err != nil {
				return fmt.Errorf("unable to send ERROR response to client %s", client.RemoteAddr())
			}

			return fmt.Errorf("%s responded with invalid credentials", client.RemoteAddr())
		}
	} else {
		return fmt.Errorf("%s sent non AUTH_RESP response to AUTHENTICATE request", client.RemoteAddr())
	}
}

// validAuthResp returns whether a DSE Password Authenticator AUTH_RESPONSE (resp)
// matches the passed in username and password.
//
// Body should be in form [uint32(len) 0 username 0 password]
func validAuthResp(resp []byte, username string, password string) bool {
	// Assumes frame has CQL header (9 bytes) + a uint32 (4 bytes) representing the length of credentials
	if len(resp) < 14 {
		return false
	}

	// Want to find the index of the 0x00 byte after the username
	// Skipping entire header (9 bytes) and auth length (4 bytes) as those might have
	// 0x00 bytes in them
	usernameStart := bytes.IndexByte(resp[13:], 0x00) + 14 // Username starts 1 byte after 0x00
	usernameEnd := bytes.IndexByte(resp[usernameStart:], 0x00) + usernameStart

	enteredUsername := string(resp[usernameStart:usernameEnd])
	enteredPassword := string(resp[usernameEnd+1:])

	return enteredUsername == username && enteredPassword == password
}

// HandleStartup will do the startup handshake and authentication to the database on behalf of
// the user.
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
			return fmt.Errorf("error occurred while reading from db connection %v", err)
		}

		f := frame.New(buf[:bytesRead])

		switch f.Opcode {
		case 0x02:
			// READY (server didn't ask for authentication)
			log.Debugf("%s did not request authorization for connection %s",
				db.RemoteAddr(), client.RemoteAddr())

			if writeBack {
				_, err := client.Write(f.RawBytes)
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
				_, err := client.Write(readyMessage(f.RawBytes))
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
		return fmt.Errorf("received invalid CQL response from database while setting up OPTIONS for "+
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
