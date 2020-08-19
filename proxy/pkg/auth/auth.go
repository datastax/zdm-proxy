package auth

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/riptano/cloud-gate/proxy/pkg/frame"

	log "github.com/sirupsen/logrus"
)

const (
	maxAuthRetries = 5
)

func HandleAstraStartup(client net.Conn, astraSession net.Conn, startupFrame []byte) error {
	log.Debugf("Initiating startup between %s and %s", client.RemoteAddr(), astraSession.RemoteAddr())
	clientFrame := startupFrame
	buf := make([]byte, 0xfffff)
	authenticated := false
	for !authenticated {
		_, err := astraSession.Write(clientFrame)
		if err != nil {
			return fmt.Errorf("unable to send startup frame from client %s to %s",
				client.RemoteAddr(), astraSession.RemoteAddr())
		}

		bytesRead, err := astraSession.Read(buf)
		if err != nil {
			return fmt.Errorf("error occurred while reading from db connection %v", err)
		}

		f := frame.New(buf[:bytesRead])

		_, err = client.Write(f.RawBytes)
		if err != nil {
			return err
		}

		switch f.Opcode {
		case 0x03, 0x0E:
			// AUTHENTICATE or AUTH_CHALLENGE
			log.Debug("Received %s frame", f.Opcode)
			break
		case 0x02:
			// READY
			log.Debugf("%s did not request authorization for connection %s",
				astraSession.RemoteAddr(), client.RemoteAddr())
			authenticated = true
		case 0x10:
			// AUTH_SUCCESS
			log.Debugf("%s successfully authenticated with %s",
				client.RemoteAddr(), astraSession.RemoteAddr())
			authenticated = true
		default:
			return fmt.Errorf("encountered error that was not READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS")
		}

		if !authenticated {
			bytesRead, err = client.Read(buf)
			clientFrame = buf[:bytesRead]
		}
	}

	return nil
}

func HandleOriginCassandraStartup(client net.Conn, originCassandra net.Conn, startupFrame []byte, username string, password string) error {
	log.Debugf("Initiating startup between %s and %s", client.RemoteAddr(), originCassandra.RemoteAddr())

	// Send client's initial startup frame to the database
	_, err := originCassandra.Write(startupFrame)
	if err != nil {
		return fmt.Errorf("unable to send startup frame from client %s to %s",
			client.RemoteAddr(), originCassandra.RemoteAddr())
	}

	authAttempts := 0
	buf := make([]byte, 0xffffff)
	for {
		bytesRead, err := originCassandra.Read(buf)
		if err != nil {
			return fmt.Errorf("error occurred while reading from db connection %v", err)
		}

		f := frame.New(buf[:bytesRead])

		switch f.Opcode {
		case 0x02:
			// READY (server didn't ask for authentication)
			log.Debugf("%s did not request authorization for connection %s",
				originCassandra.RemoteAddr(), client.RemoteAddr())

			return nil
		case 0x03, 0x0E:
			// AUTHENTICATE/AUTH_CHALLENGE (server requests authentication)
			if authAttempts >= maxAuthRetries {
				return fmt.Errorf("failed to authenticate connection to %s for %s",
					originCassandra.RemoteAddr(), client.RemoteAddr())
			}

			log.Debugf("%s requested authentication for connection %s",
				originCassandra.RemoteAddr(), client.RemoteAddr())

			authResp := authFrame(username, password, startupFrame)
			_, err := originCassandra.Write(authResp)
			if err != nil {
				return err
			}

			authAttempts++
		case 0x10:
			// AUTH_SUCCESS (authentication successful)
			log.Debugf("%s successfully authenticated with %s",
				client.RemoteAddr(), originCassandra.RemoteAddr())
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

// HandleOptions tunnels the OPTIONS request from the client to the database, and then
// tunnels the corresponding SUPPORTED response back from the database to the client.
func HandleOptions(client net.Conn, db net.Conn, f []byte) error {
	if f[4] != 0x05 {
		return fmt.Errorf("non OPTIONS frame sent into HandleOption for connection %s -> %s",
			client.RemoteAddr(), db.RemoteAddr())
	}

	log.Debugf("HO 1") // [Alice]
	_, err := db.Write(f)
	if err != nil {
		return err
	}

	log.Debugf("HO 2") // [Alice]
	buf := make([]byte, 0xffffff)
	bytesRead, err := db.Read(buf)
	if err != nil {
		return err
	}

	log.Debugf("HO 3") // [Alice]
	if bytesRead < 9 {
		return fmt.Errorf("received invalid CQL response from database while setting up OPTIONS for "+
			"connection %s -> %s", client.RemoteAddr(), db.RemoteAddr())
	}

	log.Debugf("HO 4") // [Alice]
	resp := buf[:bytesRead]
	log.Debugf("resp %v", resp) // [Alice]
	if resp[4] != 0x06 {
		return fmt.Errorf("non SUPPORTED frame received from database for connection %s -> %s",
			client.RemoteAddr(), db.RemoteAddr())
	}

	log.Debugf("HO 5") // [Alice]
	_, err = client.Write(resp)
	if err != nil {
		return err
	}

	log.Debugf("HO 6") // [Alice]
	return nil
}
