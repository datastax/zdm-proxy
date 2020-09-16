package cloudgateproxy

import (
	"encoding/binary"
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
)

const (
	maxAuthRetries = 5
)

func HandleTargetCassandraStartup(clientConnection net.Conn, targetCassandraConnection net.Conn, startupFrame []byte, serviceResponseChannel chan *Frame) error {

	log.Debugf("Initiating startup between %s and %s", clientConnection.RemoteAddr(), targetCassandraConnection.RemoteAddr())
	clientFrame := startupFrame
	buf := make([]byte, 0xfffff)
	authenticated := false
	for !authenticated {
		_, err := targetCassandraConnection.Write(clientFrame)
		if err != nil {
			return fmt.Errorf("unable to send startup frame from clientConnection %s to %s",
				clientConnection.RemoteAddr(), targetCassandraConnection.RemoteAddr())
		}

		//bytesRead, err := targetCassandraConnection.Read(buf)
		//if err != nil {
		//	return fmt.Errorf("error occurred while reading from db connection %v", err)
		//}
		//
		//f := frame.New(buf[:bytesRead])
		f := <- serviceResponseChannel
		log.Debug("HandleTargetCassandraStartup: Received frame from TargetCassandra on serviceResponseChannel")

		_, err = clientConnection.Write(f.RawBytes)
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
				targetCassandraConnection.RemoteAddr(), clientConnection.RemoteAddr())
			authenticated = true
		case 0x10:
			// AUTH_SUCCESS
			log.Debugf("%s successfully authenticated with %s",
				clientConnection.RemoteAddr(), targetCassandraConnection.RemoteAddr())
			authenticated = true
		default:
			return fmt.Errorf("encountered error that was not READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS")
		}

		// if it gets to this point and it is still not authenticated, then read again from the client connection
		// and load the next frame into clientFrame to be sent to the db on the next iteration of the loop
		if !authenticated {
			bytesRead, err := clientConnection.Read(buf)
			clientFrame = buf[:bytesRead]
			if err != nil {
				return fmt.Errorf("error occurred while reading from db connection %v", err)
			}
		}
	}

	return nil
}

func HandleOriginCassandraStartup(clientIPAddress string, originCassandraConnection net.Conn, startupFrame []byte, username string, password string, serviceResponseChannel chan *Frame) error {

	log.Debugf("Initiating startup between %s and %s", clientIPAddress, originCassandraConnection.RemoteAddr())

	// Send client's initial startup frame to the database
	_, err := originCassandraConnection.Write(startupFrame)
	if err != nil {
		return fmt.Errorf("unable to send startup frame from client %s to %s",
			clientIPAddress, originCassandraConnection.RemoteAddr())
	}

	authAttempts := 0
	//buf := make([]byte, 0xffffff)
	for {
		//bytesRead, err := originCassandraConnection.Read(buf)
		//if err != nil {
		//	return fmt.Errorf("error occurred while reading from db connection %v", err)
		//}
		//
		//f := frame.New(buf[:bytesRead])

		f := <- serviceResponseChannel
		log.Debug("HandleOriginCassandraStartup: Received frame from OriginCassandra on serviceResponseChannel")

		switch f.Opcode {
		case 0x02:
			// READY (server didn't ask for authentication)
			log.Debugf("%s did not request authorization for connection %s",
				originCassandraConnection.RemoteAddr(), clientIPAddress)

			return nil
		case 0x03, 0x0E:
			// AUTHENTICATE/AUTH_CHALLENGE (server requests authentication)
			if authAttempts >= maxAuthRetries {
				return fmt.Errorf("failed to authenticate connection to %s for %s",
					originCassandraConnection.RemoteAddr(), clientIPAddress)
			}

			log.Debugf("%s requested authentication for connection %s",
				originCassandraConnection.RemoteAddr(), clientIPAddress)

			authResp := authFrame(username, password, startupFrame)
			_, err := originCassandraConnection.Write(authResp)
			if err != nil {
				return err
			}

			authAttempts++
		case 0x10:
			// AUTH_SUCCESS (authentication successful)
			log.Debugf("%s successfully authenticated with %s",
				clientIPAddress, originCassandraConnection.RemoteAddr())
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
func HandleOptions(clientIPAddress string, clusterConnection net.Conn, f []byte, serviceResponseChannel chan *Frame, responseForClientChannel chan []byte) error {
	if f[4] != 0x05 {
		return fmt.Errorf("non OPTIONS frame sent into HandleOption for connection %s -> %s",
			clientIPAddress, clusterConnection.RemoteAddr())
	}

	log.Debugf("HO 1") // [Alice]
	_, err := clusterConnection.Write(f)
	if err != nil {
		return err
	}

	//buf := make([]byte, 0xffffff)
	//bytesRead, err := clusterConnection.Read(buf)
	//if err != nil {
	//	return err
	//}
	log.Debugf("HO 2") // [Alice]
	responseFrame := <- serviceResponseChannel
	log.Debug("HandleOptions: Received frame from TargetCassandra on serviceResponseChannel")

	log.Debugf("HO 3") // [Alice]
	//if bytesRead < 9 {
	if responseFrame.Length < 9 {
		return fmt.Errorf("received invalid CQL response from database while setting up OPTIONS for "+
			"connection %s -> %s", clientIPAddress, clusterConnection.RemoteAddr())
	}

	log.Debugf("HO 4") // [Alice]
	//resp := buf[:bytesRead]
	//log.Debugf("resp %v", resp) // [Alice]
	if responseFrame.Opcode != 0x06 {
		return fmt.Errorf("non SUPPORTED frame received from database for connection %s -> %s",
			clientIPAddress, clusterConnection.RemoteAddr())
	}

	log.Debugf("HO 5") // [Alice]
	//_, err = clientConnection.Write(responseFrame.RawBytes)
	//if err != nil {
	//	return err
	//}
	responseForClientChannel <- responseFrame.RawBytes

	log.Debugf("HO 6") // [Alice]
	return nil
}
