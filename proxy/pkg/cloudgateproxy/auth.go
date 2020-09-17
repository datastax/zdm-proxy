package cloudgateproxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	log "github.com/sirupsen/logrus"
)

const (
	maxAuthRetries = 5
)

func (p *CloudgateProxy) HandleTargetCassandraStartup(clientConnection net.Conn, targetCassandraConnection net.Conn, startupFrame *Frame) error {

	log.Debugf("Initiating startup between %s and %s", clientConnection.RemoteAddr(), targetCassandraConnection.RemoteAddr())
	var err error
	clientFrame := startupFrame
	sourceIpAddr := clientConnection.RemoteAddr().String()
	for {
		channel := p.forwardToCluster(clientFrame.RawBytes, clientFrame.Stream, true, sourceIpAddr)
		f, ok := <- channel
		if !ok {
			return fmt.Errorf("unable to send startup frame from clientConnection %s to %s",
				clientConnection.RemoteAddr(), targetCassandraConnection.RemoteAddr())
		}

		log.Debug("HandleTargetCassandraStartup: Received frame from TargetCassandra for startup")

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
			return nil
		case 0x10:
			// AUTH_SUCCESS
			log.Debugf("%s successfully authenticated with %s",
				clientConnection.RemoteAddr(), targetCassandraConnection.RemoteAddr())
			return nil
		default:
			return fmt.Errorf("encountered error that was not READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS")
		}

		// if it gets to this point, then we're in the middle of the auth flow. Read again from the client connection
		// and load the next frame into clientFrame to be sent to the db on the next iteration of the loop
		frameHeader := make([]byte, cassHdrLen)
		f, err = parseFrame(clientConnection, frameHeader, p.Metrics)

		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("%s disconnected", clientConnection)
			} else {
				return fmt.Errorf("error reading frame header: %s", err)
			}
			return err
		}
	}

	return nil
}

func (p *CloudgateProxy) HandleOriginCassandraStartup(
	clientIPAddress string, originCassandraConnection net.Conn, startupFrame *Frame, username string, password string) error {

	log.Debugf("Initiating startup between %s and %s", clientIPAddress, originCassandraConnection.RemoteAddr())

	// Send client's initial startup frame to the database
	responseChannel := p.forwardToCluster(startupFrame.RawBytes, startupFrame.Stream, false, clientIPAddress)
	f, ok := <- responseChannel
	if !ok {
		return fmt.Errorf("unable to send startup frame from client %s to %s",
			clientIPAddress, originCassandraConnection.RemoteAddr())
	}
	log.Debug("HandleOriginCassandraStartup: Received frame from OriginCassandra on the response channel")

	authAttempts := 0
	//buf := make([]byte, 0xffffff)
	for {
		//bytesRead, err := originCassandraConnection.Read(buf)
		//if err != nil {
		//	return fmt.Errorf("error occurred while reading from db connection %v", err)
		//}
		//
		//f := frame.New(buf[:bytesRead])

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
			responseChannel = p.forwardToCluster(authResp.RawBytes, authResp.Stream, false, clientIPAddress)
			f, ok = <- responseChannel
			if !ok {
				return fmt.Errorf("auth/auth challenge failed from client %s to %s",
					clientIPAddress, originCassandraConnection.RemoteAddr())
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

// HandleOptions tunnels the OPTIONS request from the client to the database, and then
// tunnels the corresponding SUPPORTED response back from the database to the client.
/*func HandleOptions(clientIPAddress string, clusterConnection net.Conn, f []byte, serviceResponseChannel chan *Frame, responseForClientChannel chan []byte) error {
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
}*/
