package cloudgateproxy

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
)

// handleStartupFrame will check the frame opcodes to determine what startup actions to take
// The process, at a high level, is that the proxy directly tunnels startup communications
// to TargetCassandra (since the client logs on with TargetCassandra credentials), and then the proxy manually
// initiates startup with the client's old database
func (ch *ClientHandler) handleStartupFrame(f *Frame) (bool, error) {

	// extracting this into a variable for convenience
	clientIPAddress := ch.clientConnector.connection.RemoteAddr().String()

	switch f.Opcode {
	// STARTUP - the STARTUP message is sent to both TargetCassandra and OriginCassandra
	case 0x01:
		log.Debugf("Handling STARTUP message")
		err := ch.handleTargetCassandraStartup(f)
		if err != nil {
			log.Debugf("Error in handling STARTUP message on TargetCassandra: %s", err)
			return false, err
		}
		log.Debugf("STARTUP message successfully handled on TargetCassandra, now proceeding with OriginCassandra")
		err = ch.originCassandraConnector.handleOriginCassandraStartup(f, clientIPAddress)
		if err != nil {
			return false, err
		}
		log.Debugf("STARTUP message successfully handled on OriginCassandra")
		return true, nil
	default:
		channel := ch.targetCassandraConnector.forwardToCluster(f.RawBytes, f.Stream)
		response, ok := <- channel
		if !ok {
			return false, fmt.Errorf("failed to forward %d request from %s to target cluster", f.Opcode, clientIPAddress)
		}
		// TODO why locking here?
		//p.lock.Lock()
		//defer p.lock.Unlock()
		ch.clientConnector.responseChannel <- response.RawBytes
		return false, nil
	}
	return false, fmt.Errorf("received non STARTUP or OPTIONS query from unauthenticated client %s", clientIPAddress)
}


func (ch *ClientHandler) handleTargetCassandraStartup(startupFrame *Frame) error {

	// extracting these into variables for convenience
	clientIPAddress := ch.clientConnector.connection.RemoteAddr().String()
	targetCassandraIPAddress := ch.targetCassandraConnector.connection.RemoteAddr().String()

	log.Infof("Initiating startup between %s and %s", clientIPAddress, targetCassandraIPAddress)
	var err error
	clientFrame := startupFrame
	for {
		channel := ch.targetCassandraConnector.forwardToCluster(clientFrame.RawBytes, clientFrame.Stream)
		f, ok := <-channel
		if !ok {
			select {
			case <-ch.clientHandlerContext.Done():
				return ShutdownErr
			default:
				return fmt.Errorf("unable to send startup frame from clientConnection %s to %s",
					clientIPAddress, targetCassandraIPAddress)
			}
		}

		log.Debug("handleTargetCassandraStartup: Received frame from TargetCassandra for startup")

		//_, err = clientConnection.Write(f.RawBytes)
		ch.clientConnector.responseChannel <- f.RawBytes

		switch f.Opcode {
		case 0x03, 0x0E:
			// AUTHENTICATE or AUTH_CHALLENGE
			log.Debugf("Received frame with opcode %d", f.Opcode)
			break
		case 0x02:
			// READY
			log.Debugf("%s did not request authorization for connection %s. Opcode %d", targetCassandraIPAddress, clientIPAddress, f.Opcode)
			return nil
		case 0x10:
			// AUTH_SUCCESS
			log.Debugf("%s successfully authenticated with %s", clientIPAddress, targetCassandraIPAddress)
			return nil
		default:
			log.Debugf("encountered error that was not READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS. Frame %v", string(*&f.RawBytes))
			return fmt.Errorf("encountered error that was not READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS")
		}

		// if it gets to this point, then we're in the middle of the auth flow. Read again from the client connection
		// and load the next frame into clientFrame to be sent to the db on the next iteration of the loop
		frameHeader := make([]byte, cassHdrLen)
		f, err = readAndParseFrame(ch.clientConnector.connection, frameHeader, ch.clientHandlerContext)

		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("in handleTargetCassandraStartup: %s disconnected", clientIPAddress)
			} else {
				return fmt.Errorf("in handleTargetCassandraStartup: error reading frame header: %s", err)
			}
			return err
		}
	}

	return nil
}

// passing in the clientIPAddress only to identify it in debug statements at this stage. //TODO remove it if possible
func (cc *ClusterConnector) handleOriginCassandraStartup(startupFrame *Frame, clientIPAddress string) error {

	log.Debugf("Initiating startup connecting to node %s", cc.connection.RemoteAddr())

	// Send client's initial startup frame to the database
	responseChannel := cc.forwardToCluster(startupFrame.RawBytes, startupFrame.Stream)
	f, ok := <- responseChannel
	if !ok {
		select {
		case <-cc.clientHandlerContext.Done():
			return ShutdownErr
		default:
			return fmt.Errorf("unable to send startup frame from client %s to %s",
				clientIPAddress, cc.connection.RemoteAddr())
		}
	}
	log.Debug("handleOriginCassandraStartup: Received frame from OriginCassandra on the response channel")

	authAttempts := 0
	// TODO can this commented out code be removed?
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
				cc.connection.RemoteAddr(), clientIPAddress)

			return nil
		case 0x03, 0x0E:
			// AUTHENTICATE/AUTH_CHALLENGE (server requests authentication)
			if authAttempts >= maxAuthRetries {
				return fmt.Errorf("failed to authenticate connection to %s for %s",
					cc.connection.RemoteAddr(), clientIPAddress)
			}

			log.Debugf("%s requested authentication for connection %s",
				cc.connection.RemoteAddr(), clientIPAddress)

			authResp := authFrame(cc.username, cc.password, startupFrame)
			responseChannel = cc.forwardToCluster(authResp.RawBytes, authResp.Stream)
			f, ok = <- responseChannel
			if !ok {
				return fmt.Errorf("auth/auth challenge failed from client %s to %s",
					clientIPAddress, cc.connection.RemoteAddr())
			}

			authAttempts++
		case 0x10:
			// AUTH_SUCCESS (authentication successful)
			log.Debugf("%s successfully authenticated with %s",
				clientIPAddress, cc.connection.RemoteAddr())
			return nil
		}
	}
}