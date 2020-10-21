package cloudgateproxy

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
)

func (ch *ClientHandler) handleTargetCassandraStartup(startupFrame *Frame) error {

	// extracting these into variables for convenience
	clientIPAddress := ch.clientConnector.connection.RemoteAddr().String()
	targetCassandraIPAddress := ch.targetCassandraConnector.connection.RemoteAddr().String()

	log.Infof("Initiating startup between %s and %s", clientIPAddress, targetCassandraIPAddress)
	phase := 1
	attempts := 0
	for {
		if attempts > maxAuthRetries {
			return errors.New("reached max number of attempts to complete target cluster handshake")
		}

		attempts++

		var channel chan *Frame
		var request *Frame

		switch phase {
		case 1:
			request = startupFrame
		case 2:
			request = authFrame(ch.targetCassandraConnector.username, ch.targetCassandraConnector.password, startupFrame)
		}

		channel = ch.targetCassandraConnector.forwardToCluster(request.RawBytes, request.StreamId)

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

		log.Debugf("handleTargetCassandraStartup: Received frame from TargetCassandra for %02x", )

		switch f.Opcode {
		case OpCodeAuthenticateResponse:
			phase = 2
			log.Debugf("Received AUTHENTICATE for target handshake")
		case OpCodeAuthChallenge:
			log.Debugf("Received AUTH_CHALLENGE for target handshake")
		case OpCodeReady:
			log.Debugf("Target cluster did not request authorization for client %s", clientIPAddress)
			return nil
		case OpCodeAuthSuccess:
			log.Debugf("%s successfully authenticated with target (%s)", clientIPAddress, targetCassandraIPAddress)
			return nil
		default:
			return fmt.Errorf(
				"received response in target handshake that was not " +
					"READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS")
		}
	}

	return nil
}
