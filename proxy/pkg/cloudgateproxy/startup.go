package cloudgateproxy

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/frame"
	log "github.com/sirupsen/logrus"
)

func (ch *ClientHandler) handleTargetCassandraStartup(startupFrame *frame.RawFrame) error {

	// extracting these into variables for convenience
	clientIPAddress := ch.clientConnector.connection.RemoteAddr()
	targetCassandraIPAddress := ch.targetCassandraConnector.connection.RemoteAddr()

	log.Infof("Initiating startup between %v and %v", clientIPAddress, targetCassandraIPAddress)
	phase := 1
	attempts := 0
	for {
		if attempts > maxAuthRetries {
			return errors.New("reached max number of attempts to complete target cluster handshake")
		}

		attempts++

		var channel chan *frame.RawFrame
		var request *frame.RawFrame

		switch phase {
		case 1:
			request = startupFrame
		case 2:
			var err error
			request, err = authFrame(ch.targetUsername, ch.targetPassword, startupFrame)
			if err != nil {
				return fmt.Errorf("could not create auth frame: %w", err)
			}
		}

		channel = ch.targetCassandraConnector.forwardToCluster(request)

		f, ok := <-channel
		if !ok {
			select {
			case <-ch.clientHandlerContext.Done():
				return ShutdownErr
			default:
				return fmt.Errorf("unable to send startup frame from clientConnection %v to %v",
					clientIPAddress, targetCassandraIPAddress)
			}
		}

		// TODO broken debug print - needs fixing with expected value for placeholder
		//log.Debugf("handleTargetCassandraStartup: Received frame from TargetCassandra for %02x", )

		switch f.RawHeader.OpCode {
		case cassandraprotocol.OpCodeAuthenticate:
			phase = 2
			log.Debugf("Received AUTHENTICATE for target handshake")
		case cassandraprotocol.OpCodeAuthChallenge:
			log.Debugf("Received AUTH_CHALLENGE for target handshake")
		case cassandraprotocol.OpCodeReady:
			log.Debugf("Target cluster did not request authorization for client %v", clientIPAddress)
			return nil
		case cassandraprotocol.OpCodeAuthSuccess:
			log.Debugf("%s successfully authenticated with target (%v)", clientIPAddress, targetCassandraIPAddress)
			return nil
		default:
			body, err := defaultCodec.DecodeBody(f.RawHeader, bytes.NewReader(f.RawBody))
			if err != nil {
				log.Warnf("could not decode body of unexpected handshake response: %v", err)
				return fmt.Errorf(
					"received response in target handshake that was not "+
						"READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS: %02x", f.RawHeader.OpCode)
			}
			return fmt.Errorf(
				"received response in target handshake that was not "+
					"READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS: %v", body.Message)
		}
	}

	return nil
}
