package cloudgateproxy

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"time"
)

const (
	maxAuthRetries = 5
)

func (ch *ClientHandler) handleTargetCassandraStartup(startupFrame *frame.RawFrame) error {

	// extracting these into variables for convenience
	clientIPAddress := ch.clientConnector.connection.RemoteAddr()
	targetCassandraIPAddress := ch.targetCassandraConnector.connection.RemoteAddr()

	log.Infof("Initiating startup between %v and %v", clientIPAddress, targetCassandraIPAddress)
	phase := 1
	attempts := 0

	var authenticator *DsePlainTextAuthenticator
	if ch.targetCreds != nil {
		authenticator = &DsePlainTextAuthenticator{
			Credentials: ch.targetCreds,
		}
	}

	var lastResponse *frame.Frame
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
			if authenticator == nil {
				return fmt.Errorf("target requested authentication but origin did not, can not proceed with target handshake")
			}

			var err error
			var parsedRequest *frame.Frame
			parsedRequest, err = performHandshakeStep(authenticator, startupFrame.Header.Version, startupFrame.Header.StreamId, lastResponse)
			if err != nil {
				return fmt.Errorf("could not perform handshake step: %w", err)
			}

			request, err = defaultCodec.ConvertToRawFrame(parsedRequest)
			if err != nil {
				return fmt.Errorf("could not convert auth response frame to raw frame: %w", err)
			}
		}

		overallRequestStartTime := time.Now()
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightRequestsTarget)
		channel = ch.targetCassandraConnector.forwardToCluster(request)

		f, ok := <-channel
		ch.metricsHandler.DecrementCountByOne(metrics.InFlightRequestsTarget)
		ch.metricsHandler.TrackInHistogram(metrics.ProxyRequestDurationTarget, overallRequestStartTime)
		if !ok {
			if ch.clientHandlerContext.Err() != nil {
				return ShutdownErr
			}

			return fmt.Errorf("unable to send startup frame from clientConnection %v to %v",
				clientIPAddress, targetCassandraIPAddress)
		}

		if !isResponseSuccessful(f) {
			ch.metricsHandler.IncrementCountByOne(metrics.FailedRequestsTarget)
		}

		parsedFrame, err := defaultCodec.ConvertFromRawFrame(f)
		if err != nil {
			return fmt.Errorf("could not decode frame from %v: %w", targetCassandraIPAddress, err)
		}
		lastResponse = parsedFrame

		switch f.Header.OpCode {
		case primitive.OpCodeAuthenticate:
			phase = 2
			log.Debugf("Received AUTHENTICATE for target handshake")
		case primitive.OpCodeAuthChallenge:
			log.Debugf("Received AUTH_CHALLENGE for target handshake")
		case primitive.OpCodeReady:
			log.Debugf("Target cluster did not request authorization for client %v", clientIPAddress)
			return nil
		case primitive.OpCodeAuthSuccess:
			log.Debugf("%s successfully authenticated with target (%v)", clientIPAddress, targetCassandraIPAddress)
			return nil
		default:
			return fmt.Errorf(
				"received response in target handshake that was not "+
					"READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS: %v", parsedFrame.Body.Message)
		}
	}
}
