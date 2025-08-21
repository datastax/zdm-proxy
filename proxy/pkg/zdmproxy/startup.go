package zdmproxy

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

const (
	maxAuthRetries = 5
)

type AuthError struct {
	errMsg *message.AuthenticationError
}

func (recv *AuthError) Error() string {
	return fmt.Sprintf("authentication error: %v", recv.errMsg)
}

func (ch *ClientHandler) handleSecondaryHandshakeStartup(
	startupRequest *frame.RawFrame, startupResponse *frame.RawFrame, asyncConnector bool) error {

	// extracting these into variables for convenience
	clientIPAddress := ch.clientConnector.connection.RemoteAddr()
	var clusterAddress net.Addr
	var logIdentifier string
	var forwardToSecondary forwardDecision
	requestTimeout := time.Duration(ch.conf.ProxyRequestTimeoutMs) * time.Millisecond
	if asyncConnector {
		clusterAddress = ch.asyncConnector.connection.RemoteAddr()
		logIdentifier = fmt.Sprintf("ASYNC-%v", ch.asyncConnector.clusterType)
		forwardToSecondary = forwardToAsyncOnly
		requestTimeout = time.Duration(ch.conf.AsyncHandshakeTimeoutMs) * time.Millisecond
	} else if ch.forwardAuthToTarget {
		// secondary is ORIGIN

		clusterAddress = ch.originCassandraConnector.connection.RemoteAddr()
		logIdentifier = "ORIGIN"
		forwardToSecondary = forwardToOrigin
	} else {
		// secondary is TARGET

		clusterAddress = ch.targetCassandraConnector.connection.RemoteAddr()
		logIdentifier = "TARGET"
		forwardToSecondary = forwardToTarget
	}

	log.Infof("Initiating startup between %v and %v (%v)", clientIPAddress, clusterAddress, logIdentifier)
	phase := 1
	attempts := 0

	var authenticator *DsePlainTextAuthenticator
	if asyncConnector {
		if ch.asyncHandshakeCreds != nil {
			authenticator = &DsePlainTextAuthenticator{
				Credentials: ch.asyncHandshakeCreds,
			}
		}
	} else if ch.secondaryHandshakeCreds != nil {
		authenticator = &DsePlainTextAuthenticator{
			Credentials: ch.secondaryHandshakeCreds,
		}
	}

	var lastResponse *frame.Frame
	for {
		if attempts > maxAuthRetries {
			return fmt.Errorf("reached max number of attempts to complete secondary (%v) handshake", logIdentifier)
		}

		attempts++

		var request *frame.RawFrame
		var response *frame.RawFrame
		requestSent := false

		switch phase {
		case 1:
			requestSent = !asyncConnector
			request = startupRequest
			response = startupResponse
		case 2:
			if authenticator == nil {
				return fmt.Errorf(
					"secondary cluster (%v) requested authentication but primary did not, "+
						"can not proceed with secondary handshake", logIdentifier)
			}

			var err error
			var parsedRequest *frame.Frame
			parsedRequest, err = performHandshakeStep(
				authenticator, startupRequest.Header.Version, startupRequest.Header.StreamId, lastResponse)
			if err != nil {
				return fmt.Errorf("could not perform handshake step: %w", err)
			}

			request, err = ch.getCodec().ConvertToRawFrame(parsedRequest)
			if err != nil {
				return fmt.Errorf("could not convert auth response frame to raw frame: %w", err)
			}
			response = nil
		}

		if !requestSent {
			overallRequestStartTime := time.Now()
			channel := make(chan *customResponse, 1)
			err := ch.executeRequest(
				NewFrameDecodeContext(request, ch.getCompression()),
				NewGenericRequestInfo(forwardToSecondary, asyncConnector, false),
				ch.LoadCurrentKeyspace(),
				overallRequestStartTime,
				channel,
				requestTimeout)

			if err != nil {
				return fmt.Errorf("unable to send secondary (%v) handshake frame to %v: %w", logIdentifier, clusterAddress, err)
			}

			select {
			case customResponse, ok := <-channel:
				if !ok || customResponse == nil {
					if ch.clientHandlerContext.Err() != nil {
						return ShutdownErr
					}

					return fmt.Errorf("error while receiving secondary handshake response from %v (%v)",
						clusterAddress, logIdentifier)
				}
				response = customResponse.aggregatedResponse
			case <-ch.clientHandlerContext.Done():
				return ShutdownErr
			}
		}

		newPhase, parsedFrame, done, err := handleSecondaryHandshakeResponse(
			phase, response, clientIPAddress, clusterAddress, ch.getCompression(), logIdentifier)
		if err != nil {
			return err
		}
		if done {
			if asyncConnector {
				if ch.asyncConnector.SetReady() {
					return nil
				} else {
					return fmt.Errorf(
						"could not set async connector (%v) as ready after a successful handshake "+
							"because the connector was already shutdown", logIdentifier)
				}
			}
			return nil
		}
		phase = newPhase
		lastResponse = parsedFrame
	}
}

func handleSecondaryHandshakeResponse(
	phase int, f *frame.RawFrame, clientIPAddress net.Addr,
	clusterAddress net.Addr, compression primitive.Compression, logIdentifier string) (int, *frame.Frame, bool, error) {
	parsedFrame, err := codecs[compression].ConvertFromRawFrame(f)
	if err != nil {
		return phase, nil, false, fmt.Errorf("could not decode frame from %v: %w", clusterAddress, err)
	}

	done := false
	switch f.Header.OpCode {
	case primitive.OpCodeAuthenticate:
		log.Debugf("Received AUTHENTICATE for secondary handshake (%v)", logIdentifier)
		return 2, parsedFrame, false, nil
	case primitive.OpCodeAuthChallenge:
		log.Debugf("Received AUTH_CHALLENGE for secondary handshake (%v)", logIdentifier)
	case primitive.OpCodeReady:
		done = true
		log.Debugf("%v (%v) did not request authorization for client %v", clusterAddress, logIdentifier, clientIPAddress)
	case primitive.OpCodeAuthSuccess:
		done = true
		log.Debugf("%s successfully authenticated with %v (%v)", clientIPAddress, clusterAddress, logIdentifier)
	default:
		authErrorMsg, ok := parsedFrame.Body.Message.(*message.AuthenticationError)
		if ok {
			return phase, parsedFrame, done, &AuthError{errMsg: authErrorMsg}
		}
		return phase, parsedFrame, done, fmt.Errorf(
			"received response in secondary handshake (%v) that was not "+
				"READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS: %v", logIdentifier, parsedFrame.Body.Message)
	}
	return phase, parsedFrame, done, nil
}

func validateSecondaryStartupResponse(f *frame.RawFrame, clusterType common.ClusterType) error {
	switch f.Header.OpCode {
	case primitive.OpCodeAuthenticate:
	case primitive.OpCodeAuthChallenge:
	case primitive.OpCodeReady:
	case primitive.OpCodeAuthSuccess:
	default:
		return fmt.Errorf(
			"received response in secondary handshake (%v) that was not "+
				"READY, AUTHENTICATE, AUTH_CHALLENGE, or AUTH_SUCCESS: %v", clusterType, f.Header.OpCode.String())
	}

	return nil
}
